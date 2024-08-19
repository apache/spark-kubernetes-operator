/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.k8s.operator;

import static org.apache.spark.k8s.operator.utils.Utils.getAppStatusListener;
import static org.apache.spark.k8s.operator.utils.Utils.getWatchedNamespaces;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.RegisteredController;
import io.javaoperatorsdk.operator.api.config.ConfigurationServiceOverrider;
import io.javaoperatorsdk.operator.api.config.ControllerConfigurationOverrider;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Interceptor;

import org.apache.spark.k8s.operator.client.KubernetesClientFactory;
import org.apache.spark.k8s.operator.config.SparkOperatorConf;
import org.apache.spark.k8s.operator.config.SparkOperatorConfigMapReconciler;
import org.apache.spark.k8s.operator.metrics.MetricsService;
import org.apache.spark.k8s.operator.metrics.MetricsSystem;
import org.apache.spark.k8s.operator.metrics.MetricsSystemFactory;
import org.apache.spark.k8s.operator.metrics.healthcheck.SentinelManager;
import org.apache.spark.k8s.operator.metrics.source.KubernetesMetricsInterceptor;
import org.apache.spark.k8s.operator.metrics.source.OperatorJosdkMetrics;
import org.apache.spark.k8s.operator.probe.ProbeService;
import org.apache.spark.k8s.operator.reconciler.SparkAppReconciler;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;

/**
 * Entry point for Spark Operator. Bootstrap the operator app by starting watch and reconciler for
 * SparkApps, starting watch for hot property loading, if enabled, and starting metrics server with
 * sentinel monitor if enabled
 */
@Slf4j
public class SparkOperator {
  private final List<Operator> registeredOperators;
  private final KubernetesClient client;
  private final SparkAppSubmissionWorker appSubmissionWorker;
  private final SparkAppStatusRecorder sparkAppStatusRecorder;
  protected Set<RegisteredController<?>> registeredSparkControllers;
  protected Set<String> watchedNamespaces;
  private final MetricsSystem metricsSystem;
  private final SentinelManager<SparkApplication> sparkApplicationSentinelManager;
  private final ProbeService probeService;
  private final MetricsService metricsService;
  private final ExecutorService metricsResourcesSingleThreadPool;

  public SparkOperator() {
    this.metricsSystem = MetricsSystemFactory.createMetricsSystem();
    this.client =
        KubernetesClientFactory.buildKubernetesClient(getClientInterceptors(metricsSystem));
    this.appSubmissionWorker = new SparkAppSubmissionWorker();
    this.sparkAppStatusRecorder = new SparkAppStatusRecorder(getAppStatusListener());
    this.registeredSparkControllers = new HashSet<>();
    this.watchedNamespaces = getWatchedNamespaces();
    this.sparkApplicationSentinelManager = new SentinelManager<>();
    this.registeredOperators = new ArrayList<>();
    this.registeredOperators.add(registerSparkOperator());
    if (SparkOperatorConf.DYNAMIC_CONFIG_ENABLED.getValue()) {
      this.registeredOperators.add(registerSparkOperatorConfMonitor());
    }
    this.metricsResourcesSingleThreadPool = Executors.newSingleThreadExecutor();
    this.probeService =
        new ProbeService(
            registeredOperators, Collections.singletonList(sparkApplicationSentinelManager), null);
    this.metricsService = new MetricsService(metricsSystem, metricsResourcesSingleThreadPool);
  }

  protected Operator registerSparkOperator() {
    Operator op = new Operator(this::overrideOperatorConfigs);
    registeredSparkControllers.add(
        op.register(
            new SparkAppReconciler(
                appSubmissionWorker, sparkAppStatusRecorder, sparkApplicationSentinelManager),
            this::overrideControllerConfigs));
    return op;
  }

  protected Operator registerSparkOperatorConfMonitor() {
    Operator op = new Operator(this::overrideConfigMonitorConfigs);
    String operatorNamespace = SparkOperatorConf.OPERATOR_NAMESPACE.getValue();
    String confSelector = SparkOperatorConf.DYNAMIC_CONFIG_SELECTOR.getValue();
    log.info(
        "Starting conf monitor in namespace: {}, with selector: {}",
        operatorNamespace,
        confSelector);
    op.register(
        new SparkOperatorConfigMapReconciler(
            this::updateWatchingNamespaces,
            SparkOperatorConf.OPERATOR_NAMESPACE.getValue(),
            unused -> getWatchedNamespaces()),
        c -> {
          c.withRateLimiter(SparkOperatorConf.getOperatorRateLimiter());
          c.settingNamespaces(operatorNamespace);
          c.withLabelSelector(confSelector);
        });
    return op;
  }

  protected boolean updateWatchingNamespaces(Set<String> namespaces) {
    if (watchedNamespaces.equals(namespaces)) {
      log.info("No watched namespace change detected");
      return false;
    }
    if (watchedNamespaces.isEmpty()) {
      log.info("Cannot update watch namespaces for operator started at cluster level.");
      return false;
    }
    if (namespaces == null || namespaces.isEmpty()) {
      log.error("Cannot updating namespaces to empty");
      return false;
    }
    registeredSparkControllers.forEach(
        c -> {
          if (c.allowsNamespaceChanges()) {
            log.info("Updating operator namespaces to {}", namespaces);
            c.changeNamespaces(namespaces);
          } else {
            log.error("Controller does not allow namespace change, skipping namespace change.");
          }
        });
    this.watchedNamespaces = new HashSet<>(namespaces);
    return true;
  }

  protected void overrideOperatorConfigs(ConfigurationServiceOverrider overrider) {
    overrider.withKubernetesClient(client);
    overrider.withStopOnInformerErrorDuringStartup(
        SparkOperatorConf.TERMINATE_ON_INFORMER_FAILURE_ENABLED.getValue());
    overrider.withTerminationTimeoutSeconds(
        SparkOperatorConf.RECONCILER_TERMINATION_TIMEOUT_SECONDS.getValue());
    int parallelism = SparkOperatorConf.RECONCILER_PARALLELISM.getValue();
    if (parallelism > 0) {
      log.info("Configuring operator with {} reconciliation threads.", parallelism);
      overrider.withConcurrentReconciliationThreads(parallelism);
    } else {
      log.info("Configuring operator with unbounded reconciliation thread pool.");
      overrider.withExecutorService(Executors.newCachedThreadPool());
    }
    if (SparkOperatorConf.LEADER_ELECTION_ENABLED.getValue()) {
      overrider.withLeaderElectionConfiguration(SparkOperatorConf.getLeaderElectionConfig());
    }
    if (SparkOperatorConf.JOSDK_METRICS_ENABLED.getValue()) {
      log.info("Adding Operator JosdkMetrics to metrics system.");
      OperatorJosdkMetrics operatorJosdkMetrics = new OperatorJosdkMetrics();
      overrider.withMetrics(operatorJosdkMetrics);
      metricsSystem.registerSource(operatorJosdkMetrics);
    }
  }

  protected void overrideConfigMonitorConfigs(ConfigurationServiceOverrider overrider) {
    overrider.withKubernetesClient(client);
    overrider.withConcurrentReconciliationThreads(
        SparkOperatorConf.DYNAMIC_CONFIG_RECONCILER_PARALLELISM.getValue());
    overrider.withStopOnInformerErrorDuringStartup(true);
    overrider.withCloseClientOnStop(false);
    overrider.withInformerStoppedHandler(
        (informer, ex) ->
            log.error("Dynamic config informer stopped: operator will not accept config updates."));
  }

  protected void overrideControllerConfigs(ControllerConfigurationOverrider<?> overrider) {
    if (watchedNamespaces.isEmpty()) {
      log.info("Initializing operator watching at cluster level.");
    } else {
      log.info("Initializing with watched namespaces {}", watchedNamespaces);
    }
    overrider.settingNamespaces(watchedNamespaces);
    overrider.withRateLimiter(SparkOperatorConf.getOperatorRateLimiter());
    overrider.withRetry(SparkOperatorConf.getOperatorRetry());
  }

  protected List<Interceptor> getClientInterceptors(MetricsSystem metricsSystem) {
    List<Interceptor> clientInterceptors = new ArrayList<>();
    if (SparkOperatorConf.KUBERNETES_CLIENT_METRICS_ENABLED.getValue()) {
      KubernetesMetricsInterceptor metricsInterceptor = new KubernetesMetricsInterceptor();
      clientInterceptors.add(metricsInterceptor);
      metricsSystem.registerSource(metricsInterceptor);
    }
    return clientInterceptors;
  }

  /**
   * Bootstrap Spark Operator
   *
   * @param args not used - operator behavior are configured from SparkOperatorConf
   */
  public static void main(String[] args) {
    SparkOperator sparkOperator = new SparkOperator();
    for (Operator operator : sparkOperator.registeredOperators) {
      operator.start();
    }
    sparkOperator.probeService.start();
    // Single thread queue to ensure MetricsService starts after the MetricsSystem
    sparkOperator.metricsResourcesSingleThreadPool.submit(sparkOperator.metricsSystem::start);
    sparkOperator.metricsResourcesSingleThreadPool.submit(sparkOperator.metricsService::start);
  }
}
