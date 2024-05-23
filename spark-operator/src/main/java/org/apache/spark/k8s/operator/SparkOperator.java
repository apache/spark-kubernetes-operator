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
import org.apache.spark.k8s.operator.listeners.SparkAppStatusListener;
import org.apache.spark.k8s.operator.metrics.MetricsService;
import org.apache.spark.k8s.operator.metrics.MetricsSystem;
import org.apache.spark.k8s.operator.metrics.MetricsSystemFactory;
import org.apache.spark.k8s.operator.metrics.healthcheck.SentinelManager;
import org.apache.spark.k8s.operator.metrics.source.KubernetesMetricsInterceptor;
import org.apache.spark.k8s.operator.metrics.source.OperatorJosdkMetrics;
import org.apache.spark.k8s.operator.probe.ProbeService;
import org.apache.spark.k8s.operator.reconciler.SparkAppReconciler;
import org.apache.spark.k8s.operator.utils.ClassLoadingUtils;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;
import org.apache.spark.k8s.operator.utils.Utils;

/**
 * Entry point for Spark Operator. Sets up reconcilers for CustomResource and health check servers
 */
@Slf4j
public class SparkOperator {
  private final KubernetesClient client;
  private final SparkAppSubmissionWorker appSubmissionWorker;
  private final SparkAppStatusRecorder sparkAppStatusRecorder;
  protected final List<Operator> operators;
  protected final Set<RegisteredController<?>> registeredSparkControllers;
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
    this.sparkAppStatusRecorder =
        new SparkAppStatusRecorder(
            ClassLoadingUtils.getStatusListener(
                SparkAppStatusListener.class,
                SparkOperatorConf.SPARK_APP_STATUS_LISTENER_CLASS_NAMES.getValue()));
    this.registeredSparkControllers = new HashSet<>();
    this.watchedNamespaces = Utils.getWatchedNamespaces();
    this.sparkApplicationSentinelManager = new SentinelManager<>();
    this.operators = registerSparkOperator(registeredSparkControllers);
    this.metricsResourcesSingleThreadPool = Executors.newSingleThreadExecutor();
    this.probeService =
        new ProbeService(
            operators, Collections.singletonList(sparkApplicationSentinelManager), null);
    this.metricsService = new MetricsService(metricsSystem, metricsResourcesSingleThreadPool);
  }

  /**
   * Register all referenced {@link io.javaoperatorsdk.operator.Operator Operators} to be started by
   * the SparkOperator, one per managed resource kind. For example, by default one operator instance
   * would be started for {@link org.apache.spark.k8s.operator.SparkApplication}, managing
   * SparkApplications and its secondary resources including Spark pods, config maps .etc. Another
   * Operator instance can be started for Operator hot properties loading. Similarly more operator
   * instances might be introduced in future for CRD version upgrade or migration
   *
   * @param registeredSparkControllers Set of new {@link
   *     io.javaoperatorsdk.operator.RegisteredController} introduced by the operators
   * @return all registered operator instances
   */
  protected List<Operator> registerSparkOperator(
      Set<RegisteredController<?>> registeredSparkControllers) {
    List<Operator> registeredOperators = new ArrayList<>();
    Operator sparkOperator = new Operator(this::overrideOperatorConfigs);
    registeredSparkControllers.add(
        sparkOperator.register(
            new SparkAppReconciler(
                appSubmissionWorker, sparkAppStatusRecorder, sparkApplicationSentinelManager),
            this::overrideControllerConfigs));
    registeredOperators.add(sparkOperator);
    if (SparkOperatorConf.DYNAMIC_CONFIG_ENABLED.getValue()) {
      Operator confMonitor = new Operator(this::overrideConfigMonitorConfigs);
      String operatorNamespace = SparkOperatorConf.OPERATOR_NAMESPACE.getValue();
      String confSelector = SparkOperatorConf.DYNAMIC_CONFIG_SELECTOR.getValue();
      log.info(
          "Starting conf monitor in namespace: {}, with selector: {}",
          operatorNamespace,
          confSelector);
      confMonitor.register(
          new SparkOperatorConfigMapReconciler(
              this::updateWatchingNamespaces,
              SparkOperatorConf.OPERATOR_NAMESPACE.getValue(),
              unused -> Utils.getWatchedNamespaces()),
          c -> {
            c.withRateLimiter(SparkOperatorConf.getOperatorRateLimiter());
            c.settingNamespaces(operatorNamespace);
            c.withLabelSelector(confSelector);
          });
      registeredOperators.add(confMonitor);
    }
    return registeredOperators;
  }

  /**
   * Handler for updating watched namespaces when dynamic config is enabled, internally update the
   * controllers / operators for the namespace change
   *
   * @param namespaces the watches namespaces set
   * @return true if the update is recognized
   */
  protected boolean updateWatchingNamespaces(Set<String> namespaces) {
    if (watchedNamespaces.equals(namespaces)) {
      log.info("No watched namespace change detected");
      return false;
    }
    if (namespaces.isEmpty()) {
      log.error(
          "Operator started at cluster level cannot be configured to watch a selection of "
              + "namespaces. Please restart operator with namespace-level watching with non-empty "
              + "list configured at spark.kubernetes.operator.watchedNamespaces");
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

  private void overrideOperatorConfigs(ConfigurationServiceOverrider overrider) {
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
      log.info("Adding OperatorJosdkMetrics.");
      OperatorJosdkMetrics operatorJosdkMetrics = new OperatorJosdkMetrics();
      overrider.withMetrics(operatorJosdkMetrics);
      metricsSystem.registerSource(operatorJosdkMetrics);
    }
  }

  private void overrideConfigMonitorConfigs(ConfigurationServiceOverrider overrider) {
    overrider.withKubernetesClient(client);
    int parallelism = SparkOperatorConf.DYNAMIC_CONFIG_RECONCILER_PARALLELISM.getValue();
    if (parallelism <= 0) {
      overrider.withExecutorService(Executors.newCachedThreadPool());
    } else {
      overrider.withConcurrentReconciliationThreads(parallelism);
    }
    overrider.withStopOnInformerErrorDuringStartup(true);
    overrider.withCloseClientOnStop(false);
    overrider.withInformerStoppedHandler(
        (informer, ex) ->
            log.error("Dynamic config informer stopped: operator will not accept config updates."));
  }

  private void overrideControllerConfigs(ControllerConfigurationOverrider<?> overrider) {
    if (watchedNamespaces.isEmpty()) {
      log.info("Initializing operator watching at cluster level.");
    } else {
      log.info("Initializing with watched namespaces {}", watchedNamespaces);
    }
    overrider.settingNamespaces(watchedNamespaces);
    overrider.withRateLimiter(SparkOperatorConf.getOperatorRateLimiter());
    overrider.withRetry(SparkOperatorConf.getOperatorRetry());
  }

  private List<Interceptor> getClientInterceptors(MetricsSystem metricsSystem) {
    List<Interceptor> clientInterceptors = new ArrayList<>();
    if (SparkOperatorConf.KUBERNETES_CLIENT_METRICS_ENABLED.getValue()) {
      KubernetesMetricsInterceptor metricsInterceptor = new KubernetesMetricsInterceptor();
      clientInterceptors.add(metricsInterceptor);
      metricsSystem.registerSource(metricsInterceptor);
    }
    return clientInterceptors;
  }

  /**
   * Entry point for Spark Operator
   *
   * @param args not used. See how to configure operator behavior at {@link
   *     org.apache.spark.k8s.operator.config.SparkOperatorConf}
   */
  public static void main(String[] args) {
    SparkOperator sparkOperator = new SparkOperator();
    for (Operator operator : sparkOperator.operators) {
      operator.start();
    }
    sparkOperator.probeService.start();
    // Single thread queue to ensure MetricsService starts after the MetricsSystem
    sparkOperator.metricsResourcesSingleThreadPool.submit(sparkOperator.metricsSystem::start);
    sparkOperator.metricsResourcesSingleThreadPool.submit(sparkOperator.metricsService::start);
  }
}
