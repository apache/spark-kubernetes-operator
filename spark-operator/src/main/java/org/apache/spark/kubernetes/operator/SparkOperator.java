/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.kubernetes.operator;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.api.config.ConfigurationServiceOverrider;
import io.javaoperatorsdk.operator.api.config.ControllerConfigurationOverrider;
import io.javaoperatorsdk.operator.processing.event.rate.LinearRateLimiter;
import io.javaoperatorsdk.operator.processing.event.rate.RateLimiter;
import io.javaoperatorsdk.operator.processing.retry.GenericRetry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.kubernetes.operator.client.KubernetesClientFactory;
import org.apache.spark.kubernetes.operator.config.SparkOperatorConfigMapReconciler;
import org.apache.spark.kubernetes.operator.config.SparkOperatorConf;
import org.apache.spark.kubernetes.operator.health.SentinelManager;
import org.apache.spark.kubernetes.operator.metrics.MetricsService;
import org.apache.spark.kubernetes.operator.metrics.MetricsSystem;
import io.javaoperatorsdk.operator.RegisteredController;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.kubernetes.operator.metrics.MetricsSystemFactory;
import org.apache.spark.kubernetes.operator.metrics.source.OperatorJosdkMetrics;
import org.apache.spark.kubernetes.operator.probe.ProbeService;
import org.apache.spark.kubernetes.operator.reconciler.SparkApplicationReconciler;
import org.apache.spark.kubernetes.operator.reconciler.SparkReconcilerUtils;
import org.apache.spark.kubernetes.operator.utils.StatusRecorder;

import java.time.Duration;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.spark.kubernetes.operator.config.SparkOperatorConf.DynamicConfigEnabled;
import static org.apache.spark.kubernetes.operator.config.SparkOperatorConf.DynamicConfigSelectorStr;
import static org.apache.spark.kubernetes.operator.config.SparkOperatorConf.OperatorNamespace;
import static org.apache.spark.kubernetes.operator.config.SparkOperatorConf.ReconcilerParallelism;
import static org.apache.spark.kubernetes.operator.config.SparkOperatorConf.TerminateOnInformerFailure;
import static org.apache.spark.kubernetes.operator.config.SparkOperatorConf.TerminationTimeoutSeconds;

/**
 * Entry point for Spark Operator.
 * Sets up reconcilers for CustomResource and health check servers
 */
@Slf4j
public class SparkOperator {
    private Operator sparkOperator;
    private Operator sparkOperatorConfMonitor;
    private KubernetesClient client;
    private StatusRecorder statusRecorder;
    private MetricsSystem metricsSystem;
    protected Set<RegisteredController<?>> registeredSparkControllers;
    protected Set<String> watchedNamespaces;

    private SentinelManager sentinelManager;
    private ProbeService probeService;
    private MetricsService metricsService;
    private ExecutorService metricsResourcesSingleThreadPool;

    public SparkOperator() {
        this.metricsSystem = MetricsSystemFactory.createMetricsSystem();
        this.client = KubernetesClientFactory.buildKubernetesClient(metricsSystem);
        this.statusRecorder = new StatusRecorder(SparkOperatorConf.getApplicationStatusListener());
        this.registeredSparkControllers = new HashSet<>();
        this.watchedNamespaces = SparkReconcilerUtils.getWatchedNamespaces();
        this.sentinelManager = new SentinelManager<SparkApplication>();
        this.sparkOperator = createOperator();
        this.sparkOperatorConfMonitor = createSparkOperatorConfMonitor();
        var operators = Stream.of(this.sparkOperator, this.sparkOperatorConfMonitor)
                .filter(Objects::nonNull).collect(Collectors.toList());
        this.probeService = new ProbeService(operators, this.sentinelManager);
        this.metricsService = new MetricsService(metricsSystem);
        this.metricsResourcesSingleThreadPool = Executors.newSingleThreadExecutor();
    }

    protected Operator createOperator() {
        Operator op = new Operator(this::overrideOperatorConfigs);
        registeredSparkControllers.add(
                op.register(new SparkApplicationReconciler(statusRecorder, sentinelManager),
                        this::overrideControllerConfigs));
        return op;
    }

    protected Operator createSparkOperatorConfMonitor() {
        if (DynamicConfigEnabled.getValue()) {
            Operator op = new Operator(client, c -> {
                c.withStopOnInformerErrorDuringStartup(true);
                c.withCloseClientOnStop(false);
                c.withInformerStoppedHandler(
                        (informer, ex) -> log.error(
                                "Dynamic config informer stopped: operator will not accept " +
                                        "config updates.")
                );
            });
            op.register(new SparkOperatorConfigMapReconciler(this::updateWatchingNamespaces), c -> {
                c.settingNamespaces(OperatorNamespace.getValue());
                c.withLabelSelector(DynamicConfigSelectorStr.getValue());
            });
            return op;
        } else {
            return null;
        }
    }

    protected Operator getOperator() {
        return this.sparkOperator;
    }

    protected ProbeService getProbeService() {
        return this.probeService;
    }

    protected boolean updateWatchingNamespaces(Set<String> namespaces) {
        if (watchedNamespaces.equals(namespaces)) {
            log.info("No watched namespace change detected");
            return false;
        }
        if (CollectionUtils.isEmpty(namespaces)) {
            log.error("Cannot updating namespaces to empty");
            return false;
        }
        registeredSparkControllers.forEach(c -> {
            if (c.allowsNamespaceChanges()) {
                log.info("Updating operator namespaces to {}", namespaces);
                c.changeNamespaces(namespaces);
            }
        });
        this.watchedNamespaces = new HashSet<>(namespaces);
        return true;
    }

    protected void overrideOperatorConfigs(ConfigurationServiceOverrider overrider) {
        overrider.withKubernetesClient(client);
        overrider.withStopOnInformerErrorDuringStartup(TerminateOnInformerFailure.getValue());
        overrider.withTerminationTimeoutSeconds(TerminationTimeoutSeconds.getValue());
        int parallelism = ReconcilerParallelism.getValue();
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
        if (SparkOperatorConf.JOSDKMetricsEnabled.getValue()) {
            log.info("Adding OperatorJosdkMetrics.");
            OperatorJosdkMetrics operatorJosdkMetrics = new OperatorJosdkMetrics();
            overrider.withMetrics(operatorJosdkMetrics);
            metricsSystem.registerSource(operatorJosdkMetrics);
        }
    }

    protected void overrideControllerConfigs(ControllerConfigurationOverrider<?> overrider) {
        if (watchedNamespaces.isEmpty()) {
            log.info("Initializing operator watching at cluster level.");
        } else {
            log.info("Initializing with watched namespaces {}", watchedNamespaces);
        }
        overrider.settingNamespaces(watchedNamespaces);

        RateLimiter<?> rateLimiter = new LinearRateLimiter(
                Duration.ofSeconds(SparkOperatorConf.RateLimiterRefreshPeriodSeconds.getValue()),
                SparkOperatorConf.RateLimiterLimit.getValue());
        overrider.withRateLimiter(rateLimiter);

        GenericRetry genericRetry = new GenericRetry()
                .setMaxAttempts(SparkOperatorConf.RetryMaxAttempts.getValue())
                .setInitialInterval(
                        Duration.ofSeconds(SparkOperatorConf.RetryInitialInternalSeconds.getValue())
                                .toMillis())
                .setIntervalMultiplier(SparkOperatorConf.RetryInternalMultiplier.getValue());
        if (SparkOperatorConf.RetryMaxIntervalSeconds.getValue() > 0) {
            genericRetry.setMaxInterval(
                    Duration.ofSeconds(SparkOperatorConf.RetryMaxIntervalSeconds.getValue())
                            .toMillis());
        }
        overrider.withRetry(genericRetry);
    }

    public static void main(String[] args) {
        SparkOperator sparkOperator = new SparkOperator();
        sparkOperator.getOperator().start();
        if (DynamicConfigEnabled.getValue() && sparkOperator.sparkOperatorConfMonitor != null) {
            sparkOperator.sparkOperatorConfMonitor.start();
        }
        sparkOperator.probeService.start();
        // MetricsServer start follows the MetricsSystem start
        // so that MetricsSystem::getSinks will not return an empty list
        sparkOperator.metricsResourcesSingleThreadPool.submit(() -> {
            sparkOperator.metricsSystem.start();
        });
        sparkOperator.metricsResourcesSingleThreadPool.submit(() -> {
            sparkOperator.metricsService.start();
        });
    }
}
