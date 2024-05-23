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

package org.apache.spark.k8s.operator.config;

import static org.apache.spark.k8s.operator.Constants.LABEL_COMPONENT_NAME;
import static org.apache.spark.k8s.operator.Constants.LABEL_RESOURCE_NAME;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_OPERATOR_NAME;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import io.javaoperatorsdk.operator.api.config.LeaderElectionConfiguration;
import io.javaoperatorsdk.operator.processing.event.rate.LinearRateLimiter;
import io.javaoperatorsdk.operator.processing.event.rate.RateLimiter;
import io.javaoperatorsdk.operator.processing.retry.GenericRetry;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;

/** Spark Operator Configuration options. */
@Slf4j
public class SparkOperatorConf {
  public static final ConfigOption<String> OperatorAppName =
      ConfigOption.<String>builder()
          .key("spark.kubernetes.operator.name")
          .typeParameterClass(String.class)
          .description("Name of the operator.")
          .defaultValue("spark-kubernetes-operator")
          .enableDynamicOverride(false)
          .build();

  public static final ConfigOption<String> OperatorNamespace =
      ConfigOption.<String>builder()
          .key("spark.kubernetes.operator.namespace")
          .typeParameterClass(String.class)
          .description("Namespace that operator is deployed within.")
          .defaultValue("default")
          .enableDynamicOverride(false)
          .build();

  public static final ConfigOption<String> OperatorWatchedNamespaces =
      ConfigOption.<String>builder()
          .key("spark.kubernetes.operator.watchedNamespaces")
          .description(
              "Comma-separated list of namespaces that the operator would be "
                  + "watching for Spark resources. If unset, operator would "
                  + "watch all namespaces by default.")
          .defaultValue(null)
          .typeParameterClass(String.class)
          .build();

  public static final ConfigOption<Boolean> TerminateOnInformerFailure =
      ConfigOption.<Boolean>builder()
          .key("spark.kubernetes.operator.terminateOnInformerFailure")
          .typeParameterClass(Boolean.class)
          .description(
              "Enable to indicate informer errors should stop operator startup. If "
                  + "disabled, operator startup will ignore recoverable errors, "
                  + "caused for example by RBAC issues and will retry "
                  + "periodically.")
          .defaultValue(false)
          .enableDynamicOverride(false)
          .build();

  public static final ConfigOption<Integer> ReconcilerTerminationTimeoutSeconds =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.reconciler.terminationTimeoutSeconds")
          .description(
              "Grace period for operator shutdown before reconciliation threads are killed.")
          .enableDynamicOverride(false)
          .typeParameterClass(Integer.class)
          .defaultValue(30)
          .build();

  public static final ConfigOption<Integer> ReconcilerParallelism =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.reconciler.parallelism")
          .description(
              "Thread pool size for Spark Operator reconcilers. Use -1 for unbounded pool.")
          .enableDynamicOverride(false)
          .typeParameterClass(Integer.class)
          .defaultValue(30)
          .build();

  public static final ConfigOption<Long> ReconcilerForegroundRequestTimeoutSeconds =
      ConfigOption.<Long>builder()
          .key("spark.kubernetes.operator.reconciler.foregroundRequestTimeoutSeconds")
          .description(
              "Timeout (in seconds) to for requests made to API server. this "
                  + "applies only to foreground requests.")
          .defaultValue(120L)
          .typeParameterClass(Long.class)
          .build();

  public static final ConfigOption<Long> SparkAppReconcileIntervalSeconds =
      ConfigOption.<Long>builder()
          .key("spark.kubernetes.operator.reconciler.intervalSeconds")
          .description(
              "Interval (in seconds) to reconcile when application is is starting "
                  + "up. Note that reconcile is always expected to be triggered "
                  + "per update - this interval controls the reconcile behavior "
                  + "when operator still need to reconcile even when there's no "
                  + "update ,e.g. for timeout checks.")
          .defaultValue(120L)
          .typeParameterClass(Long.class)
          .build();

  public static final ConfigOption<Boolean> TrimAttemptStateTransitionHistory =
      ConfigOption.<Boolean>builder()
          .key("spark.kubernetes.operator.reconciler.trimStateTransitionHistoryEnabled")
          .description(
              "When enabled, operator would trim state transition history when a "
                  + "new attempt starts, keeping previous attempt summary only.")
          .defaultValue(true)
          .typeParameterClass(Boolean.class)
          .build();

  public static final ConfigOption<String> SparkAppStatusListenerClassNames =
      ConfigOption.<String>builder()
          .key("spark.kubernetes.operator.reconciler.appStatusListenerClassNames")
          .defaultValue("")
          .description("Comma-separated names of SparkAppStatusListener class implementations")
          .enableDynamicOverride(false)
          .typeParameterClass(String.class)
          .build();

  public static final ConfigOption<Boolean> DynamicConfigEnabled =
      ConfigOption.<Boolean>builder()
          .key("spark.kubernetes.operator.dynamicConfig.enabled")
          .typeParameterClass(Boolean.class)
          .description(
              "When enabled, operator would use config map as source of truth for config "
                  + "property override. The config map need to be created in "
                  + "spark.kubernetes.operator.namespace, and labeled with operator name.")
          .defaultValue(false)
          .enableDynamicOverride(false)
          .build();

  public static final ConfigOption<String> DynamicConfigSelectorStr =
      ConfigOption.<String>builder()
          .key("spark.kubernetes.operator.dynamicConfig.selectorStr")
          .typeParameterClass(String.class)
          .description("The selector str applied to dynamic config map.")
          .defaultValue(labelsAsStr(defaultOperatorConfigLabels()))
          .enableDynamicOverride(false)
          .build();

  public static final ConfigOption<Integer> DynamicConfigReconcilerParallelism =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.dynamicConfig.reconcilerParallelism")
          .description("Parallelism for dynamic config reconciler. Use -1 for unbounded pool.")
          .enableDynamicOverride(false)
          .typeParameterClass(Integer.class)
          .defaultValue(1)
          .build();

  public static final ConfigOption<Integer> RateLimiterRefreshPeriodSeconds =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.rateLimiter.refreshPeriodSeconds")
          .description("Operator rate limiter refresh period(in seconds) for each resource.")
          .enableDynamicOverride(false)
          .typeParameterClass(Integer.class)
          .defaultValue(15)
          .build();

  public static final ConfigOption<Integer> RateLimiterLimit =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.rateLimiter.limit")
          .description(
              "Max number of reconcile loops triggered within the rate limiter refresh "
                  + "period for each resource. Setting the limit <= 0 disables the "
                  + "limiter.")
          .enableDynamicOverride(false)
          .typeParameterClass(Integer.class)
          .defaultValue(5)
          .build();

  public static final ConfigOption<Integer> RetryInitialInternalSeconds =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.retry.initialInternalSeconds")
          .description("Initial interval(in seconds) of retries on unhandled controller errors.")
          .enableDynamicOverride(false)
          .typeParameterClass(Integer.class)
          .defaultValue(5)
          .build();

  public static final ConfigOption<Double> RetryInternalMultiplier =
      ConfigOption.<Double>builder()
          .key("spark.kubernetes.operator.retry.internalMultiplier")
          .description("Interval multiplier of retries on unhandled controller errors.")
          .enableDynamicOverride(false)
          .typeParameterClass(Double.class)
          .defaultValue(1.5)
          .build();

  public static final ConfigOption<Integer> RetryMaxIntervalSeconds =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.retry.maxIntervalSeconds")
          .description(
              "Max interval(in seconds) of retries on unhandled controller errors. "
                  + "Set to -1 for unlimited.")
          .enableDynamicOverride(false)
          .typeParameterClass(Integer.class)
          .defaultValue(-1)
          .build();

  public static final ConfigOption<Integer> RetryMaxAttempts =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.retry.maxAttempts")
          .description("Max attempts of retries on unhandled controller errors.")
          .enableDynamicOverride(false)
          .typeParameterClass(Integer.class)
          .defaultValue(15)
          .build();

  public static final ConfigOption<Long> MaxRetryAttemptOnKubeServerFailure =
      ConfigOption.<Long>builder()
          .key("spark.kubernetes.operator.retry.maxRetryAttemptOnKubeServerFailure")
          .description(
              "Maximal number of retry attempts of requests to k8s server upon "
                  + "response 429 and 5xx. This would be performed on top of k8s client "
                  + "spark.kubernetes.operator.retry.maxAttempts. ")
          .defaultValue(3L)
          .typeParameterClass(Long.class)
          .build();

  public static final ConfigOption<Long> RetryAttemptAfterSeconds =
      ConfigOption.<Long>builder()
          .key("spark.kubernetes.operator.retry.attemptAfterSeconds")
          .description(
              "Default time (in seconds) to wait till next request. This would be used if "
                  + "server does not set Retry-After in response.")
          .defaultValue(1L)
          .typeParameterClass(Long.class)
          .build();

  public static final ConfigOption<Long> MaxRetryAttemptAfterSeconds =
      ConfigOption.<Long>builder()
          .key("spark.kubernetes.operator.retry.maxAttemptAfterSeconds")
          .description("Maximal time (in seconds) to wait till next request.")
          .defaultValue(15L)
          .typeParameterClass(Long.class)
          .build();

  public static final ConfigOption<Long> StatusPatchMaxRetry =
      ConfigOption.<Long>builder()
          .key("spark.kubernetes.operator.retry.maxStatusPatchAttempts")
          .description(
              "Maximal number of retry attempts of requests to k8s server for resource "
                  + "status update. This would be performed on top of k8s client "
                  + "spark.kubernetes.operator.retry.maxAttempts to overcome potential "
                  + "conflicting update on the same SparkApplication. ")
          .defaultValue(3L)
          .typeParameterClass(Long.class)
          .build();

  public static final ConfigOption<Long> SecondaryResourceCreateMaxAttempts =
      ConfigOption.<Long>builder()
          .key("spark.kubernetes.operator.retry.secondaryResourceCreateMaxAttempts")
          .description(
              "Maximal number of retry attempts of requesting secondary resource for Spark "
                  + "application. This would be performed on top of k8s client "
                  + "spark.kubernetes.operator.retry.maxAttempts to overcome potential "
                  + "conflicting reconcile on the same SparkApplication. ")
          .defaultValue(3L)
          .typeParameterClass(Long.class)
          .build();

  public static final ConfigOption<Boolean> JOSDKMetricsEnabled =
      ConfigOption.<Boolean>builder()
          .key("spark.kubernetes.operator.metrics.josdkMetricsEnabled")
          .description(
              "When enabled, the josdk metrics will be added in metrics source and "
                  + "configured for operator.")
          .defaultValue(true)
          .build();

  public static final ConfigOption<Boolean> KubernetesClientMetricsEnabled =
      ConfigOption.<Boolean>builder()
          .key("spark.kubernetes.operator.metrics.clientMetricsEnabled")
          .defaultValue(true)
          .description(
              "Enable KubernetesClient metrics for measuring the HTTP traffic to "
                  + "the Kubernetes API Server. Since the metrics is collected "
                  + "via Okhttp interceptors, can be disabled when opt in "
                  + "customized interceptors.")
          .build();

  public static final ConfigOption<Boolean> KubernetesClientMetricsGroupByResponseCodeGroupEnabled =
      ConfigOption.<Boolean>builder()
          .key("spark.kubernetes.operator.metrics.clientMetricsGroupByResponseCodeEnabled")
          .description(
              "When enabled, additional metrics group by http response code group(1xx, "
                  + "2xx, 3xx, 4xx, 5xx) received from API server will be added. Users "
                  + "can disable it when their monitoring system can combine lower level "
                  + "kubernetes.client.http.response.<3-digit-response-code> metrics.")
          .defaultValue(true)
          .build();

  public static final ConfigOption<Integer> OperatorMetricsPort =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.metrics.port")
          .defaultValue(19090)
          .description("The port used for checking metrics")
          .typeParameterClass(Integer.class)
          .enableDynamicOverride(false)
          .build();

  public static final ConfigOption<Integer> OperatorProbePort =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.health.probePort")
          .defaultValue(18080)
          .description("The port used for health/readiness check probe status.")
          .typeParameterClass(Integer.class)
          .enableDynamicOverride(false)
          .build();

  public static final ConfigOption<Integer> SentinelExecutorServicePoolSize =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.health.sentinelExecutorPoolSize")
          .description(
              "Size of executor service in Sentinel Managers to check the health "
                  + "of sentinel resources.")
          .defaultValue(3)
          .enableDynamicOverride(false)
          .typeParameterClass(Integer.class)
          .build();

  public static final ConfigOption<Long> SENTINEL_RESOURCE_RECONCILIATION_DELAY =
      ConfigOption.<Long>builder()
          .key("spark.kubernetes.operator.health.sentinelResourceReconciliationDelaySeconds")
          .defaultValue(60L)
          .description(
              "Allowed max time(seconds) between spec update and reconciliation "
                  + "for sentinel resources.")
          .enableDynamicOverride(true)
          .typeParameterClass(Long.class)
          .build();

  public static final ConfigOption<Boolean> LEADER_ELECTION_ENABLED =
      ConfigOption.<Boolean>builder()
          .key("spark.kubernetes.operator.leaderElection.enabled")
          .defaultValue(false)
          .description(
              "Enable leader election for the operator to allow running standby instances.")
          .enableDynamicOverride(false)
          .typeParameterClass(Boolean.class)
          .build();

  public static final ConfigOption<String> LEADER_ELECTION_LEASE_NAME =
      ConfigOption.<String>builder()
          .key("spark.kubernetes.operator.leaderElection.leaseName")
          .defaultValue("spark-operator-lease")
          .description(
              "Leader election lease name, must be unique for leases in the same namespace.")
          .enableDynamicOverride(false)
          .typeParameterClass(String.class)
          .build();

  public static final ConfigOption<Long> LEADER_ELECTION_LEASE_DURATION_SECONDS =
      ConfigOption.<Long>builder()
          .key("spark.kubernetes.operator.leaderElection.leaseDurationSeconds")
          .defaultValue(1200L)
          .description("Leader election lease duration.")
          .enableDynamicOverride(false)
          .typeParameterClass(Long.class)
          .build();

  public static final ConfigOption<Long> LEADER_ELECTION_RENEW_DEADLINE_SECONDS =
      ConfigOption.<Long>builder()
          .key("spark.kubernetes.operator.leaderElection.renewDeadlineSeconds")
          .defaultValue(600L)
          .description("Leader election renew deadline.")
          .enableDynamicOverride(false)
          .typeParameterClass(Long.class)
          .build();

  public static final ConfigOption<Long> LEADER_ELECTION_RETRY_PERIOD_SECONDS =
      ConfigOption.<Long>builder()
          .key("spark.kubernetes.operator.leaderElection.retryPeriodSeconds")
          .defaultValue(180L)
          .description("Leader election retry period.")
          .enableDynamicOverride(false)
          .typeParameterClass(Long.class)
          .build();

  public static LeaderElectionConfiguration getLeaderElectionConfig() {
    return new LeaderElectionConfiguration(
        LEADER_ELECTION_LEASE_NAME.getValue(),
        OperatorNamespace.getValue(),
        Duration.ofSeconds(LEADER_ELECTION_LEASE_DURATION_SECONDS.getValue()),
        Duration.ofSeconds(LEADER_ELECTION_RENEW_DEADLINE_SECONDS.getValue()),
        Duration.ofSeconds(LEADER_ELECTION_RETRY_PERIOD_SECONDS.getValue()));
  }

  public static GenericRetry getOperatorRetry() {
    GenericRetry genericRetry =
        new GenericRetry()
            .setMaxAttempts(RetryMaxAttempts.getValue())
            .setInitialInterval(
                Duration.ofSeconds(RetryInitialInternalSeconds.getValue()).toMillis())
            .setIntervalMultiplier(RetryInternalMultiplier.getValue());
    if (RetryMaxIntervalSeconds.getValue() > 0) {
      genericRetry.setMaxInterval(
          Duration.ofSeconds(RetryMaxIntervalSeconds.getValue()).toMillis());
    }
    return genericRetry;
  }

  public static RateLimiter<?> getOperatorRateLimiter() {
    return new LinearRateLimiter(
        Duration.ofSeconds(RateLimiterRefreshPeriodSeconds.getValue()),
        RateLimiterLimit.getValue());
  }

  public static Map<String, String> defaultCommonOperatorResourceLabels() {
    Map<String, String> labels = new HashMap<>();
    labels.put(LABEL_RESOURCE_NAME, OperatorAppName.getValue());
    return labels;
  }

  public static Map<String, String> defaultOperatorConfigLabels() {
    Map<String, String> labels = new HashMap<>(defaultCommonOperatorResourceLabels());
    labels.put(LABEL_COMPONENT_NAME, "operator-dynamic-config-overrides");
    return labels;
  }

  public static Map<String, String> defaultManagedResourceLabels() {
    Map<String, String> labels = new HashMap<>();
    labels.put(LABEL_SPARK_OPERATOR_NAME, OperatorAppName.getValue());
    return labels;
  }

  public static Map<String, String> sparkAppResourceLabels(final SparkApplication app) {
    return sparkAppResourceLabels(app.getMetadata().getName());
  }

  public static Map<String, String> sparkAppResourceLabels(final String appName) {
    Map<String, String> labels = defaultManagedResourceLabels();
    labels.put(Constants.LABEL_SPARK_APPLICATION_NAME, appName);
    return labels;
  }

  protected static String labelsAsStr(Map<String, String> labels) {
    return labels.entrySet().stream()
        .map(e -> String.join("=", e.getKey(), e.getValue()))
        .collect(Collectors.joining(","));
  }
}
