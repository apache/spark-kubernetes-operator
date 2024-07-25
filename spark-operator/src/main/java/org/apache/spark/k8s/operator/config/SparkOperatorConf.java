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

import java.time.Duration;

import io.javaoperatorsdk.operator.api.config.LeaderElectionConfiguration;
import io.javaoperatorsdk.operator.processing.event.rate.LinearRateLimiter;
import io.javaoperatorsdk.operator.processing.event.rate.RateLimiter;
import io.javaoperatorsdk.operator.processing.retry.GenericRetry;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.utils.Utils;

/** Spark Operator Configuration options. */
@Slf4j
public class SparkOperatorConf {
  public static final ConfigOption<String> OPERATOR_APP_NAME =
      ConfigOption.<String>builder()
          .key("spark.kubernetes.operator.name")
          .enableDynamicOverride(false)
          .description("Name of the operator.")
          .typeParameterClass(String.class)
          .defaultValue("spark-kubernetes-operator")
          .build();

  public static final ConfigOption<String> OPERATOR_NAMESPACE =
      ConfigOption.<String>builder()
          .key("spark.kubernetes.operator.namespace")
          .enableDynamicOverride(false)
          .description("Namespace that operator is deployed within.")
          .typeParameterClass(String.class)
          .defaultValue("default")
          .build();

  public static final ConfigOption<String> OPERATOR_WATCHED_NAMESPACES =
      ConfigOption.<String>builder()
          .key("spark.kubernetes.operator.watchedNamespaces")
          .enableDynamicOverride(true)
          .description(
              "Comma-separated list of namespaces that the operator would be watching for "
                  + "Spark resources. If set to '*', operator would watch all namespaces.")
          .typeParameterClass(String.class)
          .defaultValue("default")
          .build();

  public static final ConfigOption<Boolean> TERMINATE_ON_INFORMER_FAILURE_ENABLED =
      ConfigOption.<Boolean>builder()
          .key("spark.kubernetes.operator.terminateOnInformerFailureEnabled")
          .enableDynamicOverride(false)
          .description(
              "Enable to indicate informer errors should stop operator startup. If "
                  + "disabled, operator startup will ignore recoverable errors, "
                  + "caused for example by RBAC issues and will retry "
                  + "periodically.")
          .typeParameterClass(Boolean.class)
          .defaultValue(false)
          .build();

  public static final ConfigOption<Integer> RECONCILER_TERMINATION_TIMEOUT_SECONDS =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.reconciler.terminationTimeoutSeconds")
          .enableDynamicOverride(false)
          .description(
              "Grace period for operator shutdown before reconciliation threads are killed.")
          .typeParameterClass(Integer.class)
          .defaultValue(30)
          .build();

  public static final ConfigOption<Integer> RECONCILER_PARALLELISM =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.reconciler.parallelism")
          .enableDynamicOverride(false)
          .description(
              "Thread pool size for Spark Operator reconcilers. Unbounded pool would be used if "
                  + "set to non-positive number.")
          .typeParameterClass(Integer.class)
          .defaultValue(50)
          .build();

  public static final ConfigOption<Long> RECONCILER_FOREGROUND_REQUEST_TIMEOUT_SECONDS =
      ConfigOption.<Long>builder()
          .key("spark.kubernetes.operator.reconciler.foregroundRequestTimeoutSeconds")
          .enableDynamicOverride(true)
          .description(
              "Timeout (in seconds) to for requests made to API server. This "
                  + "applies only to foreground requests.")
          .typeParameterClass(Long.class)
          .defaultValue(30L)
          .build();

  public static final ConfigOption<Long> RECONCILER_INTERVAL_SECONDS =
      ConfigOption.<Long>builder()
          .key("spark.kubernetes.operator.reconciler.intervalSeconds")
          .enableDynamicOverride(true)
          .description(
              "Interval (in seconds, non-negative) to reconcile Spark applications. Note that "
                  + "reconciliation is always expected to be triggered when app spec / status is "
                  + "updated. This interval controls the reconcile behavior of operator "
                  + "reconciliation even when there's no update on SparkApplication, e.g. to "
                  + "determine whether a hanging app needs to be proactively terminated. Thus "
                  + "this is recommended to set to above 2 minutes to avoid unnecessary no-op "
                  + "reconciliation.")
          .typeParameterClass(Long.class)
          .defaultValue(120L)
          .build();

  public static final ConfigOption<Boolean> TRIM_ATTEMPT_STATE_TRANSITION_HISTORY =
      ConfigOption.<Boolean>builder()
          .key("spark.kubernetes.operator.reconciler.trimStateTransitionHistoryEnabled")
          .enableDynamicOverride(true)
          .description(
              "When enabled, operator would trim state transition history when a "
                  + "new attempt starts, keeping previous attempt summary only.")
          .typeParameterClass(Boolean.class)
          .defaultValue(true)
          .build();

  public static final ConfigOption<String> SPARK_APP_STATUS_LISTENER_CLASS_NAMES =
      ConfigOption.<String>builder()
          .key("spark.kubernetes.operator.reconciler.appStatusListenerClassNames")
          .enableDynamicOverride(false)
          .description("Comma-separated names of SparkAppStatusListener class implementations")
          .typeParameterClass(String.class)
          .defaultValue("")
          .build();

  public static final ConfigOption<Boolean> DYNAMIC_CONFIG_ENABLED =
      ConfigOption.<Boolean>builder()
          .key("spark.kubernetes.operator.dynamicConfig.enabled")
          .enableDynamicOverride(false)
          .description(
              "When enabled, operator would use config map as source of truth for config "
                  + "property override. The config map need to be created in "
                  + "spark.kubernetes.operator.namespace, and labeled with operator name.")
          .typeParameterClass(Boolean.class)
          .defaultValue(false)
          .build();

  public static final ConfigOption<String> DYNAMIC_CONFIG_SELECTOR =
      ConfigOption.<String>builder()
          .key("spark.kubernetes.operator.dynamicConfig.selector")
          .enableDynamicOverride(false)
          .description("The selector str applied to dynamic config map.")
          .typeParameterClass(String.class)
          .defaultValue(Utils.labelsAsStr(Utils.defaultOperatorConfigLabels()))
          .build();

  public static final ConfigOption<Integer> DYNAMIC_CONFIG_RECONCILER_PARALLELISM =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.dynamicConfig.reconcilerParallelism")
          .enableDynamicOverride(false)
          .description(
              "Parallelism for dynamic config reconciler. Unbounded pool would be used "
                  + "if set to non-positive number.")
          .typeParameterClass(Integer.class)
          .defaultValue(1)
          .build();

  public static final ConfigOption<Integer> RECONCILER_RATE_LIMITER_REFRESH_PERIOD_SECONDS =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.reconciler.rateLimiter.refreshPeriodSeconds")
          .enableDynamicOverride(false)
          .description("Operator rate limiter refresh period(in seconds) for each resource.")
          .typeParameterClass(Integer.class)
          .defaultValue(15)
          .build();

  public static final ConfigOption<Integer> RECONCILER_RATE_LIMITER_MAX_LOOP_FOR_PERIOD =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.reconciler.rateLimiter.maxLoopForPeriod")
          .enableDynamicOverride(false)
          .description(
              "Max number of reconcile loops triggered within the rate limiter refresh "
                  + "period for each resource. Setting the limit <= 0 disables the "
                  + "limiter.")
          .typeParameterClass(Integer.class)
          .defaultValue(5)
          .build();

  public static final ConfigOption<Integer> RECONCILER_RETRY_INITIAL_INTERVAL_SECONDS =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.reconciler.retry.initialIntervalSeconds")
          .enableDynamicOverride(false)
          .description("Initial interval(in seconds) of retries on unhandled controller errors.")
          .typeParameterClass(Integer.class)
          .defaultValue(5)
          .build();

  public static final ConfigOption<Double> RECONCILER_RETRY_INTERVAL_MULTIPLIER =
      ConfigOption.<Double>builder()
          .key("spark.kubernetes.operator.reconciler.retry.intervalMultiplier")
          .enableDynamicOverride(false)
          .description(
              "Interval multiplier of retries on unhandled controller errors. Setting "
                  + "this to 1 for linear retry.")
          .typeParameterClass(Double.class)
          .defaultValue(1.5)
          .build();

  public static final ConfigOption<Integer> RECONCILER_RETRY_MAX_INTERVAL_SECONDS =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.reconciler.retry.maxIntervalSeconds")
          .enableDynamicOverride(false)
          .description(
              "Max interval(in seconds) of retries on unhandled controller errors. "
                  + "Set to non-positive for unlimited.")
          .typeParameterClass(Integer.class)
          .defaultValue(-1)
          .build();

  public static final ConfigOption<Integer> API_RETRY_MAX_ATTEMPTS =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.api.retryMaxAttempts")
          .enableDynamicOverride(false)
          .description(
              "Max attempts of retries on unhandled controller errors. Setting this to "
                  + "non-positive value means no retry.")
          .typeParameterClass(Integer.class)
          .defaultValue(15)
          .build();

  public static final ConfigOption<Long> API_RETRY_ATTEMPT_AFTER_SECONDS =
      ConfigOption.<Long>builder()
          .key("spark.kubernetes.operator.api.retryAttemptAfterSeconds")
          .enableDynamicOverride(false)
          .description(
              "Default time (in seconds) to wait till next request. This would be used if "
                  + "server does not set Retry-After in response. Setting this to non-positive "
                  + "number means immediate retry.")
          .typeParameterClass(Long.class)
          .defaultValue(1L)
          .build();

  public static final ConfigOption<Long> API_STATUS_PATCH_MAX_ATTEMPTS =
      ConfigOption.<Long>builder()
          .key("spark.kubernetes.operator.api.statusPatchMaxAttempts")
          .enableDynamicOverride(false)
          .description(
              "Maximal number of retry attempts of requests to k8s server for resource "
                  + "status update. This would be performed on top of k8s client "
                  + "spark.kubernetes.operator.retry.maxAttempts to overcome potential "
                  + "conflicting update on the same SparkApplication. This should be positive "
                  + "number.")
          .typeParameterClass(Long.class)
          .defaultValue(3L)
          .build();

  public static final ConfigOption<Long> API_SECONDARY_RESOURCE_CREATE_MAX_ATTEMPTS =
      ConfigOption.<Long>builder()
          .key("spark.kubernetes.operator.api.secondaryResourceCreateMaxAttempts")
          .enableDynamicOverride(false)
          .description(
              "Maximal number of retry attempts of requesting secondary resource for Spark "
                  + "application. This would be performed on top of k8s client "
                  + "spark.kubernetes.operator.retry.maxAttempts to overcome potential "
                  + "conflicting reconcile on the same SparkApplication. This should be "
                  + "positive number")
          .typeParameterClass(Long.class)
          .defaultValue(3L)
          .build();

  public static final ConfigOption<Boolean> JOSDK_METRICS_ENABLED =
      ConfigOption.<Boolean>builder()
          .key("spark.kubernetes.operator.metrics.josdkMetricsEnabled")
          .enableDynamicOverride(false)
          .description(
              "When enabled, the josdk metrics will be added in metrics source and "
                  + "configured for operator.")
          .typeParameterClass(Boolean.class)
          .defaultValue(true)
          .build();

  public static final ConfigOption<Boolean> KUBERNETES_CLIENT_METRICS_ENABLED =
      ConfigOption.<Boolean>builder()
          .key("spark.kubernetes.operator.metrics.clientMetricsEnabled")
          .enableDynamicOverride(false)
          .description(
              "Enable KubernetesClient metrics for measuring the HTTP traffic to "
                  + "the Kubernetes API Server. Since the metrics is collected "
                  + "via Okhttp interceptors, can be disabled when opt in "
                  + "customized interceptors.")
          .typeParameterClass(Boolean.class)
          .defaultValue(true)
          .build();

  public static final ConfigOption<Boolean>
      KUBERNETES_CLIENT_METRICS_GROUP_BY_RESPONSE_CODE_GROUP_ENABLED =
          ConfigOption.<Boolean>builder()
              .key("spark.kubernetes.operator.metrics.clientMetricsGroupByResponseCodeEnabled")
              .enableDynamicOverride(false)
              .description(
                  "When enabled, additional metrics group by http response code group(1xx, "
                      + "2xx, 3xx, 4xx, 5xx) received from API server will be added. Users "
                      + "can disable it when their monitoring system can combine lower level "
                      + "kubernetes.client.http.response.<3-digit-response-code> metrics.")
              .typeParameterClass(Boolean.class)
              .defaultValue(true)
              .build();

  public static final ConfigOption<Integer> OPERATOR_METRICS_PORT =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.metrics.port")
          .enableDynamicOverride(false)
          .description("The port used for checking metrics")
          .typeParameterClass(Integer.class)
          .defaultValue(19090)
          .build();

  public static final ConfigOption<Integer> OPERATOR_PROBE_PORT =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.health.probePort")
          .enableDynamicOverride(false)
          .description("The port used for health/readiness check probe status.")
          .typeParameterClass(Integer.class)
          .defaultValue(19091)
          .build();

  public static final ConfigOption<Integer> SENTINEL_EXECUTOR_SERVICE_POOL_SIZE =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.health.sentinelExecutorPoolSize")
          .typeParameterClass(Integer.class)
          .description(
              "Size of executor service in Sentinel Managers to check the health "
                  + "of sentinel resources.")
          .defaultValue(3)
          .enableDynamicOverride(false)
          .build();

  public static final ConfigOption<Integer> SENTINEL_RESOURCE_RECONCILIATION_DELAY =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.health.sentinelResourceReconciliationDelaySeconds")
          .enableDynamicOverride(true)
          .description(
              "Allowed max time(seconds) between spec update and reconciliation "
                  + "for sentinel resources.")
          .typeParameterClass(Integer.class)
          .defaultValue(60)
          .build();

  public static final ConfigOption<Boolean> LEADER_ELECTION_ENABLED =
      ConfigOption.<Boolean>builder()
          .key("spark.kubernetes.operator.leaderElection.enabled")
          .enableDynamicOverride(false)
          .description(
              "Enable leader election for the operator to allow running standby instances. When "
                  + "this is disabled, only one operator instance is expected to be up and "
                  + "running at any time (replica = 1) to avoid race condition.")
          .typeParameterClass(Boolean.class)
          .defaultValue(false)
          .build();

  public static final ConfigOption<String> LEADER_ELECTION_LEASE_NAME =
      ConfigOption.<String>builder()
          .key("spark.kubernetes.operator.leaderElection.leaseName")
          .enableDynamicOverride(false)
          .description(
              "Leader election lease name, must be unique for leases in the same namespace.")
          .typeParameterClass(String.class)
          .defaultValue("spark-operator-lease")
          .build();

  public static final ConfigOption<Integer> LEADER_ELECTION_LEASE_DURATION_SECONDS =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.leaderElection.leaseDurationSeconds")
          .enableDynamicOverride(false)
          .description("Leader election lease duration in seconds, non-negative.")
          .typeParameterClass(Integer.class)
          .defaultValue(180)
          .build();

  public static final ConfigOption<Integer> LEADER_ELECTION_RENEW_DEADLINE_SECONDS =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.leaderElection.renewDeadlineSeconds")
          .enableDynamicOverride(false)
          .description(
              "Leader election renew deadline in seconds, non-negative. This needs to be "
                  + "smaller than the lease duration to allow current leader renew the lease "
                  + "before lease expires.")
          .typeParameterClass(Integer.class)
          .defaultValue(120)
          .build();

  public static final ConfigOption<Integer> LEADER_ELECTION_RETRY_PERIOD_SECONDS =
      ConfigOption.<Integer>builder()
          .key("spark.kubernetes.operator.leaderElection.retryPeriodSeconds")
          .enableDynamicOverride(false)
          .description("Leader election retry period in seconds, non-negative.")
          .typeParameterClass(Integer.class)
          .defaultValue(5)
          .build();

  public static LeaderElectionConfiguration getLeaderElectionConfig() {
    return new LeaderElectionConfiguration(
        LEADER_ELECTION_LEASE_NAME.getValue(),
        OPERATOR_NAMESPACE.getValue(),
        Duration.ofSeconds(ensurePositiveIntFor(LEADER_ELECTION_LEASE_DURATION_SECONDS)),
        Duration.ofSeconds(ensurePositiveIntFor(LEADER_ELECTION_RENEW_DEADLINE_SECONDS)),
        Duration.ofSeconds(ensurePositiveIntFor(LEADER_ELECTION_RETRY_PERIOD_SECONDS)));
  }

  public static GenericRetry getOperatorRetry() {
    GenericRetry genericRetry =
        new GenericRetry()
            .setMaxAttempts(ensureNonNegativeIntFor(API_RETRY_MAX_ATTEMPTS))
            .setInitialInterval(
                Duration.ofSeconds(
                        ensureNonNegativeIntFor(RECONCILER_RETRY_INITIAL_INTERVAL_SECONDS))
                    .toMillis())
            .setIntervalMultiplier(RECONCILER_RETRY_INTERVAL_MULTIPLIER.getValue());
    if (RECONCILER_RETRY_MAX_INTERVAL_SECONDS.getValue() > 0) {
      genericRetry.setMaxInterval(
          Duration.ofSeconds(RECONCILER_RETRY_MAX_INTERVAL_SECONDS.getValue()).toMillis());
    } else {
      log.info("Reconciler retry policy is configured with unlimited max attempts");
    }
    return genericRetry;
  }

  public static RateLimiter<?> getOperatorRateLimiter() {
    return new LinearRateLimiter(
        Duration.ofSeconds(ensureNonNegativeIntFor(RECONCILER_RATE_LIMITER_REFRESH_PERIOD_SECONDS)),
        ensureNonNegativeIntFor(RECONCILER_RATE_LIMITER_MAX_LOOP_FOR_PERIOD));
  }

  private static int ensureNonNegativeIntFor(ConfigOption<Integer> configOption) {
    return ensureValid(configOption.getValue(), configOption.getDescription(), 0, 0);
  }

  private static int ensurePositiveIntFor(ConfigOption<Integer> configOption) {
    return ensureValid(configOption.getValue(), configOption.getDescription(), 1, 1);
  }

  private static int ensureValid(int value, String description, int minValue, int defaultValue) {
    if (value < minValue) {
      if (defaultValue < minValue) {
        throw new IllegalArgumentException(
            "Default value for " + description + " must be greater than " + minValue);
      }
      log.warn(
          "Requested {} should be greater than {}. Requested: {}, using {} (default) instead",
          description,
          minValue,
          value,
          defaultValue);
      return defaultValue;
    }
    return value;
  }
}
