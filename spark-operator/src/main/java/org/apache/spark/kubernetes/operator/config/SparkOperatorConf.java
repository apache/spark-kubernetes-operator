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

package org.apache.spark.kubernetes.operator.config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import io.javaoperatorsdk.operator.api.config.LeaderElectionConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import org.apache.spark.kubernetes.operator.listeners.ApplicationStatusListener;

import static org.apache.spark.kubernetes.operator.reconciler.SparkReconcilerUtils.defaultOperatorConfigLabels;
import static org.apache.spark.kubernetes.operator.reconciler.SparkReconcilerUtils.labelsAsStr;

/**
 * Spark Operator Configuration options.
 */
@Slf4j
public class SparkOperatorConf {
  public static final String METRIC_PREFIX = "spark.metrics.conf.operator.";
  public static final String SINK = "sink.";
  public static final String CLASS = "class";

  public static final ConfigOption<String> OperatorAppName = ConfigOption.<String>builder()
      .key("spark.operator.name")
      .typeParameterClass(String.class)
      .description("Name of the operator.")
      .defaultValue("spark-kubernetes-operator")
      .enableDynamicOverride(false)
      .build();
  public static final ConfigOption<String> OperatorNamespace = ConfigOption.<String>builder()
      .key("spark.operator.namespace")
      .typeParameterClass(String.class)
      .description("Namespace that operator is deployed within.")
      .defaultValue("spark-system")
      .enableDynamicOverride(false)
      .build();
  public static final ConfigOption<Boolean> DynamicConfigEnabled = ConfigOption.<Boolean>builder()
      .key("spark.operator.dynamic.config.enabled")
      .typeParameterClass(Boolean.class)
      .description(
          "When enabled, operator would use config map as source of truth for config " +
              "property override. The config map need to be created in " +
              "spark.operator.namespace, and labeled with operator name.")
      .defaultValue(false)
      .enableDynamicOverride(false)
      .build();
  public static final ConfigOption<String> DynamicConfigSelectorStr =
      ConfigOption.<String>builder()
          .key("spark.operator.dynamic.config.selector.str")
          .typeParameterClass(String.class)
          .description("The selector str applied to dynamic config map.")
          .defaultValue(labelsAsStr(defaultOperatorConfigLabels()))
          .enableDynamicOverride(false)
          .build();
  public static final ConfigOption<Boolean> TerminateOnInformerFailure =
      ConfigOption.<Boolean>builder()
          .key("spark.operator.terminate.on.informer.failure")
          .typeParameterClass(Boolean.class)
          .description(
              "Enable to indicate informer errors should stop operator startup. If " +
                  "disabled, operator startup will ignore recoverable errors, " +
                  "caused for example by RBAC issues and will retry " +
                  "periodically.")
          .defaultValue(false)
          .enableDynamicOverride(false)
          .build();
  public static final ConfigOption<Integer> TerminationTimeoutSeconds =
      ConfigOption.<Integer>builder()
          .key("spark.operator.termination.timeout.seconds")
          .description(
              "Grace period for operator shutdown before reconciliation threads " +
                  "are killed.")
          .enableDynamicOverride(false)
          .typeParameterClass(Integer.class)
          .defaultValue(30)
          .build();
  public static final ConfigOption<Integer> ReconcilerParallelism =
      ConfigOption.<Integer>builder()
          .key("spark.operator.reconciler.parallelism")
          .description(
              "Thread pool size for Spark Operator reconcilers. Use -1 for " +
                  "unbounded pool.")
          .enableDynamicOverride(false)
          .typeParameterClass(Integer.class)
          .defaultValue(30)
          .build();
  public static final ConfigOption<Integer> RateLimiterRefreshPeriodSeconds =
      ConfigOption.<Integer>builder()
          .key("spark.operator.rate.limiter.refresh.period.seconds")
          .description(
              "Operator rate limiter refresh period(in seconds) for each resource.")
          .enableDynamicOverride(false)
          .typeParameterClass(Integer.class)
          .defaultValue(15)
          .build();
  public static final ConfigOption<Integer> RateLimiterLimit = ConfigOption.<Integer>builder()
      .key("spark.operator.rate.limiter.limit")
      .description(
          "Max number of reconcile loops triggered within the rate limiter refresh " +
              "period for each resource. Setting the limit <= 0 disables the " +
              "limiter.")
      .enableDynamicOverride(false)
      .typeParameterClass(Integer.class)
      .defaultValue(5)
      .build();
  public static final ConfigOption<Integer> RetryInitialInternalSeconds =
      ConfigOption.<Integer>builder()
          .key("spark.operator.retry.initial.internal.seconds")
          .description(
              "Initial interval(in seconds) of retries on unhandled controller " +
                  "errors.")
          .enableDynamicOverride(false)
          .typeParameterClass(Integer.class)
          .defaultValue(5)
          .build();
  public static final ConfigOption<Double> RetryInternalMultiplier =
      ConfigOption.<Double>builder()
          .key("spark.operator.retry.internal.multiplier")
          .description("Interval multiplier of retries on unhandled controller errors.")
          .enableDynamicOverride(false)
          .typeParameterClass(Double.class)
          .defaultValue(1.5)
          .build();
  public static final ConfigOption<Integer> RetryMaxIntervalSeconds =
      ConfigOption.<Integer>builder()
          .key("spark.operator.retry.max.interval.seconds")
          .description(
              "Max interval(in seconds) of retries on unhandled controller errors. " +
                  "Set to -1 for unlimited.")
          .enableDynamicOverride(false)
          .typeParameterClass(Integer.class)
          .defaultValue(-1)
          .build();
  public static final ConfigOption<Integer> RetryMaxAttempts = ConfigOption.<Integer>builder()
      .key("spark.operator.retry.max.attempts")
      .description("Max attempts of retries on unhandled controller errors.")
      .enableDynamicOverride(false)
      .typeParameterClass(Integer.class)
      .defaultValue(15)
      .build();
  public static final ConfigOption<Long> DriverCreateMaxAttempts = ConfigOption.<Long>builder()
      .key("spark.operator.driver.create.max.attempts")
      .description(
          "Maximal number of retry attempts of requesting driver for Spark application.")
      .defaultValue(3L)
      .typeParameterClass(Long.class)
      .build();
  public static final ConfigOption<Long> MaxRetryAttemptOnKubeServerFailure =
      ConfigOption.<Long>builder()
          .key("spark.operator.max.retry.attempts.on.k8s.failure")
          .description(
              "Maximal number of retry attempts of requests to k8s server upon " +
                  "response 429 and 5xx.")
          .defaultValue(3L)
          .typeParameterClass(Long.class)
          .build();
  public static final ConfigOption<Long> RetryAttemptAfterSeconds = ConfigOption.<Long>builder()
      .key("spark.operator.retry.attempt.after.seconds")
      .description(
          "Default time (in seconds) to wait till next request. This would be used if " +
              "server does not set Retry-After in response.")
      .defaultValue(1L)
      .typeParameterClass(Long.class)
      .build();
  public static final ConfigOption<Long> MaxRetryAttemptAfterSeconds =
      ConfigOption.<Long>builder()
          .key("spark.operator.max.retry.attempt.after.seconds")
          .description("Maximal time (in seconds) to wait till next request.")
          .defaultValue(15L)
          .typeParameterClass(Long.class)
          .build();
  public static final ConfigOption<Long> StatusPatchMaxRetry = ConfigOption.<Long>builder()
      .key("spark.operator.status.patch.max.retry")
      .description(
          "Maximal number of retry attempts of requests to k8s server for resource " +
              "status update.")
      .defaultValue(3L)
      .typeParameterClass(Long.class)
      .build();
  public static final ConfigOption<Long> StatusPatchFailureBackoffSeconds =
      ConfigOption.<Long>builder()
          .key("spark.operator.status.patch.failure.backoff.seconds")
          .description(
              "Default time (in seconds) to wait till next request to patch " +
                  "resource status update.")
          .defaultValue(3L)
          .typeParameterClass(Long.class)
          .build();
  public static final ConfigOption<Long> AppReconcileIntervalSeconds =
      ConfigOption.<Long>builder()
          .key("spark.operator.app.reconcile.interval.seconds")
          .description(
              "Interval (in seconds) to reconcile when application is is starting " +
                  "up. Note that reconcile is always expected to be triggered " +
                  "per update - this interval controls the reconcile behavior " +
                  "when operator still need to reconcile even when there's no " +
                  "update ,e.g. for timeout checks.")
          .defaultValue(120L)
          .typeParameterClass(Long.class)
          .build();
  public static final ConfigOption<Long> ForegroundRequestTimeoutSeconds =
      ConfigOption.<Long>builder()
          .key("spark.operator.foreground.request.timeout.seconds")
          .description(
              "Timeout (in seconds) to for requests made to API server. this " +
                  "applies only to foreground requests.")
          .defaultValue(120L)
          .typeParameterClass(Long.class)
          .build();
  public static final ConfigOption<String> OperatorWatchedNamespaces =
      ConfigOption.<String>builder()
          .key("spark.operator.watched.namespaces")
          .description(
              "Comma-separated list of namespaces that the operator would be " +
                  "watching for Spark resources. If unset, operator would " +
                  "watch all namespaces by default.")
          .defaultValue(null)
          .typeParameterClass(String.class)
          .build();
  public static final ConfigOption<Boolean> TrimAttemptStateTransitionHistory =
      ConfigOption.<Boolean>builder()
          .key("spark.operator.trim.attempt.state.transition.history")
          .description(
              "When enabled, operator would trim state transition history when a " +
                  "new attempt starts, keeping previous attempt summary only.")
          .defaultValue(true)
          .typeParameterClass(Boolean.class)
          .build();

  public static final ConfigOption<Boolean> JOSDKMetricsEnabled = ConfigOption.<Boolean>builder()
      .key("spark.operator.josdk.metrics.enabled")
      .description(
          "When enabled, the josdk metrics will be added in metrics source and " +
              "configured for operator.")
      .defaultValue(true)
      .build();

  public static final ConfigOption<Boolean> KubernetesClientMetricsEnabled =
      ConfigOption.<Boolean>builder()
          .key("spark.operator.kubernetes.client.metrics.enabled")
          .defaultValue(true)
          .description(
              "Enable KubernetesClient metrics for measuring the HTTP traffic to " +
                  "the Kubernetes API Server. Since the metrics is collected " +
                  "via Okhttp interceptors, can be disabled when opt in " +
                  "customized interceptors.")
          .build();

  public static final ConfigOption<Boolean>
      KubernetesClientMetricsGroupByResponseCodeGroupEnabled = ConfigOption.<Boolean>builder()
      .key("spark.operator.kubernetes.client.metrics.group.by.response.code.group.enable")
      .description(
          "When enabled, additional metrics group by http response code group(1xx, " +
              "2xx, 3xx, 4xx, 5xx) received from API server will be added. Users " +
              "can disable it when their monitoring system can combine lower level " +
              "kubernetes.client.http.response.<3-digit-response-code> metrics.")
      .defaultValue(true)
      .build();
  public static final ConfigOption<Integer> OperatorProbePort = ConfigOption.<Integer>builder()
      .key("spark.operator.probe.port")
      .defaultValue(18080)
      .description("The port used for health/readiness check probe status.")
      .typeParameterClass(Integer.class)
      .enableDynamicOverride(false)
      .build();

  public static final ConfigOption<Integer> OperatorMetricsPort = ConfigOption.<Integer>builder()
      .key("spark.operator.metrics.port")
      .defaultValue(19090)
      .description("The port used for checking metrics")
      .typeParameterClass(Integer.class)
      .enableDynamicOverride(false)
      .build();

  public static final ConfigOption<Integer> SentinelExecutorServicePoolSize =
      ConfigOption.<Integer>builder()
          .key("spark.operator.sentinel.executor.pool.size")
          .description(
              "Size of executor service in Sentinel Managers to check the health " +
                  "of sentinel resources.")
          .defaultValue(3)
          .enableDynamicOverride(false)
          .typeParameterClass(Integer.class)
          .build();

  public static final ConfigOption<Long> SENTINEL_RESOURCE_RECONCILIATION_DELAY =
      ConfigOption.<Long>builder()
          .key("spark.operator.health.sentinel.resource.reconciliation.delay.seconds")
          .defaultValue(60L)
          .description(
              "Allowed max time(seconds) between spec update and reconciliation " +
                  "for sentinel resources.")
          .enableDynamicOverride(true)
          .typeParameterClass(Long.class)
          .build();
  public static final ConfigOption<String> APPLICATION_STATUS_LISTENER_CLASS_NAMES =
      ConfigOption.<String>builder()
          .key("spark.operator.application.status.listener.class.names")
          .defaultValue("")
          .description(
              "Comma-separated names of ApplicationStatusListener class " +
                  "implementations")
          .enableDynamicOverride(false)
          .typeParameterClass(String.class)
          .build();
  public static final ConfigOption<Boolean> LEADER_ELECTION_ENABLED =
      ConfigOption.<Boolean>builder()
          .key("spark.operator.leader.election.enabled")
          .defaultValue(false)
          .description(
              "Enable leader election for the operator to allow running standby " +
                  "instances.")
          .enableDynamicOverride(false)
          .typeParameterClass(Boolean.class)
          .build();
  public static final ConfigOption<String> LEADER_ELECTION_LEASE_NAME =
      ConfigOption.<String>builder()
          .key("spark.operator.leader.election.lease.name")
          .defaultValue("spark-operator-lease")
          .description(
              "Leader election lease name, must be unique for leases in the same " +
                  "namespace.")
          .enableDynamicOverride(false)
          .typeParameterClass(String.class)
          .build();
  public static final ConfigOption<Long> LEADER_ELECTION_LEASE_DURATION_SECONDS =
      ConfigOption.<Long>builder()
          .key("spark.operator.leader.election.lease.duration.seconds")
          .defaultValue(1200L)
          .description("Leader election lease duration.")
          .enableDynamicOverride(false)
          .typeParameterClass(Long.class)
          .build();
  public static final ConfigOption<Long> LEADER_ELECTION_RENEW_DEADLINE_SECONDS =
      ConfigOption.<Long>builder()
          .key("spark.operator.leader.election.renew.deadline.seconds")
          .defaultValue(600L)
          .description("Leader election renew deadline.")
          .enableDynamicOverride(false)
          .typeParameterClass(Long.class)
          .build();
  public static final ConfigOption<Long> LEADER_ELECTION_RETRY_PERIOD_SECONDS =
      ConfigOption.<Long>builder()
          .key("spark.operator.leader.election.retry.period.seconds")
          .defaultValue(180L)
          .description("Leader election retry period.")
          .enableDynamicOverride(false)
          .typeParameterClass(Long.class)
          .build();

  public static List<ApplicationStatusListener> getApplicationStatusListener() {
    List<ApplicationStatusListener> listeners = new ArrayList<>();
    String listenerNamesStr =
        SparkOperatorConf.APPLICATION_STATUS_LISTENER_CLASS_NAMES.getValue();
    if (StringUtils.isNotBlank(listenerNamesStr)) {
      try {
        List<String> listenerNames =
            Arrays.stream(listenerNamesStr.split(",")).map(String::trim)
                .collect(Collectors.toList());
        for (String name : listenerNames) {
          Class listenerClass = Class.forName(name);
          if (ApplicationStatusListener.class.isAssignableFrom(listenerClass)) {
            listeners.add((ApplicationStatusListener)
                listenerClass.getConstructor().newInstance());
          }
        }
      } catch (Exception e) {
        if (log.isErrorEnabled()) {
          log.error("Failed to initialize listeners for operator with {}",
              listenerNamesStr, e);
        }
      }
    }
    return listeners;
  }

  public static LeaderElectionConfiguration getLeaderElectionConfig() {
    return new LeaderElectionConfiguration(LEADER_ELECTION_LEASE_NAME.getValue(),
        OperatorNamespace.getValue(),
        Duration.ofSeconds(LEADER_ELECTION_LEASE_DURATION_SECONDS.getValue()),
        Duration.ofSeconds(LEADER_ELECTION_RENEW_DEADLINE_SECONDS.getValue()),
        Duration.ofSeconds(LEADER_ELECTION_RETRY_PERIOD_SECONDS.getValue()));
  }
}
