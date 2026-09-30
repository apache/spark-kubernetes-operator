<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Migration Guide

This document lists the behavior changes of Apache Spark K8s Operator releases which may affect an
existing deployment, and how to restore the previous behavior where possible. When upgrading across
several releases, read every section in between.

New features which are disabled by default are not listed, and neither are changes which only
affect building the project, CI, tests or examples.

## Upgrading from 1.0 to 1.1

### Kubernetes and CRDs

- Since 1.1.0, K8s 1.35 or newer is recommended instead of 1.34 ([SPARK-59692](https://issues.apache.org/jira/browse/SPARK-59692)).
- Since 1.1.0, the CRDs have `spec.suspend` for `SparkApplication` and `SparkCluster`, and the
  `Suspended` state for `SparkCluster`. Since `helm upgrade` does not update CRDs, replace them
  before upgrading the operator, e.g. with the CRDs of the Helm chart `1.9.0`, which deploys
  1.1.0. Otherwise, the API server rejects or drops `spec.suspend`, and rejects the `Suspended`
  state ([SPARK-59475](https://issues.apache.org/jira/browse/SPARK-59475), [SPARK-59750](https://issues.apache.org/jira/browse/SPARK-59750)).

  ```bash
  helm repo update
  helm pull spark/spark-kubernetes-operator --version 1.9.0 --untar
  kubectl replace -f spark-kubernetes-operator/crds/
  ```

### Helm Chart

- Since 1.1.0, `helm upgrade --reuse-values` from chart `1.8.0` fails, since it renders the new
  chart with the default values of chart `1.8.0`, which lack
  `operatorDeployment.networkPolicy.enabled`, `operatorConfiguration.dynamicConfig.enabled` and
  `operatorRbac.kueue` that the chart schema now requires. Use `--reset-then-reuse-values` of Helm
  3.14 or newer, or pass the values file again with `-f` ([SPARK-59504](https://issues.apache.org/jira/browse/SPARK-59504), [SPARK-59519](https://issues.apache.org/jira/browse/SPARK-59519)).
- Since 1.1.0, `operatorDeployment.networkPolicy.enable` and
  `operatorConfiguration.dynamicConfig.enable` are deprecated in favor of `enabled`, and
  `helm install` and `helm upgrade` print a warning for them. They are still honored until chart
  `2.0.0`: a feature is enabled when either key is `true`, so a legacy `enable: true` must be
  removed to disable it ([SPARK-59504](https://issues.apache.org/jira/browse/SPARK-59504), [SPARK-59540](https://issues.apache.org/jira/browse/SPARK-59540)).
- Since 1.1.0, `operatorRbac.configManagement.create: false` takes effect. The chart then no longer
  creates the Role and RoleBinding for the `configMap` source of the dynamic config, which 1.0
  created regardless. To restore the behavior before 1.1.0, set it to `true`, the default
  ([SPARK-59537](https://issues.apache.org/jira/browse/SPARK-59537)).
- Since 1.1.0, the operator ClusterRole and Roles grant only `get`, `create` and `patch` on
  `events`, which are the verbs the operator uses. 1.0 also granted `list`, `watch`, `update` and
  `delete`. If other workloads use the operator ServiceAccount and need them, grant them with
  another Role or ClusterRole ([SPARK-59587](https://issues.apache.org/jira/browse/SPARK-59587)).
- Since 1.1.0, each option of `operatorDeployment.operatorPod.operatorContainer.jvmArgs` is passed
  to the operator JVM as its own argument. 1.0 passed the whole value as one argument, which the
  JVM took as a single `-Dfile.encoding` system property, so the other options had no effect. The
  default options now take effect, e.g. the Parallel GC and a heap of 80% of the container memory
  limit (`2Gi` by default), fully touched at startup, instead of at most 25%. An
  `OutOfMemoryError` now crashes the JVM, so the container restarts. Keep a memory limit on the
  operator container, since the percentages apply to the node memory otherwise. The options also
  take precedence over `JAVA_TOOL_OPTIONS` and `JDK_JAVA_OPTIONS` in `operatorContainer.env`, and
  cannot contain a space, since the value is split on spaces. To restore the behavior before
  1.1.0, set `jvmArgs` to `-Dfile.encoding=UTF8` ([SPARK-58426](https://issues.apache.org/jira/browse/SPARK-58426)).

### Operator

- Since 1.1.0, a non-positive `spark.kubernetes.operator.reconciler.retry.maxIntervalSeconds`,
  which is the default, leaves the interval between retries on reconciliation errors unlimited as
  documented. 1.0 capped it at about 15 seconds, so with the other defaults, the retries now span
  about 49 minutes instead of about 3 minutes. To restore the behavior before 1.1.0, set it to
  `15` ([SPARK-59892](https://issues.apache.org/jira/browse/SPARK-59892)).

### SparkApplication

- Since 1.1.0, when the operator retries creating the driver resources of an application, it waits
  as long as the `Retry-After` hint of the API server asks, capped at
  `spark.kubernetes.operator.api.secondaryResourceCreateMaxBackoffMillis`. 1.0 ignored the hint,
  and retried errors other than `409` and `429`, e.g. `503`, right away. A request which the API
  server never answered, e.g. on a connection reset or a timeout, is retried with exponential
  backoff too, while 1.0 failed the attempt with `SchedulingFailure` at once. The number of
  attempts is still bounded by `spark.kubernetes.operator.api.secondaryResourceCreateMaxAttempts`
  ([SPARK-59092](https://issues.apache.org/jira/browse/SPARK-59092), [SPARK-59694](https://issues.apache.org/jira/browse/SPARK-59694)).
- Since 1.1.0, the duration of an attempt which is compared with `restartCounterResetMillis` is
  measured from the first state after `Submitted` or `ScheduledToRestart`, normally
  `DriverRequested`, instead of from `Submitted` or `ScheduledToRestart`. The restart backoff no
  longer counts, so an attempt which reset the restart counters in 1.0 may not reset them, and the
  application may reach `maxRestartAttempts` earlier. To keep a similar threshold, decrease
  `restartCounterResetMillis` by the restart backoff, e.g. `restartBackoffMillis`
  ([SPARK-59542](https://issues.apache.org/jira/browse/SPARK-59542)).
- Since 1.1.0, an application which is configured to restart ends in `ResourceReleased` instead of
  `TerminatedWithoutReleaseResources` after its last attempt, even if its `resourceRetainPolicy`
  retains the resources, since they are released at the end of every attempt. So
  `resourceRetainDurationMillis` no longer applies to it, while `ttlAfterStopMillis` still deletes
  it at the same time as in 1.0. A tool which waits for `TerminatedWithoutReleaseResources` has to
  wait for `ResourceReleased` instead ([SPARK-59732](https://issues.apache.org/jira/browse/SPARK-59732)).
- Since 1.1.0, an application whose driver pod fails before any of its containers starts, e.g.
  when the pod is evicted or its node is deleted, moves to `Failed`, or `DriverEvicted` if it is
  evicted, right away. 1.0 kept it in `DriverRequested` until `DriverStartTimedOut`, or forever if
  `driverStartTimeoutMillis` was disabled. Since `DriverStartTimedOut` is an infrastructure failure
  while the others are not, `restartPolicy: OnInfrastructureFailure` no longer restarts such an
  application. Use `OnFailure` to restart it ([SPARK-59729](https://issues.apache.org/jira/browse/SPARK-59729)).

### SparkCluster

The changes of the master and worker resources below apply when the operator creates them, i.e. to
a `SparkCluster` created after the upgrade. A running `SparkCluster` keeps its resources until it
is recreated, e.g. by suspending and resuming it with [`spec.suspend`](spark_custom_resources.md#suspend).

- Since 1.1.0, the workers start the external shuffle service by default, so that dynamic
  allocation works out of the box. To restore the behavior before 1.1.0, set
  `spark.shuffle.service.enabled` to `false` in `spec.sparkConf` ([SPARK-58835](https://issues.apache.org/jira/browse/SPARK-58835)).
- Since 1.1.0, the NetworkPolicy of the workers also admits ingress from every driver pod
  (`spark-role: driver`) in the namespace, so that the driver of an application running on the
  cluster can fetch task results larger than `spark.task.maxDirectResultSize` from the executors.
  1.0 admitted only the pods of the cluster ([SPARK-58649](https://issues.apache.org/jira/browse/SPARK-58649)).
- Since 1.1.0, the `terminationGracePeriodSeconds` of the master and worker pod templates is
  respected. 1.0 always overwrote it with `0`, which is still the default. To restore the behavior
  before 1.1.0, remove it from the pod templates ([SPARK-59795](https://issues.apache.org/jira/browse/SPARK-59795)).
- Since 1.1.0, the `dnsConfig` of the worker pod template is kept, and the domain of the worker
  service is appended to its `searches`. 1.0 replaced the whole `dnsConfig`. To restore the
  behavior before 1.1.0, remove `dnsConfig` from the worker pod template ([SPARK-59797](https://issues.apache.org/jira/browse/SPARK-59797)).
- Since 1.1.0, a cluster whose master and worker resources cannot be requested because of a
  failure which may clear on its own, e.g. a timeout, a `429`, a `500` or a request which the API
  server never answered, stays `Submitted` and is retried at the next reconciliation, instead of
  failing with `SchedulingFailure` and then `Failed`. A request which the API server rejected
  still fails the cluster ([SPARK-59750](https://issues.apache.org/jira/browse/SPARK-59750)).
- Since 1.1.0, the message of the `Failed` state after `SchedulingFailure` is
  `Cluster failed, since requesting its resources failed. See the preceding SchedulingFailure state for the cause.`
  instead of `Cannot process cluster status.` ([SPARK-59870](https://issues.apache.org/jira/browse/SPARK-59870)).

### Docker Image

- Since 1.1.0, the image is based on `alpine:3.24` instead of `alpine:3.23`, and it does not
  install `libstdc++`, which the operator does not use. An image built on top of it which needs
  the package has to install it ([SPARK-59834](https://issues.apache.org/jira/browse/SPARK-59834), [SPARK-59836](https://issues.apache.org/jira/browse/SPARK-59836)).

  ```dockerfile
  FROM apache/spark-kubernetes-operator:1.1.0
  USER root
  RUN apk add --no-cache libstdc++
  USER spark
  ```

### Java API

- Since 1.1.0, `org.apache.spark.k8s.operator.Constants` is `final` and has a private constructor,
  so it cannot be extended or instantiated ([SPARK-59679](https://issues.apache.org/jira/browse/SPARK-59679)).
- Since 1.1.0, `ApplicationStatus.terminateOrRestart(RestartConfig, ResourceRetainPolicy, String, boolean)`
  is deprecated for removal. It ignores `resourceRetainPolicy`, so it no longer returns
  `TerminatedWithoutReleaseResources`. Use `terminateOrRestart(RestartConfig, String, boolean)`
  instead ([SPARK-59734](https://issues.apache.org/jira/browse/SPARK-59734)).
- Since 1.1.0, `ClusterStateSummary` has the `Suspended` constant, so an exhaustive `switch` over
  it needs a case for it ([SPARK-59750](https://issues.apache.org/jira/browse/SPARK-59750)).
