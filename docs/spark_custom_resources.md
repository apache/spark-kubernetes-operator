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

# Spark Operator API

The core user facing API of the Spark Kubernetes Operator is the `SparkApplication` and
`SparkCluster` Custom Resources Definition (CRD). Spark custom resource extends
standard k8s API, defines Spark Application spec and tracks status.

Once the Spark Operator is installed and running in your Kubernetes environment, it will
continuously watch SparkApplication(s) and SparkCluster(s) submitted, via k8s API client or
kubectl by the user, orchestrate secondary resources (pods, configmaps .etc).

Please check out the [quickstart](../README.md) as well for installing operator.

The full CRD schemas (all `spec` and `status` fields with types and descriptions) can be
browsed on the web at
[doc.crds.dev](https://doc.crds.dev/github.com/apache/spark-kubernetes-operator).

## SparkApplication

SparkApplication can be defined in YAML format. User may configure the application entrypoint
and configurations. Let's start with the [Spark-Pi example](../examples/pi.yaml):

```yaml
apiVersion: spark.apache.org/v1
kind: SparkApplication
metadata:
  name: pi
spec:
  # Entry point for the app
  mainClass: "org.apache.spark.examples.SparkPi"
  jars: "local:///opt/spark/examples/jars/spark-examples.jar"
  sparkConf:
    spark.dynamicAllocation.enabled: "true"
    spark.dynamicAllocation.maxExecutors: "3"
    spark.kubernetes.authenticate.driver.serviceAccountName: "spark"
    spark.kubernetes.container.image: "apache/spark:4.2.0-scala"
  applicationTolerations:
    resourceRetainPolicy: OnFailure
    ttlAfterStopMillis: 10000
  runtimeVersions:
    sparkVersion: "4.2.0"
```

After application is submitted, Operator will add status information to your application based on
the observed state:

```bash
kubectl get sparkapp pi -o yaml
```

### Write and build your SparkApplication

It's straightforward to convert your spark-submit application to `SparkApplication` yaml.
Operators constructs driver spec in the similar approach. To submit Java / scala application,
use `.spec.jars` and `.spec.mainClass`. Similarly, set `pyFiles` for Python applications.

While building images to use by driver and executor, it's recommended to use official
[Spark Docker](https://github.com/apache/spark-docker) as base images. Check the pod template
support (`.spec.driverSpec.podTemplateSpec` and `.spec.executorSpec.podTemplateSpec`) as well for
setting custom Spark home and work dir.

### Pod Template Support

It is possible to configure pod template for driver & executor pods for configure spec that are
not configurable from SparkConf.

Spark Operator supports defining pod template for driver and executor pods in two ways:

1. Set `PodTemplateSpec` in `SparkApplication`
2. Config `spark.kubernetes.[driver/executor].podTemplateFile`

If pod template spec is set in application spec (option 1), it would take higher precedence
than option 2. Also `spark.kubernetes.[driver/executor].podTemplateFile` would be unset to
avoid multiple override.

When pod template is set as remote file in conf properties (option 2), please ensure Spark
Operator has necessary permission to access the remote file location, e.g. deploy operator
with proper workload identity with target S3 / Cloud Storage bucket access. Similar permission
requirements are also needed driver pod: operator needs template file access to create driver,
and driver needs the same for creating executors.

Please be advised that Spark still overrides necessary pod configuration in both options. For
more details,
refer [Spark doc](https://spark.apache.org/docs/latest/running-on-kubernetes.html#pod-template).

## Enable Additional Ingress for Driver

Operator may create [Ingress](https://kubernetes.io/docs/concepts/services-networking/ingress/) for
Spark driver of running applications on demand. For example, to expose Spark UI - which is by
default enabled on driver port 4040, you may configure

```yaml
spec:
  driverServiceIngressList:
    - serviceMetadata:
        name: "spark-ui-service"
      serviceSpec:
        ports:
          - protocol: TCP
            port: 80
            targetPort: 4040
      ingressMetadata:
        name: "spark-ui-ingress"
        annotations:
          nginx.ingress.kubernetes.io/rewrite-target: /
      ingressSpec:
        ingressClassName: nginx-example
        rules:
          - http:
              paths:
                - path: "/"
                  pathType: Prefix
                  backend:
                    service:
                      name: spark-ui-service
                      port:
                        number: 80
```

Spark Operator by default would populate the `.spec.selector` field of the created Service to match
the driver labels. If `.ingressSpec.rules` is not provided, Spark Operator would also populate one
default rule backed by the associated Service. It's recommended to always provide the ingress spec
to make sure it's compatible with your
[IngressController](https://kubernetes.io/docs/concepts/services-networking/ingress-controllers/).

## Enable Gateway API Route for Driver

As an alternative to `Ingress`, the operator can expose driver endpoints via the Kubernetes
[Gateway API](https://gateway-api.sigs.k8s.io/) by creating `HTTPRoute` or `GRPCRoute` resources
alongside a backing Service. Use `driverHttpRouteList` for HTTP endpoints (for example the Spark
UI) and `driverGrpcRouteList` for gRPC endpoints (for example Spark Connect).

```yaml
spec:
  driverHttpRouteList:
    - serviceMetadata:
        name: "spark-ui-service"
      serviceSpec:
        ports:
          - protocol: TCP
            port: 80
            targetPort: 4040
      httpRouteMetadata:
        name: "spark-ui-route"
      httpRouteSpec:
        parentRefs:
          - group: gateway.networking.k8s.io
            kind: Gateway
            name: my-gateway
        rules:
          - matches:
              - path:
                  type: PathPrefix
                  value: "/"
            backendRefs:
              - name: spark-ui-service
                port: 80
```

Prerequisites and behavior:

- The Gateway API v1 CRDs (`httproutes.gateway.networking.k8s.io`,
  `grpcroutes.gateway.networking.k8s.io`) must be installed on the cluster. If a CRD is missing,
  reconciliation of a SparkApplication that references the corresponding route list will fail
  with a `no matches for kind "HTTPRoute"` (or `"GRPCRoute"`) error from the Kubernetes API.
- `httpRouteSpec.parentRefs` (and `grpcRouteSpec.parentRefs`) is required — without at least one
  `parentRef` the route object would be orphaned from any `Gateway` and receive no traffic. The
  operator rejects SparkApplications where any route-list entry omits `parentRefs`.
- As with Ingress, the Service's `.spec.selector` defaults to the driver labels, and if
  `httpRouteSpec.rules` / `grpcRouteSpec.rules` is not provided the operator populates a single
  default rule that backends to the first port of the associated Service.

## Create and Mount ConfigMap

It is possible to ask operator to create configmap so they can be used by driver and/or executor
pods on the fly. `configMapSpecs` allows you to specify the desired metadata and data as string
literals for the configmap(s) to be created.

```yaml
spec:
  configMapSpecs:
    - name: "example-config-map"
      data:
        foo: "bar"
```

Like other app-specific resources, the created configmap has owner reference to Spark driver and
therefore shares the same lifecycle and garbage collection mechanism with the associated app.  

This feature can be used to create lightweight override config files for given Spark app. For
example, below snippet would create and mount a configmap with metrics property file, then use it
in SparkConf:

```yaml
spec:
  sparkConf:
    spark.metrics.conf: "/etc/metrics/metrics.properties"
  driverSpec:
    podTemplateSpec:
      spec:
        containers:
          - volumeMounts:
              - name: "config-override"
                mountPath: "/etc/metrics"
                readOnly: true
        volumes:
          - name: config-override
            configMap:
              name: metrics-configmap
  executorSpec:
    podTemplateSpec:
      spec:
        containers:
          - volumeMounts:
              - name: "config-override"
                mountPath: "/etc/metrics"
                readOnly: true
        volumes:
          - name: config-override
            configMap:
              name: metrics-configmap
  configMapSpecs:
    - name: "metrics-configmap"
      data:
        metrics.properties: "*.sink.jmx.class=org.apache.spark.metrics.sink.JmxSink\n"

```

## Understanding Failure Types

In addition to the general `Failed` state (that driver pod fails or driver container exits
with non-zero code), Spark Operator introduces a few different failure state for ease of
app status monitoring at high level, and for ease of setting up different handlers if users
are creating / managing SparkApplications with external microservices or workflow engines.

Spark Operator recognizes "infrastructure failure" in the best effort way. It is possible to
configure different restart policy on general failure(s) vs. on potential infrastructure
failure(s). For example, you may configure the app to restart only upon infrastructure
failures. If a Spark application fails with `DriverStartTimedOut`, `ExecutorsStartTimedOut`,
or `SchedulingFailure`, it is more likely that the app failed as a result of infrastructure
reason(s), including scenarios like driver or executors cannot be scheduled or cannot
initialize in configured time window for scheduler reasons, as a result of insufficient
capacity, cannot get IP allocated, cannot pull images, or k8s API server issues at
scheduling, etc.

Please be advised that this is a best-effort failure identification. You may still need to
debug actual failure from the driver pods. Spark Operator would stage the last observed
driver pod status with the stopping state for audit purposes.

## Configure the Tolerations for SparkApplication

### Restart

Spark Operator enables configure app restart behavior for different failure types. Here's a
sample restart config snippet:

``` yaml
restartConfig:
  # acceptable values are 'Never', 'Always', 'OnFailure' and 'OnInfrastructureFailure'
  restartPolicy: Never
  # operator would retry the application if configured. All resources from current attempt
  # would be deleted before starting next attempt
  maxRestartAttempts: 3
  # backoff time (in millis) that operator would wait before next attempt
  restartBackoffMillis: 30000
```

### Granular Restart Control

For more fine-grained control over restart behavior, you can configure different retry limits
and backoff times for specific failure types. This allows you to handle different failure
scenarios with appropriate strategies.

The operator maintains multiple counters to track different types of restarts:
- General restart counter: Tracks all restarts
- Consecutive failure counter: Tracks consecutive failures
- Consecutive scheduling failure counter: Tracks consecutive scheduling failures only

#### Restart Behavior Control

- Consecutive failure tracking: The failure-specific counters track consecutive failures
  of the app, distinguishing between persistent failures (requiring intervention) and
  transient issues (safe for retry).
  - Example: with `restartPolicy=Always`, `maxRestartAttempts=5`, and `maxRestartOnFailure=2`:
  - The app tolerates at most 2 consecutive failures; the 3rd consecutive failure stops it,
    within an overall cap of 5 total restarts.
  - In other words, the sequence F -> F -> F stops on the 3rd F.
  - The sequence F -> S -> F -> S -> F continues, because each successful attempt
    resets the consecutive-failure counter.
- Granular control over `SchedulingFailure`: similarly, it's possible to control the maximal
  restart and backoff interval for consecutive `SchedulingFailure` attempts, as it can be highly
  associated with API server rejections, quota exceeded, resource constraints.

#### Restart Limit Evaluation

When an attempt ends, limits are checked in order:
  1. General limit (`maxRestartAttempts`) is checked for every restart
  2. For failures, the most specific applicable limit is also checked:
     - Scheduling failures (SchedulingFailure) → `maxRestartOnSchedulingFailure` (if set)
     - Other failures → `maxRestartOnFailure` (if set)
  3. The application stops if any applicable limit is exceeded


#### Configuration Fields

```yaml
restartConfig:
  restartPolicy: Always
  # Default restart configuration (applies to all restarts)
  maxRestartAttempts: 5
  restartBackoffMillis: 30000  # 30 seconds

  # Override for consecutive general failures (application crashes, driver failures, etc.)
  # This counter resets to 0 on success
  maxRestartOnFailure: 3
  restartBackoffMillisForFailure: 60000  # 1 minute

  # Override for consecutive scheduling failures
  maxRestartOnSchedulingFailure: 1
  restartBackoffMillisForSchedulingFailure: 300000  # 5 minutes
```

#### Example Use Cases

Tolerate transient failures but stop on persistent issues:

```yaml
restartConfig:
  restartPolicy: Always
  maxRestartAttempts: 100  # Allow many total attempts
  restartBackoffMillis: 30000
  # But stop after 3 consecutive failures (indicates persistent problem)
  maxRestartOnFailure: 3
  restartBackoffMillisForFailure: 60000
```

Mitigate API server stress during scheduling failures:

```yaml
restartConfig:
  restartPolicy: Always
  maxRestartAttempts: 50
  restartBackoffMillis: 30000
  # Stop quickly on scheduling failures to avoid overwhelming API server
  maxRestartOnSchedulingFailure: 2
  restartBackoffMillisForSchedulingFailure: 600000  # 10 minutes
```


| Field                                                                                   | Type    | Default Value | Description                                                                                                                                                                                                                              |
|-----------------------------------------------------------------------------------------|---------|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| .spec.applicationTolerations.restartConfig.restartPolicy                                | string  | Never         | Restart policy: `Never`, `Always`, `OnFailure`, or `OnInfrastructureFailure`                                                                                                                                                             |
| .spec.applicationTolerations.restartConfig.maxRestartAttempts                           | integer | 3             | Maximum number of restart attempts for all scenarios (always checked)                                                                                                                                                                    |
| .spec.applicationTolerations.restartConfig.restartBackoffMillis                         | integer | 30000         | Default backoff time in milliseconds between restart attempts                                                                                                                                                                            |
| .spec.applicationTolerations.restartConfig.maxRestartOnFailure                          | integer | null          | Maximum consecutive failures before stopping. Resets to 0 on success. If null, uses maxRestartAttempts                                                                                                                                   |
| .spec.applicationTolerations.restartConfig.restartBackoffMillisForFailure               | integer | null          | Backoff time for application failures. If null, uses restartBackoffMillis                                                                                                                                                                |
| .spec.applicationTolerations.restartConfig.maxRestartOnSchedulingFailure                | integer | null          | Maximum consecutive scheduling failures before stopping. Scheduling failures occur when the API server rejects requests (e.g., quota exceeded, resource constraints). Resets to 0 on success. If null, falls back to maxRestartOnFailure |
| .spec.applicationTolerations.restartConfig.restartBackoffMillisForSchedulingFailure     | integer | null          | Backoff time for scheduling failures. If null, falls back to restartBackoffMillisForFailure                                                                                                                                              |

### Restart Counter reset

The `restartCounterResetMillis` field controls automatic restart counter resets for long-running
application attempts. When set to a non-negative value (in milliseconds), the operator will reset
all restart counters (including the general counter and both failure counters) if an application
attempt runs successfully for at least the specified duration before ending. The duration is
measured from the first state after `Submitted` / `ScheduledToRestart` (normally
`DriverRequested`), so time spent in restart backoff or suspended is not counted.

Time-based reset takes highest precedence over all limit checks. If an attempt runs longer than
`restartCounterResetMillis`, the operator will always restart with reset counters, regardless
of how many times the application has previously failed.

This feature enables applications to recover from early instability: you can limit fast-failing
restarts (which often indicate configuration or infrastructure issues) while allowing indefinite
restarts for applications that demonstrate stable operation for extended periods.

For example, setting

```yaml

restartConfig:
  ## 1hr
  restartCounterResetMillis: 3600000
  maxRestartAttempts: 3

```

means the application can fail and restart up to 3 times, but if any attempt runs for more than
1 hour, the counter resets to zero, allowing another 3 restart attempts.

The default value is -1, which disables automatic counter resets.

### Timeouts

It's possible to configure applications to be proactively terminated and resubmitted in particular
cases to avoid resource deadlock.

| Field                                                                                   | Type    | Default Value | Description                                                                                                        |
|-----------------------------------------------------------------------------------------|---------|---------------|--------------------------------------------------------------------------------------------------------------------|
| .spec.applicationTolerations.applicationTimeoutConfig.driverStartTimeoutMillis          | integer | 300000        | Time to wait for driver reaches running state after requested driver.                                              |
| .spec.applicationTolerations.applicationTimeoutConfig.executorStartTimeoutMillis        | integer | 300000        | Time to wait for driver to acquire minimal number of running executors.                                            |
| .spec.applicationTolerations.applicationTimeoutConfig.forceTerminationGracePeriodMillis | integer | 300000        | Time to wait for force delete resources at the end of attempt.                                                     |
| .spec.applicationTolerations.applicationTimeoutConfig.driverReadyTimeoutMillis          | integer | 300000        | Time to wait for driver reaches ready state.                                                                       |
| .spec.applicationTolerations.applicationTimeoutConfig.terminationRequeuePeriodMillis    | integer | 2000          | Back-off time when releasing resource need to be re-attempted for application.                                     |

### Instance Config

Instance Config helps operator to decide whether an application is running healthy. When
the underlying cluster has batch scheduler enabled, you may configure the apps to be
started if and only if there are sufficient resources. If, however, the cluster does not
have a batch scheduler, operator may help avoid app hanging with `InstanceConfig` that
describes the bare minimal tolerable scenario.

For example, with below spec:

```yaml
applicationTolerations:
  instanceConfig:
    minExecutors: 3
    initExecutors: 5
    maxExecutors: 10
sparkConf:
  spark.executor.instances: "10"
```

Spark would try to bring up 10 executors as defined in SparkConf. In addition, from
operator perspective,

* If Spark app acquires less than 5 executors in given time window (.spec.
  applicationTolerations.applicationTimeoutConfig.executorStartTimeoutMillis) after
  submitted, it would be shut down proactively in order to avoid resource deadlock.
* Spark app would be marked as 'RunningWithBelowThresholdExecutors' if it loses executors after
  successfully start up.
* Spark app would be marked as 'RunningHealthy' if it has at least min executors after
  successfully started up.

### Delete Resources On Termination

Operator by default would delete all created resources at the end of an attempt. It would
try to record the last observed driver status in `status` field of the application for
troubleshooting purpose.

On the other hand, when developing an application, it's possible to configure

```yaml
applicationTolerations:
  # Acceptable values are 'Always', 'OnFailure', 'Never'
  # Setting this to 'OnFailure' would retain secondary resources if and only if the app fails
  resourceRetainPolicy: OnFailure
  # Secondary resources would be garbage collected 10 minutes after app termination 
  resourceRetainDurationMillis: 600000
  # Garbage collect the SparkApplication custom resource itself 30 minutes after termination
  ttlAfterStopMillis: 1800000
```

to avoid operator attempt to delete driver pod and driver resources if app fails. Similarly,
if resourceRetainPolicy is set to `Always`, operator would not delete driver resources
when app ends. They would be by default kept with the same lifecycle as the App. It's also
possible to configure `resourceRetainDurationMillis` to define the maximal retain duration for
these resources. Note that this applies only to operator-created resources (driver pod, SparkConf
configmap .etc). You may also want to tune `spark.kubernetes.driver.service.deleteOnTermination`
and `spark.kubernetes.executor.deleteOnTermination` to control the behavior of driver-created
resources. `ttlAfterStopMillis` controls the garbage collection behavior at the SparkApplication
level after it stops. When set to a non-negative value, Spark operator would garbage collect the
application (and therefore all its associated resources) after given timeout. If the application
is configured to restart, `resourceRetainPolicy` and `resourceRetainDurationMillis` would not be
applied, and secondary resources would be released at the end of each attempt including the last
one. `ttlAfterStopMillis` would be applied after the last attempt.

For example, if an app with below configuration:

```yaml
applicationTolerations:
  restartConfig:
    restartPolicy: OnFailure
    maxRestartAttempts: 1
  resourceRetainPolicy: Always
  resourceRetainDurationMillis: 30000
  ttlAfterStopMillis: 60000
```

ends up with status like:

```yaml
status:
#... the 1st attempt
      "5":
        currentStateSummary: Failed
      "6":
        currentStateSummary: ScheduledToRestart
# ...the 2nd attempt
      "11":
        currentStateSummary: Succeeded
      "12":
        currentStateSummary: ResourceReleased
```

The retain policy does not take effect because the app is configured to restart. Secondary
resources are released between attempts between `5` and `6`, and after the last attempt at `12`.
TTL would be calculated based on the last state.

| Field                                                     | Type                              | Default Value | Description                                                                                                                                                                                               |
|-----------------------------------------------------------|-----------------------------------|---------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| .spec.applicationTolerations.resourceRetainPolicy         | `Always` / `OnFailure` / `Never`  | Never         | Configure operator to delete / retain secondary resources for an app after it terminates.                                                                                                                 |
| .spec.applicationTolerations.resourceRetainDurationMillis | integer                           | -1            | Time to wait in milliseconds for releasing **secondary resources** after termination. Setting to negative value would disable the retention duration check for secondary resources after termination.     |
| .spec.applicationTolerations.ttlAfterStopMillis           | integer                           | -1            | Time-to-live in milliseconds for SparkApplication and **all its associated secondary resources**. If set to a negative value, the application would be retained and not be garbage collected by operator. |

Note that `ttlAfterStopMillis` applies to the app as well as its secondary resources. If both
`resourceRetainDurationMillis` and `ttlAfterStopMillis` are set to non-negative value and the
latter is smaller, then it takes higher precedence: operator would remove all resources related
to this app after `ttlAfterStopMillis`.

## Suspend

Both `SparkApplication` and `SparkCluster` support `.spec.suspend`. When it is set to `true` before
the driver pod or the master / worker StatefulSets are requested, the operator keeps the resource in
its initializing state (`Submitted`, or `ScheduledToRestart` for an application that is scheduled to
restart) and does not request them. Setting it back to `false` resumes the regular lifecycle. A
running `SparkApplication` or `SparkCluster` is stopped instead, as described below.

`Submitted` here is the operator's in-memory view. For a valid resource created with
`suspend: true`, the initial `Submitted` status is not persisted to the API server, so
`kubectl get` shows an empty `Current State` and no state transition events are published until
initialization resumes. Instead, the `SuspendHeld` [event](configuration.md#kubernetes-events) is
published by default. Since the status is not there to fall back on, it is republished every 30
minutes by default while the hold lasts, so that it outlives the event retention of the API
server. An application held later, in `ScheduledToRestart`, or in `Submitted` after it was resumed
as described below, keeps the status which was already written and gets the same event, since that
status says that a restart is due or that the application is resumed, not that the next attempt is
withheld. If the driver, or the master of a `SparkCluster` which has not started yet, cannot be
read to check whether it was requested already, e.g. since the API server rejects the read, the
resource is neither held nor started, and the `SuspendCheckFailed` event is published instead by
default, until the check succeeds. A failure at the transport level, such as a timeout, or a
throttled read is retried without the event. Once the check succeeds again, the next `SuspendHeld`
event is published right away and supersedes it.

``` yaml
apiVersion: spark.apache.org/v1
kind: SparkApplication
metadata:
  name: suspended-pi
spec:
  suspend: true
  mainClass: "org.apache.spark.examples.SparkPi"
  jars: "local:///opt/spark/examples/jars/spark-examples.jar"
  runtimeVersions:
    sparkVersion: "4.2.0"
```

* Setting it to `true` on a `SparkApplication` whose driver is requested, i.e. from
  `DriverRequested` to `RunningWithBelowThresholdExecutors`, stops its current attempt. The
  application enters `Suspended` first, and then the operator deletes its driver pod, which deletes
  its executor pods and the other resources which the driver owns, e.g. its ConfigMaps and
  Services, like at the end of an attempt, whatever its `resourceRetainPolicy` is. The progress of
  the attempt is lost. An attempt whose driver has completed or failed by then ends as usual
  instead, and if the application is configured to restart, its next attempt is held as above. A
  failure to delete the driver pod or the Kueue `Workload`, other than one which may clear on its
  own, publishes the `SuspendReleaseFailed` [event](configuration.md#kubernetes-events) by default.
* Setting it back to `false` starts a new attempt of the application from `Submitted` once its
  driver and executor pods are gone and its Kueue `Workload`, if any, is released. A pod which is
  still terminating `forceTerminationGracePeriodMillis` after its grace period ended, e.g. on a lost
  node, no longer holds the application, and a driver pod is force deleted then. This is measured
  per pod, so that the pods are waited for even if deleting them failed or started late. The new
  attempt runs the application again from scratch with its current spec. Like a restart, it gets
  the next attempt ID, and with
  `spark.kubernetes.operator.reconciler.trimStateTransitionHistoryEnabled`, the state transition
  history of the suspended attempt moves to `previousAttemptSummary`. The attempt ID is part of the
  names of its driver resources unless `spark.app.id` is set, while the resources which the spec
  names, e.g. through `configMapSpecs`, keep their names. Unlike a restart, it starts without the
  restart backoff and does not count against the restart limits, so the restart counters stay as
  they are, and it starts even with `restartPolicy: Never`. Only the counter of consecutive
  scheduling failures starts over, since the driver of the suspended attempt was requested.
  `restartCounterResetMillis` and the timeouts of `applicationTimeoutConfig` apply to the new
  attempt on its own, so neither the run before the suspension nor the time suspended counts.
* Setting it to `true` on a running `SparkCluster` (`RunningHealthy`) stops it, since a cluster
  runs until it is deleted and has no other way to give its resources back. The cluster enters
  `Suspended` first, and then the operator deletes its master and worker StatefulSets, and the
  HorizontalPodAutoscaler and PodDisruptionBudget of its workers if any. Its Services and
  NetworkPolicy are kept. Any application running on the cluster is terminated with it, since there
  is no graceful decommission. A failure to delete them, other than one which may clear on its own,
  publishes the `SuspendReleaseFailed` [event](configuration.md#kubernetes-events) by default.
* Setting it back to `false` moves the cluster to `Submitted` once its master and worker pods are
  gone and its Kueue `Workload`, if any, is released, and the master and workers are requested
  again from scratch, like a newly created cluster. Only pods labeled with the `master` or `worker`
  `spark-role` count, so other pods which carry the `spark.operator/spark-cluster-name` label do
  not hold the cluster back. A request which may yet succeed, such as a timeout or an unavailable
  API server or admission webhook, is retried, while a rejected one fails the cluster with
  `SchedulingFailure`, as for a newly created cluster. With
  `spark.kubernetes.operator.reconciler.trimStateTransitionHistoryEnabled`, the resumed cluster drops
  the state transition history of its previous run, so that suspending it again and again keeps the
  status bounded. Setting it to `true` again moves the cluster back to `Suspended`. A pod which is
  still terminating 5 minutes after its grace period ended, e.g. on a lost node, no longer holds the
  Kueue `Workload`, so that it does not hold the quota forever. The cluster still stays `Suspended`
  until such a pod is gone, since it keeps the name of the master or worker to create, and the
  message of its `Suspended` state names such pods.
* The `Suspended` state says why the resource is suspended in `status.currentState.suspendReason`:
  `SpecSuspend` if it is held by `spec.suspend`, or `KueueEviction` if the eviction of its Kueue
  `Workload` released it, see [Kueue](#kueue). The operator relies on this field, not on the
  message, which is for users to read and may be reworded. `spec.suspend` takes precedence, so
  setting it on a resource suspended by an eviction records another `Suspended` state with
  `SpecSuspend`. A `Suspended` state without it is treated as `SpecSuspend`.
* An operator version without the `Suspended` state cannot read a `SparkApplication` or a
  `SparkCluster` whose status has it, even after the resource is resumed. A resumed
  `SparkApplication` keeps it in its state transition history, or with
  `spark.kubernetes.operator.reconciler.trimStateTransitionHistoryEnabled` in that of
  `previousAttemptSummary`, and a resumed `SparkCluster` keeps it unless that setting drops the
  history of its previous run. So delete such applications, and delete such clusters or resume the
  suspended ones with that setting enabled, before downgrading the operator.
* Deleting a suspended resource works as usual.
* This is the building block for external job queueing systems. See [Kueue](#kueue) for the
  built-in integration.

## Kueue

When `spark.kubernetes.operator.kueue.enabled` is set, a `SparkApplication` or a `SparkCluster`
labeled with `kueue.x-k8s.io/queue-name` is queued by [Kueue](https://kueue.sigs.k8s.io/). The
operator creates a Kueue `Workload` that describes the driver and executor (or master and worker)
pod sets, and holds the creation of those resources until Kueue admits the `Workload`.

See [kueue-single-clusterqueue-setup.yaml](../examples/kueue-single-clusterqueue-setup.yaml) for the
queues the administrator sets up, and [pi-on-kueue.yaml](../examples/pi-on-kueue.yaml) and
[cluster-on-kueue.yaml](../examples/cluster-on-kueue.yaml) for a `SparkApplication` and a
`SparkCluster` labeled with the `LocalQueue` of the setup.

```yaml
apiVersion: spark.apache.org/v1
kind: SparkApplication
metadata:
  name: pi-on-kueue
  labels:
    kueue.x-k8s.io/queue-name: spark-queue
spec:
  mainClass: "org.apache.spark.examples.SparkPi"
  jars: "local:///opt/spark/examples/jars/spark-examples.jar"
  sparkConf:
    spark.executor.instances: "1"
  runtimeVersions:
    sparkVersion: "4.2.0"
```

The following table summarizes which Kueue features are supported. The items after it describe the
behavior in detail, and
[Kueue Admission and Workload Lifecycle](architecture.md#kueue-admission-and-workload-lifecycle)
relates it to the reconciliation.

| Feature                                                                   | Support             | Notes                                                                                                                                                                                                                                        |
|---------------------------------------------------------------------------|---------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Kueue `v1beta2` API                                                       | Supported           | Kueue v0.20.0 or later is recommended, see [Compatibility](operations.md#compatibility).                                                                                                                                                     |
| Queueing by the `kueue.x-k8s.io/queue-name` label                         | Supported           | Both `SparkApplication` and `SparkCluster` are queued if `spark.kubernetes.operator.kueue.enabled` is set, e.g. by `operatorRbac.kueue.enabled` of the Helm chart, see [Optional Prerequisites](operations.md#optional-prerequisites).       |
| Waiting for the admission                                                 | Supported           | The resource stays in its initializing state without its driver (or master and workers), and the `KueueAdmissionPending` and `KueueAdmitted` [events](configuration.md#kubernetes-events) report the progress.                               |
| Changing the queue label                                                  | Supported           | A change is followed until the `Workload` reserves quota, while the `ValidatingAdmissionPolicy` of the Helm chart rejects one once the resource started, unless it is `Suspended` or `ScheduledToRestart`.                                   |
| `Workload` priority                                                       | Supported           | The `WorkloadPriorityClass` of the `kueue.x-k8s.io/priority-class` label takes precedence over the `priorityClassName` of the pod templates and then the `globalDefault` `PriorityClass`, like Kueue built-in integrations.                  |
| `ResourceFlavor` node labels and tolerations                              | Supported           | The `nodeLabels` and `tolerations` of the flavors which Kueue assigns to a pod set are added to its pods, like Kueue built-in integrations.                                                                                                  |
| Admission checks, e.g. `ProvisioningRequest`                              | Supported           | The labels, annotations, node selector and tolerations in the `podSetUpdates` of the checks are added to the pods once Kueue admits the `Workload`.                                                                                          |
| Preemption and other evictions                                            | Supported           | An evicted resource, e.g. a preempted one or one whose `ClusterQueue` is stopped with `HoldAndDrain`, is queued again with a new `Workload`, and a running one is released through `Suspended` first.                                        |
| `Workload` deactivation, e.g. `kueuectl stop workload`                    | Supported           | A resource waits while its `Workload` is deactivated, and a running one is released through `Suspended` and queued again once the `Workload` is reactivated.                                                                                 |
| `waitForPodsReady`                                                        | Supported           | The operator records the `PodsReady` condition once the driver and `spark.executor.instances` executors, or the master and workers, are ready, and handles a `PodsReadyTimeout` like any other eviction.                                     |
| `spec.suspend`                                                            | Supported           | It takes precedence over the queueing, so that a suspended resource releases its `Workload`, see [Suspend](#suspend).                                                                                                                        |
| Releasing the quota of an application attempt                             | Supported           | The `Workload` is deleted once the driver and executor pods of the attempt are gone, or gets the `Finished` condition when a `Succeeded`, `Failed` or `DriverEvicted` application [retains its resources](#delete-resources-on-termination). |
| Requeuing backoff after an eviction                                       | Partially supported | The resource waits for the backoff which Kueue records, but its new `Workload` starts the requeue count over, so the backoff does not grow and a `backoffLimitCount` above 0 never deactivates it.                                           |
| `recoveryTimeout` of `waitForPodsReady`                                   | Not supported       | The `PodsReady` condition stays `True` when a pod is lost later, e.g. an executor which Spark replaces.                                                                                                                                      |
| Maximum execution time (`kueue.x-k8s.io/max-exec-time-seconds`)           | Not supported       | Use the Spark native `spark.driver.timeout` instead, as in [pi-with-driver-timeout.yaml](../examples/pi-with-driver-timeout.yaml).                                                                                                           |
| Partial admission                                                         | Not supported       | The pod sets of a `Workload` have no `minCount`, so Kueue admits all the requested pods or none.                                                                                                                                             |
| Topology Aware Scheduling                                                 | Not supported       | The operator applies neither the topology assignment nor the scheduling gate of Kueue to the pods.                                                                                                                                           |
| MultiKueue                                                                | Not supported       | `SparkApplication` and `SparkCluster` have no `spec.managedBy` field, which MultiKueue needs to run them on a worker cluster.                                                                                                                |
| Elastic workloads (`Workload` slices)                                     | Not supported       | A resource has one `Workload` whose pod set counts are fixed.                                                                                                                                                                                |
| `manageJobsWithoutQueueName` and `managedJobsNamespaceSelector`           | Not supported       | Kueue does not manage the Spark custom resources itself, so a resource without the `kueue.x-k8s.io/queue-name` label is not queued and starts right away.                                                                                    |
| Dynamic allocation (`spark.dynamicAllocation.enabled`)                    | Not supported       | Such a `SparkApplication` fails with `SchedulingFailure` instead of being queued.                                                                                                                                                            |
| `SparkCluster` with `minWorkers < maxWorkers`                             | Not supported       | Such a `SparkCluster` fails with `SchedulingFailure` instead of being queued.                                                                                                                                                                |
| Pod template files (`spark.kubernetes.{driver,executor}.podTemplateFile`) | Not supported       | A `SparkApplication` whose pod template is set only in such a file fails with `SchedulingFailure`, so set it in the [spec](#pod-template-support) instead.                                                                                   |

* The Helm chart sets `spark.kubernetes.operator.kueue.enabled` together with the Kueue RBAC rules
  when `operatorRbac.kueue.enabled` is set. Kueue and its `LocalQueue` must exist as well. See
  [Optional Prerequisites](operations.md#optional-prerequisites). Without the integration, the
  label is ignored, so the resource is not queued and starts right away, and the operator does
  not access any Kueue resource, like Kueue ignores a job whose integration is not enabled. Since
  the author of the resource may not see the operator configuration, the `KueueDisabled` warning
  [event](configuration.md#kubernetes-events) is published by default.
* The `Workload` is named `<lower-cased kind>-<resource name>` and is owned by the Spark resource,
  so it is garbage collected along with it.
* Like Kueue built-in integrations, the `Workload` gets the priority of the `WorkloadPriorityClass`
  named by the `kueue.x-k8s.io/priority-class` label. Without the label, the `priorityClassName` of
  the driver (or master) pod template is used, then the one of the executor (or worker) pod
  template, and then the `globalDefault` `PriorityClass`. The resource keeps waiting without a
  `Workload` while the named priority class does not exist. Changing the label before the
  `Workload` reserves quota updates its priority in place. After that Kueue freezes the presence,
  the group and the kind of the priority class, so only the name of a `WorkloadPriorityClass` still
  changes. Like Kueue, a `Workload` which took its priority from a `PriorityClass` never follows
  the label, and a changed value of the same class does not affect the existing `Workload`.
* While the `Workload` waits for quota, the resource stays in its initializing state (`Submitted`,
  or `ScheduledToRestart` for a restarted attempt) and no driver (or master / worker) is created.
  Like `spec.suspend`, the initial `Submitted` status of the first attempt is not persisted to the
  API server, so `kubectl get` shows an empty `Current State` and no state transition events are
  published until the `Workload` is admitted. Instead, the `KueueAdmissionPending` and
  `KueueAdmitted` [events](configuration.md#kubernetes-events) are published by default. Since
  the status is not there to fall back on, the pending event is republished while the `Workload`
  waits, so that it outlives the event retention of the API server. Use `kubectl get workload` to
  see the admission status. If the spec changes while waiting, the
  `Workload` is recreated with the new resource requests. Like Kueue, if the
  `kueue.x-k8s.io/queue-name` label changes before the `Workload` reserves quota, the `Workload` is
  moved to the new queue in place. After that Kueue freezes the queue name, so the change is
  ignored and the resource stays in the old queue. If the label is removed while waiting, the
  `Workload` is deleted, so that Kueue does not admit it later into quota which nothing uses, and
  the resource starts without Kueue.
* A queued resource spends one more reconciliation on the admission itself. With the default rate
  limiter (5 reconciliations per 15 seconds), an application that finishes within the first 15
  seconds may be observed only after its driver completed. It then reports its terminal state up
  to one refresh period late, without the driver states in between such as `DriverStarted`.
* When a `SparkApplication` attempt stops and its resources are released, the operator deletes the
  `Workload` so that Kueue releases the quota, even if the `kueue.x-k8s.io/queue-name` label was
  removed after the admission. The `Workload` is deleted only after the driver and executor pods
  are gone, since terminating pods still occupy the quota, and the application keeps its state
  until then. The operator stops waiting `forceTerminationGracePeriodMillis` after the attempt
  stopped, the `SparkApplication` was deleted, or its retention expired, so that a pod stuck in
  terminating does not hold the application. A restarted attempt is queued again with the label. When the resources are
  retained by `resourceRetainPolicy`, the `Workload` of a `Succeeded`, `Failed`, or
  `DriverEvicted` application gets the Kueue `Finished` condition instead, which releases the
  quota while keeping the `Workload`, or is deleted if the label was removed. Other retained
  resources, e.g. a driver still running after a start timeout, keep the quota until they are
  released.
* A `SparkCluster` requests the resources set on the `master` and `worker` containers of its pod
  templates. A missing request defaults to the limit, or else to 1 CPU and `SPARK_DAEMON_MEMORY`
  plus overhead. A worker uses `SPARK_WORKER_CORES` for the CPU and adds `SPARK_WORKER_MEMORY` to
  the memory when they are set. A `SparkCluster` keeps the quota until it is deleted, suspended or
  evicted.
  Set a cpu and memory request or limit on the `worker` container, or `SPARK_WORKER_CORES` and
  `SPARK_WORKER_MEMORY`: a worker with none of them advertises the whole node to its executors,
  well above the default the `Workload` requests.
* Like Kueue built-in integrations, the `nodeLabels` and `tolerations` of the `ResourceFlavor`s
  assigned to a pod set are added to the node selector and tolerations of its pods. The executor
  pods get them through the executor pod template, which the operator creates if not set. They
  stay on the pods even if the `kueue.x-k8s.io/queue-name` label is removed after the driver (or
  master) was created, since the `Workload` keeps the quota until it is released with the
  resources. A node label that conflicts with the node selector of the pods fails the resource
  with `SchedulingFailure`, and deletes the `Workload` unless its driver or master is running
  already, whose pods still hold the quota it reserved. Only fixing the node selector or the flavor
  resolves it: a restarted attempt requests the quota again and hits the same conflict, so an
  application which restarts on `SchedulingFailure` should bound the attempts with
  [`maxRestartOnSchedulingFailure`](#granular-restart-control). `ResourceFlavor`s are
  cluster-scoped, so reading them needs the rules which `operatorRbac.kueue.enabled` grants
  through the ClusterRole, hence `operatorRbac.clusterRole.create` as well. Until they can be
  read, the resource is held and the read is retried.
* Like Kueue built-in integrations, the `labels`, `annotations`, `nodeSelector` and `tolerations`
  which the [admission checks](https://kueue.sigs.k8s.io/docs/concepts/admission_check/) of the
  `Workload` report for a pod set in `status.admissionChecks[].podSetUpdates` are added to its pods
  as well, once all the checks are `Ready` and Kueue admits the `Workload`. The executor pods get
  them through the executor pod template, like the flavors. So a resource can use a
  [ProvisioningRequest](https://kueue.sigs.k8s.io/docs/concepts/admission_check/provisioning_request/)
  admission check, whose `autoscaling.x-k8s.io/consume-provisioning-request` and
  `autoscaling.x-k8s.io/provisioning-class-name` annotations, and the node selector which its
  `ProvisioningRequestConfig` sets, bind the pods to the capacity provisioned for them. The
  operator reads them from the `Workload`, so they need no further RBAC rules. Like Kueue, an
  update must not change a value which the flavors or an earlier admission check set, nor a value
  of the node selector, labels or annotations of the pods, and such a conflict fails the resource
  like a node label conflict above. The labels and annotations which Spark sets on the driver and
  executor pods itself, e.g. `spark-role` or the ones of
  `spark.kubernetes.{driver,executor}.{label,annotation}.*`, are not checked, and Spark's value
  wins over an update of the same key.
* `spec.suspend` takes precedence. A suspended resource does not get a `Workload`, and suspending a
  queued resource deletes its `Workload` to release the quota. While its driver, or the master of a
  `SparkCluster` which has not started yet, cannot be read to check whether it was requested, as
  described in [Suspend](#suspend), the `Workload` is kept instead, and a pending one may still be
  admitted. Suspending a running `SparkApplication` deletes its `Workload` only after its driver and
  executor pods are gone, or are still terminating `forceTerminationGracePeriodMillis` after their
  grace period ended, and suspending a running `SparkCluster` only after its master and worker pods
  are gone, or are still terminating 5 minutes after their grace period ended, since terminating
  pods occupy the quota. The `Workload` is deleted even if the `kueue.x-k8s.io/queue-name` label was
  removed after the admission. The resource is queued again when it is resumed with the
  `kueue.x-k8s.io/queue-name` label.
* Like the webhooks of Kueue built-in integrations, which let only a suspended job change its queue,
  the Helm chart installs a `ValidatingAdmissionPolicy` with `operatorRbac.kueue.enabled`. It
  rejects an update that adds, changes or removes the `kueue.x-k8s.io/queue-name` label of a
  `SparkApplication` or a `SparkCluster` once its status says that it started, since the `Workload`
  admitted for it keeps its quota in that queue. The label may change again when a
  `SparkApplication` is `ScheduledToRestart`, whose next attempt is queued with a new `Workload`, or
  when a `SparkApplication` or a `SparkCluster` is `Suspended`. So the label of a terminated
  resource, e.g. `Failed`, stays as it was. To move a running resource to another queue, set
  `spec.suspend` to `true`, change the label once the resource is `Suspended`, and then resume it,
  while a running `SparkApplication` also moves with its next attempt after a restart. Before a
  resource starts, the operator follows the label as described
  above. The policy reads only the status, so a resource whose first status is not persisted yet,
  e.g. after the update following the request of its driver or master failed, looks like it has not
  started. Its label may still change then, while the operator keeps the admitted `Workload` of such
  a resource. The policy is cluster-scoped, so it is named `<release name>-spark-kueue-queue-name`
  and matches only the namespaces which the chart sets as the watched namespaces, or all namespaces
  if `workloadResources.namespaces.overrideWatchedNamespaces` is disabled. Whoever installs the
  chart needs access to the `validatingadmissionpolicies` and `validatingadmissionpolicybindings` of
  `admissionregistration.k8s.io`, while the operator does not.
* Once the integration is disabled or the access of the operator to `Workload`s is revoked, e.g.
  by disabling `operatorRbac.kueue.enabled`, the operator no longer deletes them, and a `Workload`
  left behind keeps its quota until its owner is deleted. While the integration stays enabled
  without the access, a suspended `SparkApplication` or `SparkCluster` does not resume, and a
  stopping `SparkApplication` neither terminates nor restarts, until the access is restored or the
  integration is disabled, since the operator keeps deleting their `Workload`s by name, which is
  denied even once a `Workload` is gone. Before disabling either, let the queued
  `SparkApplication`s finish or suspend them, and suspend the queued
  `SparkCluster`s, so that the operator releases their `Workload`s itself. A suspended resource
  whose driver, or whose master which has not started yet, cannot be read keeps its `Workload`
  until it can, see [Suspend](#suspend). Then list the remaining ones with
  `kubectl get workloads -A -l spark.operator/spark-app-name` and
  `kubectl get workloads -A -l spark.operator/spark-cluster-name`, and delete only those whose
  owner has no running pods, e.g. of a `Failed` `SparkCluster`.
* Dynamic allocation, a `SparkCluster` with `minWorkers < maxWorkers`, and pod template files set
  through `spark.kubernetes.{driver,executor}.podTemplateFile` are not supported yet. Such a
  resource fails with `SchedulingFailure` instead of being queued.
* A resource whose `Workload` is deactivated (`spec.active` set to `false`), e.g. by
  `kueuectl stop workload`, before its driver (or master) is requested waits until the `Workload`
  is reactivated, since Kueue neither counts nor admits a deactivated `Workload`, and the
  `KueueAdmissionPending` event says so. The operator keeps the `Workload` as it is meanwhile, even
  if the spec of the resource changes, unless `spec.suspend` deletes it as described above, after
  which the resumed resource is queued with an active `Workload`. Once it is
  reactivated, a pending `Workload` keeps its place in the queue, while one which Kueue admitted
  before is deleted, like an evicted one below, since Kueue counts its admission again. A spec
  change made while it is deactivated is applied only if the operator sees the reactivation
  before Kueue admits the pending `Workload` again. Otherwise, the resources are requested under
  the admission of the old spec, like after a spec change right after an admission.
* A `Workload` which Kueue evicts before the driver (or master) is requested, e.g. to preempt it
  for a workload of a higher priority, is kept until the requeue backoff which Kueue records on it
  (`status.requeueState.requeueAt`) elapses, e.g. after an admission check asked for a retry, and
  the `KueueAdmissionPending` event names the eviction meanwhile. Kueue keeps the quota of an
  evicted `Workload` until then, so the operator then deletes it, and the resource is queued again
  with a new `Workload`. Unlike Kueue built-in integrations, which keep the evicted
  `Workload`, the new `Workload` is ordered in its queue like a newly created one, and it starts
  over the retry counts of its admission checks and the requeue count of its `PodsReadyTimeout`s.
  So their backoff does not grow with each eviction, e.g. it stays at
  `requeuingStrategy.backoffBaseSeconds` (60 seconds by default) for a `PodsReadyTimeout`, and a
  `requeuingStrategy.backoffLimitCount` above 0 never deactivates the `Workload`.
* A running `SparkCluster` whose `Workload` Kueue evicts, e.g. to preempt it for a workload of a
  higher priority or since its `ClusterQueue` is stopped, is released like on `spec.suspend`
  without setting it. The cluster enters `Suspended` with a message giving the reason of the
  eviction, and the operator deletes its master and worker StatefulSets. Kueue keeps the quota of
  an evicted `Workload` until then, so the operator deletes the `Workload` only after the master
  and worker pods are gone, like on `spec.suspend`, and after its requeue backoff elapsed, like
  above. The cluster then moves to `Submitted` and is queued again with a new `Workload`. The
  Workload informer reconciles the cluster as soon as its `Workload` is evicted.
* A running `SparkCluster` whose `Workload` is deactivated is released as well, since Kueue stops
  counting its quota right away. Unlike an evicted one, the operator keeps the `Workload`, so the
  cluster stays `Suspended` until the `Workload` is reactivated, and then it is queued again.
* Like Kueue built-in integrations, the operator records the `PodsReady` condition on the
  `Workload` once as many pods of each of its pod sets as requested are ready: the driver and
  `spark.executor.instances` executors of a `SparkApplication`, regardless of the executor
  thresholds of its `applicationTolerations`, or the master and workers of a `SparkCluster`. The
  `Workload` of an application whose executors are not pods has the driver pod set only, and so
  requests no quota for them, e.g. with the `spark://` master URL of a `SparkCluster` as
  `spark.master`, or with `spark.kubernetes.driver.master` set to `local[*]`. Kueue
  enables its `waitForPodsReady` configuration by default since v0.19, which evicts a `Workload`
  whose pods are not ready within the `timeout` (30 minutes by default) after its admission, so a
  resource which starts in time is not evicted, and `blockAdmission` admits the next workload once
  the pods are ready. Unlike Kueue built-in integrations, the condition stays `True` when a pod is
  lost later, e.g. an executor which Spark replaces, so the `recoveryTimeout` does not apply.
* A `PodsReadyTimeout` of a running `SparkCluster`, whose master or workers are not ready in time,
  releases the cluster like any other eviction above, even if they are ready by now, as Kueue
  built-in integrations do. So does a failure to record the `PodsReady` condition, e.g. without the
  permission for the `workloads/status` subresource, which the `KueuePodsReadyUpdateFailed`
  [event](configuration.md#kubernetes-events) reports. As the backoff does not grow, see above, a
  cluster whose master or workers never get ready, e.g. since they cannot be scheduled, or whose
  condition cannot be recorded, is admitted and evicted again and again until it is fixed,
  suspended or deleted. With `blockAdmission`, it holds back every other workload from each
  admission until its `Workload` is deleted after the backoff.
* A `SparkApplication` whose driver is requested, i.e. from `DriverRequested` to
  `RunningWithBelowThresholdExecutors`, is released like a running `SparkCluster` above when Kueue
  evicts its `Workload`, e.g. to preempt it or by a `PodsReadyTimeout`, or deactivates it. The
  application enters `Suspended` with a message giving the reason of the eviction, and the
  operator deletes its driver pod, which deletes its executor pods. Like for a cluster, the
  `Workload` is deleted once the pods are gone and its requeue backoff elapsed, or kept until it is
  reactivated if it was deactivated, and then the application starts a new attempt from
  `Submitted`, which is queued again with a new `Workload`. Like a resumed one, the new attempt
  runs the application again from scratch and does not count against the restart limits, so it
  starts even with `restartPolicy: Never`. An attempt whose driver has completed or failed by then
  ends as usual instead, and `spec.suspend` takes precedence, so an application which is suspended
  by it as well stays `Suspended` until it is resumed rather than being queued again.
* Unlike without Kueue, a `SparkApplication` whose driver and `spark.executor.instances` executors
  are not all ready within the `waitForPodsReady` timeout, e.g. since some executors cannot be
  scheduled, does not keep running with fewer executors, even if its `applicationTolerations`
  allow it, e.g. in `RunningWithPartialCapacity`. Its `Workload` requested the quota for all of
  them, so the `PodsReadyTimeout` releases the application as above, and it runs again from
  scratch with a new attempt, which waits for the quota of all of them again. Like such a cluster,
  an application whose executors never get ready is admitted and evicted again and again, with a
  new attempt each time, until it is fixed, suspended or deleted.
* To limit the execution time of a `SparkApplication`, the Spark native `spark.driver.timeout` is
  recommended instead of the `kueue.x-k8s.io/max-exec-time-seconds` label, which is not copied to
  the `Workload`. It requires `spark.plugins=org.apache.spark.deploy.DriverTimeoutPlugin`, as in
  [this example](../examples/pi-with-driver-timeout.yaml).

## Spark Cluster

Spark Operator also supports launching Spark clusters in k8s via `SparkCluster` custom resource,
which takes minimal effort to specify desired master and worker instances spec.

To deploy a Spark cluster, you may start with specifying the desired Spark version, worker count as
well as the SparkConf as in the [example](../examples/qa-cluster-with-one-worker.yaml). Master &
worker instances would be deployed as [StatefulSets](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/)
and exposed via k8s [service(s)](https://kubernetes.io/docs/concepts/services-networking/service/).

Like Pod Template Support for Applications, it's also possible to submit template(s) for the Spark
instances for `SparkCluster` to configure spec that's not supported via SparkConf. It's worth notice
that Spark may overwrite certain fields.

The master and worker pods use `terminationGracePeriodSeconds: 0` by default to delete a
`SparkCluster` faster, unless their pod templates set it. A longer grace period delays stopping
a [suspended](#suspend) cluster and releasing its [Kueue](#kueue) quota accordingly.
