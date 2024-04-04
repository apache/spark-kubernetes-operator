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

# Configuration

## Configure Operator

Spark Operator supports different ways to configure the behavior:

* **spark-operator.properties** provided when deploying the operator. In addition to the
  [property file](../build-tools/helm/spark-kubernetes-operator/conf/spark-operator.
  properties), it is also possible to override or append config properties in helm [Values
  files](../build-tools/helm/spark-kubernetes-operator/values.yaml).
* **System Properties** : when provided as system properties (e.g. via -D options to the
  operator JVM), it overrides the values provided in property file.
* **Hot property loading** : when enabled, a [configmap](https://kubernetes.
  io/docs/concepts/configuration/configmap/) would be created with the operator in the same
  namespace. Operator would monitor updates performed on the configmap. Hot properties
  override takes highest precedence.
    - An example use case: operator use hot properties to figure the list of namespace(s) to
      operate Spark applications. The hot properties config map can be updated and
      maintained by user or additional microservice to tune the operator behavior without
      rebooting it.
    - Please be advised that not all properties can be hot-loaded and honored at runtime.
      Refer the list of supported properties section for more details.

To enable hot properties loading, update the **helm chart values file** with

```

operatorConfiguration:
  spark-operator.properties: |+
    spark.operator.dynamic.config.enabled=true
    # ... all other config overides...
  dynamicConfig:
    create: true

```

## Supported Config Properties

| Name                                                                         | Type    | Default Value                                | Allow Hot Property Override | Description                                                                                                                                                                                                                                                                          |
|------------------------------------------------------------------------------|---------|----------------------------------------------|-----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| spark.operator.name                                                          | string  | spark-kubernetes-operator                    | false                       | Name of the operator.                                                                                                                                                                                                                                                                |
| spark.operator.namespace                                                     | string  | spark-system                                 | false                       | Namespace that operator is deployed within.                                                                                                                                                                                                                                          |
| spark.operator.watched.namespaces                                            | string  |                                              | true                        | Comma-separated list of namespaces that the operator would be watching for Spark resources. If unset, operator would watch all namespaces by default. When deployed via Helm, please note that the value should be a subset of .Values.appResources.namespaces.data.                 |
| spark.operator.dynamic.config.enabled                                        | boolean | false                                        | false                       | When enabled, operator would use config map as source of truth for config property override. The config map need to be created in spark.operator.namespace, and labeled with operator name.                                                                                          |
| spark.operator.dynamic.config.selector.str                                   | string  | `app.kubernetes.io/component=dynamic-config` | false                       | The selector str applied to dynamic config map.                                                                                                                                                                                                                                      |
| spark.operator.terminate.on.informer.failure                                 | boolean | false                                        | false                       | Enable to indicate informer errors should stop operator startup. If disabled, operator startup will ignore recoverable errors, caused for example by RBAC issues and will retry periodically                                                                                         |
| spark.operator.termination.timeout.seconds                                   | integer | 30                                           | false                       | Grace period for operator shutdown before reconciliation threads are killed.                                                                                                                                                                                                         |
| spark.operator.reconciler.parallelism                                        | integer | 30                                           | false                       | Thread pool size for Spark Operator reconcilers. Use -1 for unbounded pool.                                                                                                                                                                                                          |
| spark.operator.rate.limiter.refresh.period.seconds                           | integer | 15                                           | false                       | Operator rate limiter refresh period(in seconds) for each resource.                                                                                                                                                                                                                  |
| spark.operator.rate.limiter.limit                                            | integer | 5                                            | false                       | Max number of reconcile loops triggered within the rate limiter refresh period for each resource. Setting the limit <= 0 disables the limiter.                                                                                                                                       |
| spark.operator.retry.initial.internal.seconds                                | integer | 5                                            | false                       | Initial interval(in seconds) of retries on unhandled controller errors                                                                                                                                                                                                               |
| spark.operator.retry.internal.multiplier                                     | double  | 1.5                                          | false                       | Interval multiplier of retries on unhandled controller errors.                                                                                                                                                                                                                       |
| spark.operator.retry.max.interval.seconds                                    | integer | -1                                           | false                       | Max interval(in seconds) of retries on unhandled controller errors. Set to -1 for unlimited.                                                                                                                                                                                         |
| spark.operator.retry.max.attempts                                            | integer | 15                                           | false                       | Max attempts of retries on unhandled controller errors.                                                                                                                                                                                                                              |
| spark.operator.driver.create.max.attempts                                    | integer | 3                                            | true                        | Maximal number of retry attempts of requesting driver for Spark application.                                                                                                                                                                                                         |
| spark.operator.max.retry.attempts.on.k8s.failure                             | long    | 3                                            | true                        | Maximal number of retry attempts of requests to k8s server upon response 429 and 5xx.                                                                                                                                                                                                |
| spark.operator.retry.attempt.after.seconds                                   | long    | 1                                            | true                        | Default time (in seconds) to wait till next request. This would be used if server does not set Retry-After in response.                                                                                                                                                              |
| spark.operator.max.retry.attempt.after.seconds                               | long    | 15                                           | true                        | Maximal time (in seconds) to wait till next request.                                                                                                                                                                                                                                 |
| spark.operator.status.patch.max.retry                                        | long    | 3                                            | true                        | Maximal number of retry attempts of requests to k8s server for resource status update.                                                                                                                                                                                               |
| spark.operator.status.patch.failure.backoff.seconds                          | long    | 3                                            | true                        | Default time (in seconds) to wait till next request to patch resource status update.                                                                                                                                                                                                 |
| spark.operator.app.reconcile.interval.seconds                                | long    | 120                                          | true                        | Interval (in seconds) to reconcile when application is is starting up. Note that reconcile is always expected to be triggered per update - this interval controls the reconcile behavior when operator still need to reconcile even when there's no update ,e.g. for timeout checks. |
| spark.operator.foreground.request.timeout.seconds                            | long    | 120                                          | true                        | Timeout (in seconds) to for requests made to API server. this applies only to foreground requests.                                                                                                                                                                                   |
| spark.operator.trim.attempt.state.transition.history                         | boolean | true                                         | true                        | When enabled, operator would trim state transition history when a new attempt starts, keeping previous attempt summary only.                                                                                                                                                         |
| spark.operator.josdk.metrics.enabled                                         | boolean | true                                         | true                        | When enabled, the josdk metrics will be added in metrics source and configured for operator.                                                                                                                                                                                         |
| spark.operator.kubernetes.client.metrics.enabled                             | boolean | true                                         | true                        | Enable KubernetesClient metrics for measuring the HTTP traffic to the Kubernetes API Server. Since the metrics is collected via Okhttp interceptors, can be disabled when opt in customized interceptors.                                                                            |
| spark.operator.kubernetes.client.metrics.group.by.response.code.group.enable | boolean | true                                         | true                        | When enabled, additional metrics group by http response code group(1xx, 2xx, 3xx, 4xx, 5xx) received from API server will be added. Users can disable it when their monitoring system can combine lower level kubernetes.client.http.response.<3-digit-response-code> metrics.       |
| spark.operator.probe.port                                                    | integer | 18080                                        | false                       | The port used for health/readiness check probe status.                                                                                                                                                                                                                               |
| spark.operator.sentinel.executor.pool.size                                   | integer | 3                                            | false                       | Size of executor service in Sentinel Managers to check the health of sentinel resources.                                                                                                                                                                                             |
| spark.operator.health.sentinel.resource.reconciliation.delay.seconds         | integer | 60                                           | true                        | Allowed max time(seconds) between spec update and reconciliation for sentinel resources.                                                                                                                                                                                             |
| spark.operator.leader.election.enabled                                       | boolean | false                                        | false                       | Enable leader election for the operator to allow running standby instances.                                                                                                                                                                                                          |
| spark.operator.leader.election.lease.name                                    | string  | spark-operator-lease                         | false                       | Leader election lease name, must be unique for leases in the same namespace.                                                                                                                                                                                                         |
| spark.operator.leader.election.lease.duration.seconds                        | long    | 1200                                         | false                       | Leader election lease duration.                                                                                                                                                                                                                                                      |
| spark.operator.leader.election.renew.deadline.seconds                        | long    | 600                                          | false                       | Leader election renew deadline.                                                                                                                                                                                                                                                      |
| spark.operator.leader.election.retry.period.seconds                          | long    | 180                                          | false                       | Leader election retry period.                                                                                                                                                                                                                                                        |
| spark.operator.metrics.port                                                  | integer | 19090                                        | false                       | The port used for export metrics.                                                                                                                                                                                                                                                    |

## Config Metrics Publishing Behavior

Spark Operator uses the same source & sink interface as Apache Spark. This means you can 
use existing Spark metrics sink for both applications and the operator.
