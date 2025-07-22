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
    spark.dynamicAllocation.shuffleTracking.enabled: "true"
    spark.dynamicAllocation.maxExecutors: "3"
    spark.kubernetes.authenticate.driver.serviceAccountName: "spark"
    spark.kubernetes.container.image: "apache/spark:4.0.0"
  applicationTolerations:
    resourceRetainPolicy: OnFailure
  runtimeVersions:
    scalaVersion: "2.13"
    sparkVersion: "4.0.0"
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
failures. If Spark application fails as a result of `DriverStartTimedOut`,
`ExecutorsStartTimedOut`, `SchedulingFailure`.

It is more likely that the app failed as a result of infrastructure reason(s), including
scenarios like driver or executors cannot be scheduled or cannot initialize in configured
time window for scheduler reasons, as a result of insufficient capacity, cannot get IP
allocated, cannot pull images, or k8s API server issue at scheduling .etc.

Please be advised that this is a best-effort failure identification. You may still need to
debug actual failure from the driver pods. Spark Operator would stage the last observed
driver pod status with the stopping state for audit purposes.

## Configure the Tolerations for SparkApplication

### Restart

Spark Operator enables configure app restart behavior for different failure types. Here's a
sample restart config snippet:

``` yaml
restartConfig:
  # accptable values are 'Never', 'Always', 'OnFailure' and 'OnInfrastructureFailure'
  restartPolicy: Never
  # operator would retry the application if configured. All resources from current attepmt
  # would be deleted before starting next attempt
  maxRestartAttempts: 3
  # backoff time (in millis) that operator would wait before next attempt
  restartBackoffMillis: 30000
```

### Timeouts

It's possible to configure applications to be proactively terminated and resubmitted in particular
cases to avoid resource deadlock.

| Field                                                                                   | Type    | Default Value | Descritpion                                                                                                        |
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

* If Spark app acquires less than 5 executors in given tine window (.spec.
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
is configured to restart, `resourceRetainPolicy`, `resourceRetainDurationMillis` and
`ttlAfterStopMillis` would be applied only to the last attempt.

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
