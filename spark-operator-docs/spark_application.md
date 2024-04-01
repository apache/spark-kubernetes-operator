## Spark Application API

The core user facing API of the Spark Kubernetes Operator is the SparkApplication Custom 
Resources Definition (CRD). Spark Application CustomResource extends standard k8s API, 
defines Spark Application spec and tracks status.  

Once the Spark Operator is installed and running in your Kubernetes environment, it will 
continuously watch SparkApplication(s) submitted, via k8s API client or kubectl by the user,
orchestrate secondary resources (pods, configmaps .etc). 

Please check out the [quickstart](getting_started.md) as well for installing operator.

## SparkApplication

SparkApplication can be defined in YAML format and with bare minimal required fields in 
order to start:

```
apiVersion: org.apache.spark/v1alpha1
kind: SparkApplication
metadata:
  name: spark-pi
  namespace: spark-test
spec:
  mainClass: "org.apache.spark.examples.SparkPi"
  jars: "local:///opt/spark/examples/jars/spark-examples.jar"
  sparkConf:
    spark.executor.instances: "5"
    spark.kubernetes.container.image: "spark:3.4.1-scala2.12-java11-python3-r-ubuntu"
    spark.kubernetes.namespace: "spark-test"
    spark.kubernetes.authenticate.driver.serviceAccountName: "spark"
  runtimeVersions:
    scalaVersion: v2_12
    sparkVersion: v3_4_1

```


After submitted, Operator will add status information to your application based on the 
observed state:

```
kubectl get sparkapp spark-pi -o yaml
```

### Write and build your SparkApplication

It's straightforward to convert your spark-submit application to `SparkApplication` yaml. 
Operators constructs driver spec in the similar approach. To submit Java / scala application, 
use `.spec.jars` and `.spec.mainClass`. Similarly, set `pyFiles` or `sparkRFiles` for Python / 
SparkR applications.

While building images to use by driver and executor, it's recommended to use official 
[Spark Docker](https://github.com/apache/spark-docker) as base images. Check the pod template 
support (`.spec.driverSpec.podTemplateSpec` and `.spec.executorSpec.podTemplateSpec`) as well for 
setting custom Spark home and work dir.  

### Pod Template Support

It is possible to configure pod template for driver & executor pods for configure spec that are
not configurable from
SparkConf.

Spark Operator supports defining pod template for driver and executor pods in two ways:

1. Set `PodTemplateSpec` in `SparkApplication`
2. Config `spark.kubernetes.[driver/executor].podTemplateFile`

See [this example](../spark-operator/src/main/resources/streaming.yaml) for configure pod
template in SparkApplication.

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

## Understanding Failure Types

In addition to the general `FAILURE` state (that driver pod fails or driver container exits 
with non-zero code), Spark Operator introduces a few different failure state for ease of 
app status monitoring at high level, and for ease of setting up different handlers if users 
are creating / managing SparkApplications with external microservices or workflow engines. 


Spark Operator recognizes "infrastructure failure" in the best effort way. It is possible to 
configure different restart policy on general failure(s) vs. on potential infrastructure 
failure(s). For example, you may configure the app to restart only upon infrastructure 
failures. If Spark application fails as a result of 

```
DRIVER_LAUNCH_TIMED_OUT
EXECUTORS_LAUNCH_TIMED_OUT
SCHEDULING_FAILURE
```

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
  # accptable values are 'NEVER', 'ALWAYS', ON_FAILURE and 'ON_INFRASTRUCTURE_FAILURE'
  restartPolicy: NEVER
  # operator would retry the application if configured. All resources from current attepmt 
  # would be deleted before starting next attempt
  maxRestartAttempts: 3 
  # backoff time (in millis) that operator would wait before next attempt
  restartBackoffMillis: 30000
```

### Timeouts

Example for configure timeouts:

```yaml
applicationTimeoutConfig:
  # timeouts set to 5min

  # time to wait for driver reaches running state after requested driver
  driverStartTimeoutMillis: 300000

  # time to wait for driver reaches ready state  
  sparkSessionStartTimeoutMillis: 300000

  # time to wait for driver to acquire minimal number of running executors
  executorStartTimeoutMillis: 300000

  # time to wait for force delete resources at the end of attempt
  forceTerminationGracePeriodMillis: 300000
```
 
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
* Spark app would be marked as 'RUNNING_WITH_PARTIAL_CAPACITY' if it loses executors after 
  successfully start up.
* Spark app would be marked as 'RUNNING_HEALTHY' if it has at least min executors after 
  successfully started up.

### Delete Resources On Termination

Operator by default would delete all created resources at the end of an attempt. It would 
try to record the last observed driver status in `status` field of the application for 
troubleshooting purpose. 

On the other hand, when developing an application, it's possible to configure

```yaml
applicationTolerations:
  deleteOnTermination: false
```

So operator would not attempt to delete resources after app terminates. Note that this 
applies only to operator-created resources (driver .etc). You may also want to tune
`spark.kubernetes.executor.deleteOnTermination` to control the behavior of driver-created 
resources.

## Supported Spark Versions

Spark Version is a required field for SparkApplication. At current phase, operator uses
single submission-worker mode to support all listed versions.

```yaml
runtimeVersions:
  # Supported values are:
  # v3_5_1, v3_5_0, v3_4_1, v3_4_0, v3_3_3, v3_3_1, v3_3_0, v3_2_0
  sparkVersion: v3_4_0
```
