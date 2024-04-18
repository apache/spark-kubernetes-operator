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

## Getting Started

This doc provides a quick introduction to creating and managing Spark applications with 
Operator. 

To follow along with this guide, first, clone this repository and have a 
[Minikube](https://minikube.sigs.k8s.io/docs/) cluster ready for a quick start of running examples 
locally. Make sure to update kube config as well - this example would deploy Spark Operator 
and run Spark application(s) in the current context and namespace.

It is possible to try the operator on remote k8s cluster (EKS / GKE .etc). To do so, make 
sure you publish the built operator image to a docker registry that's accessible for the 
cluster. 

### Compatibility

- JDK17, or 23
- Operator used fabric8 which assumes to be compatible with available k8s versions.
- Spark versions 3.4 and above

### Start minikube

Start miniKube and make it access locally-built image

```shell
minikube start
eval $(minikube docker-env)
```

### Build Spark Operator Locally

   ```bash
   # Build a local container image which can be used for minikube.etc. 
   # For testing in remote k8s cluster, please also do `docker push` to make it available 
   # to the cluster / nodes 
   docker build --build-arg BASE_VERSION=0.1.0 -t spark-kubernetes-operator:0.1.0 .
   
   # Generate CRD yaml and make it available for chart deployment
   ./gradlew spark-operator-api:copyGeneratedCRD     
   ```
### Install the Spark Operator

   ```bash
   helm install spark-kubernetes-operator -f build-tools/helm/spark-kubernetes-operator/values.yaml build-tools/helm/spark-kubernetes-operator/
   ```
### Verify the Installation

Check if the pods are up and running:
   ```shell
   $ kubectl get pods
   ```

Which would show operator pod like

```
NAME                                        READY   STATUS    RESTARTS   AGE
spark-kubernetes-operator-995d88bdf-nwr7r   1/1     Running   0          16s
```

You may also find the installed CRD with

   ```shell
   $ kubectl get crd sparkapplications.org.apache.spark
   ```


### Start Spark Application

Start Spark-pi with

   ```bash
   kubectl create -f spark-operator/src/main/resources/spark-pi.yaml
   ```

### Monitor Spark Application State Transition

   ```bash
   kubectl get sparkapp spark-pi -o yaml 
   ```

It should give Spark application spec as well as the state transition history, for example

```
apiVersion: org.apache.spark/v1alpha1
kind: SparkApplication
metadata:
  creationTimestamp: "2024-04-02T22:24:47Z"
  finalizers:
  - sparkapplications.org.apache.spark/finalizer
  generation: 2
  name: spark-pi
  namespace: default
  resourceVersion: "963"
  uid: 356dedb1-0c09-4515-9233-165d28ae6d27
spec:
  applicationTolerations:
    applicationTimeoutConfig:
      driverStartTimeoutMillis: 300000
      executorStartTimeoutMillis: 300000
      forceTerminationGracePeriodMillis: 300000
      sparkSessionStartTimeoutMillis: 300000
      terminationRequeuePeriodMillis: 2000
    resourceRetentionPolicy: OnFailure
    instanceConfig:
      initExecutors: 0
      maxExecutors: 0
      minExecutors: 0
    restartConfig:
      maxRestartAttempts: 3
      restartBackoffMillis: 30000
      restartPolicy: NEVER
  deploymentMode: CLUSTER_MODE
  driverArgs: []
  jars: local:///opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar
  mainClass: org.apache.spark.examples.SparkPi
  runtimeVersions:
    scalaVersion: v2_12
    sparkVersion: v3_5_1
  sparkConf:
    spark.executor.instances: "5"
    spark.kubernetes.authenticate.driver.serviceAccountName: spark
    spark.kubernetes.container.image: spark:3.5.1-scala2.12-java11-python3-r-ubuntu
    spark.kubernetes.namespace: spark-test
status:
  currentAttemptSummary:
    attemptInfo:
      id: 0
  currentState:
    currentStateSummary: RUNNING_HEALTHY
    lastTransitionTime: "2024-04-02T22:24:52.342061Z"
    message: 'Application is running healthy. '
  stateTransitionHistory:
    "0":
      currentStateSummary: SUBMITTED
      lastTransitionTime: "2024-04-02T22:24:47.592355Z"
      message: 'Spark application has been created on Kubernetes Cluster. '
    "1":
      currentStateSummary: DRIVER_REQUESTED
      lastTransitionTime: "2024-04-02T22:24:50.268363Z"
      message: 'Requested driver from resource scheduler. '
    "2":
      currentStateSummary: DRIVER_STARTED
      lastTransitionTime: "2024-04-02T22:24:52.238794Z"
      message: 'Driver has started running. '
    "3":
      currentStateSummary: DRIVER_READY
      lastTransitionTime: "2024-04-02T22:24:52.239101Z"
      message: 'Driver has reached ready state. '
    "4":
      currentStateSummary: RUNNING_HEALTHY
      lastTransitionTime: "2024-04-02T22:24:52.342061Z"
      message: 'Application is running healthy. '
```

Delete application Spark-pi and its secondary resources with

   ```bash
   kubectl delete -f spark-operator/src/main/resources/spark-pi.yaml
   ```


#### Uninstallation

To remove the installed resources from your cluster, use:

```bash
helm uninstall spark-kubernetes-operator
```

### More examples

More PySpark / SparkR examples can be found under [e2e-tests](../e2e-tests).

Read more about how to understand, write and build your SparkApplication [here](spark_application.md).
