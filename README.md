# Apache Spark K8s Operator

[![GitHub Actions Build](https://github.com/apache/spark-kubernetes-operator/actions/workflows/build_and_test.yml/badge.svg)](https://github.com/apache/spark-kubernetes-operator/actions/workflows/build_and_test.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Repo Size](https://img.shields.io/github/repo-size/apache/spark-kubernetes-operator)](https://img.shields.io/github/repo-size/apache/spark-kubernetes-operator)

Apache Spark™ K8s Operator is a subproject of [Apache Spark](https://spark.apache.org/) and
aims to extend K8s resource manager to manage Apache Spark applications via
[Operator Pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/).

## Building Spark K8s Operator

Spark K8s Operator is built using Gradle.
To build, run:

```bash
$ ./gradlew build -x test
```

## Running Tests

```bash
$ ./gradlew build
```

## Build Docker Image

```bash
$ ./gradlew buildDockerImage
```

## Install Helm Chart

```bash
$ ./gradlew spark-operator-api:relocateGeneratedCRD

$ helm install spark-kubernetes-operator --create-namespace -f build-tools/helm/spark-kubernetes-operator/values.yaml build-tools/helm/spark-kubernetes-operator/
```

## Run Spark Pi App

```bash
$ kubectl apply -f examples/pi.yaml

$ kubectl get sparkapp
NAME   CURRENT STATE      AGE
pi     ResourceReleased   4m10s

$ kubectl delete sparkapp/pi
```

## Run Spark Cluster

```bash
$ kubectl apply -f examples/prod-cluster-with-three-workers.yaml

$ kubectl get sparkcluster
NAME   CURRENT STATE    AGE
prod   RunningHealthy   10s

$ kubectl port-forward prod-master-0 6066 &

$ ./examples/submit-pi-to-prod.sh
{
  "action" : "CreateSubmissionResponse",
  "message" : "Driver successfully submitted as driver-20240821181327-0000",
  "serverSparkVersion" : "4.0.0-preview1",
  "submissionId" : "driver-20240821181327-0000",
  "success" : true
}

$ curl http://localhost:6066/v1/submissions/status/driver-20240821181327-0000/
{
  "action" : "SubmissionStatusResponse",
  "driverState" : "FINISHED",
  "serverSparkVersion" : "4.0.0-preview1",
  "submissionId" : "driver-20240821181327-0000",
  "success" : true,
  "workerHostPort" : "10.1.5.188:42099",
  "workerId" : "worker-20240821181236-10.1.5.188-42099"
}

$ kubectl delete sparkcluster prod
sparkcluster.spark.apache.org "prod" deleted
```

## Run Spark Pi App on Apache YuniKorn scheduler

```bash
$ helm install yunikorn yunikorn/yunikorn --namespace yunikorn --version 1.5.2 --create-namespace --set embedAdmissionController=false

$ kubectl apply -f examples/pi-on-yunikorn.yaml

$ kubectl describe pod pi-on-yunikorn-0-driver
...
Events:
  Type    Reason             Age   From      Message
  ----    ------             ----  ----      -------
  Normal  Scheduling         14s   yunikorn  default/pi-on-yunikorn-0-driver is queued and waiting for allocation
  Normal  Scheduled          14s   yunikorn  Successfully assigned default/pi-on-yunikorn-0-driver to node docker-desktop
  Normal  PodBindSuccessful  14s   yunikorn  Pod default/pi-on-yunikorn-0-driver is successfully bound to node docker-desktop
  Normal  TaskCompleted      6s    yunikorn  Task default/pi-on-yunikorn-0-driver is completed
  Normal  Pulled             13s   kubelet   Container image "spark:4.0.0-preview1" already present on machine
  Normal  Created            13s   kubelet   Created container spark-kubernetes-driver
  Normal  Started            13s   kubelet   Started container spark-kubernetes-driver

$ kubectl delete sparkapp pi-on-yunikorn
sparkapplication.spark.apache.org "pi-on-yunikorn" deleted
```

## Try nightly build for testing

As of now, you can try `spark-kubernetes-operator` nightly version in the following way.

```
$ helm install spark-kubernetes-operator \
https://nightlies.apache.org/spark/charts/spark-kubernetes-operator-0.1.0-SNAPSHOT.tgz \
--set image.repository=dongjoon/spark-kubernetes-operator
```

## Contributing

Please review the [Contribution to Spark guide](https://spark.apache.org/contributing.html)
for information on how to get started contributing to the project.
