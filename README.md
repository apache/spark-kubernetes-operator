# Apache Spark K8s Operator

Apache Spark K8s Operator is a subproject of [Apache Spark](https://spark.apache.org/) and
aims to extend K8s resource manager to manage Apache Spark applications via
[Operator Pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/).

[![GitHub Actions Build](https://github.com/apache/spark-kubernetes-operator/actions/workflows/build_and_test.yml/badge.svg)](https://github.com/apache/spark-kubernetes-operator/actions/workflows/build_and_test.yml)

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

## Contributing

Please review the [Contribution to Spark guide](https://spark.apache.org/contributing.html)
for information on how to get started contributing to the project.
