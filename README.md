# Apache Spark K8s Operator

[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/spark-kubernetes-operator)](https://artifacthub.io/packages/search?repo=spark-kubernetes-operator)
[![GitHub Actions Build](https://github.com/apache/spark-kubernetes-operator/actions/workflows/build_and_test.yml/badge.svg)](https://github.com/apache/spark-kubernetes-operator/actions/workflows/build_and_test.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Repo Size](https://img.shields.io/github/repo-size/apache/spark-kubernetes-operator)](https://img.shields.io/github/repo-size/apache/spark-kubernetes-operator)

Apache Spark™ K8s Operator is a subproject of [Apache Spark](https://spark.apache.org/) and
aims to extend K8s resource manager to manage Apache Spark applications via
[Operator Pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/).

## Install Helm Chart

Apache Spark provides a Helm Chart.

- <https://apache.github.io/spark-kubernetes-operator/>
- <https://artifacthub.io/packages/helm/spark-kubernetes-operator/spark-kubernetes-operator/>

```bash
helm repo add spark https://apache.github.io/spark-kubernetes-operator
helm repo update
helm install spark spark/spark-kubernetes-operator
```

## Building Spark K8s Operator

Spark K8s Operator is built using Gradle.
To build, run:

```bash
./gradlew build -x test
```

## Running Tests

```bash
./gradlew build
```

## Build Docker Image

```bash
./gradlew buildDockerImage
```

## Install Helm Chart from the source code

```bash
helm install spark -f build-tools/helm/spark-kubernetes-operator/values.yaml build-tools/helm/spark-kubernetes-operator/
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
  "message" : "Driver successfully submitted as driver-20251219002524-0000",
  "serverSparkVersion" : "4.1.0",
  "submissionId" : "driver-20251219002524-0000",
  "success" : true
}

$ curl http://localhost:6066/v1/submissions/status/driver-20251219002524-0000/
{
  "action" : "SubmissionStatusResponse",
  "driverState" : "FINISHED",
  "serverSparkVersion" : "4.1.0",
  "submissionId" : "driver-20251219002524-0000",
  "success" : true,
  "workerHostPort" : "10.1.0.190:46501",
  "workerId" : "worker-20251219002506-10.1.0.190-46501"
}

$ kubectl delete sparkcluster prod
sparkcluster.spark.apache.org "prod" deleted
```

## Run Spark Pi App on Apache YuniKorn scheduler

If you have not yet done so, follow [YuniKorn docs](https://yunikorn.apache.org/docs/#install) to install the latest version:

```bash
helm repo add yunikorn https://apache.github.io/yunikorn-release

helm repo update

helm install yunikorn yunikorn/yunikorn --namespace yunikorn --version 1.7.0 --create-namespace --set embedAdmissionController=false
```

Submit a Spark app to YuniKorn enabled cluster:

```bash

$ kubectl apply -f examples/pi-on-yunikorn.yaml

$ kubectl describe pod pi-on-yunikorn-0-driver
...
Events:
  Type    Reason             Age   From      Message
  ----    ------             ----  ----      -------
  Normal  Scheduling         1s    yunikorn  default/pi-on-yunikorn-0-driver is queued and waiting for allocation
  Normal  Scheduled          1s    yunikorn  Successfully assigned default/pi-on-yunikorn-0-driver to node docker-desktop
  Normal  PodBindSuccessful  1s    yunikorn  Pod default/pi-on-yunikorn-0-driver is successfully bound to node docker-desktop
  Normal  Pulled             0s    kubelet   Container image "apache/spark:4.1.0-scala" already present on machine
  Normal  Created            0s    kubelet   Created container: spark-kubernetes-driver
  Normal  Started            0s    kubelet   Started container spark-kubernetes-driver

$ kubectl delete sparkapp pi-on-yunikorn
sparkapplication.spark.apache.org "pi-on-yunikorn" deleted from default namespace
```

## Clean Up

Check the existing Spark applications and clusters. If exists, delete them.

```bash
$ kubectl get sparkapp
No resources found in default namespace.

$ kubectl get sparkcluster
No resources found in default namespace.
```

Remove HelmChart and CRDs.

```bash
helm uninstall spark

kubectl delete crd sparkapplications.spark.apache.org

kubectl delete crd sparkclusters.spark.apache.org
```

## Contributing

Please review the [Contribution to Spark guide](https://spark.apache.org/contributing.html)
for information on how to get started contributing to the project.
