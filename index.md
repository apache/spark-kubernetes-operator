# Apache Spark™ K8s Operator

[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/spark-kubernetes-operator)](https://artifacthub.io/packages/helm/spark-kubernetes-operator/spark-kubernetes-operator)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Apache Spark™ K8s Operator is a subproject of [Apache Spark](https://spark.apache.org/) and
aims to extend K8s resource manager to manage Apache Spark applications and clusters via
[Operator Pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/).

## Releases

- [0.2.0 (2025-05-20)](https://github.com/apache/spark-kubernetes-operator/releases/tag/0.2.0)
- [0.1.0 (2025-05-08)](https://github.com/apache/spark-kubernetes-operator/releases/tag/v0.1.0)

## Requirements

- Apache Spark 3.5+
- Kubernetes 1.30+ cluster
- Helm 3.0+

## Install Helm Chart

```bash
$ helm repo add spark https://apache.github.io/spark-kubernetes-operator
$ helm repo update
$ helm install spark spark/spark-kubernetes-operator
$ helm list
NAME  NAMESPACE REVISION UPDATED                             STATUS   CHART                           APP VERSION
spark default   1        2025-05-20 11:32:45.58896 -0700 PDT deployed spark-kubernetes-operator-1.0.0 0.2.0
```

## Run Spark Pi App

```bash
$ kubectl apply -f https://apache.github.io/spark-kubernetes-operator/pi.yaml
sparkapplication.spark.apache.org/pi created

$ kubectl get sparkapp
NAME   CURRENT STATE      AGE
pi     ResourceReleased   4m10s

$ kubectl delete sparkapp pi
sparkapplication.spark.apache.org "pi" deleted
```

## Run Spark Connect Server (A long-running app)

```bash
$ kubectl apply -f https://apache.github.io/spark-kubernetes-operator/spark-connect-server.yaml
sparkapplication.spark.apache.org/spark-connect-server created

$ kubectl get sparkapp
NAME                   CURRENT STATE    AGE
spark-connect-server   RunningHealthy   14h

$ kubectl delete sparkapp spark-connect-server
sparkapplication.spark.apache.org "spark-connect-server" deleted
```

## Run [Spark Connect Swift](https://apache.github.io/spark-connect-swift/) App via [K8s Job](https://kubernetes.io/docs/concepts/workloads/controllers/job/)

```bash
$ kubectl apply -f https://apache.github.io/spark-kubernetes-operator/pi-swift.yaml
job.batch/spark-connect-swift-pi created

$ kubectl logs -f job/spark-connect-swift-pi
Pi is roughly 3.1426151426151425
```

## Run Spark Cluster

```bash
$ kubectl apply -f https://raw.githubusercontent.com/apache/spark-kubernetes-operator/refs/tags/0.2.0/examples/prod-cluster-with-three-workers.yaml
sparkcluster.spark.apache.org/prod created

$ kubectl get sparkcluster
NAME   CURRENT STATE    AGE
prod   RunningHealthy   10s

$ kubectl delete sparkcluster prod
sparkcluster.spark.apache.org "prod" deleted
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
