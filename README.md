# Apache Spark Kubernetes Operator

[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/spark-kubernetes-operator)](https://artifacthub.io/packages/search?repo=spark-kubernetes-operator)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Apache Spark™ K8s Operator is a subproject of [Apache Spark](https://spark.apache.org/) and
aims to extend K8s resource manager to manage Apache Spark applications and clusters via
[Operator Pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/).

## Releases

- <https://github.com/apache/spark-kubernetes-operator/releases/tag/v0.1.0>

## Install Helm Chart

```bash
$ helm repo add spark-kubernetes-operator https://apache.github.io/spark-kubernetes-operator
$ helm repo update
$ helm install spark-kubernetes-operator spark-kubernetes-operator/spark-kubernetes-operator
$ helm list
NAME                     	NAMESPACE	REVISION	UPDATED                             	STATUS  	CHART                          	APP VERSION
spark-kubernetes-operator	default  	1       	2025-05-13 12:11:15.303067 -0700 PDT	deployed	spark-kubernetes-operator-0.1.0	0.1.0
```

## Run Spark Pi App

```bash
$ kubectl apply -f https://raw.githubusercontent.com/apache/spark-kubernetes-operator/refs/tags/v0.1.0/examples/pi.yaml

$ kubectl get sparkapp
NAME   CURRENT STATE      AGE
pi     ResourceReleased   4m10s

$ kubectl delete sparkapp/pi
```

## Run Spark Cluster

```bash
$ kubectl apply -f https://raw.githubusercontent.com/apache/spark-kubernetes-operator/refs/tags/v0.1.0/examples/prod-cluster-with-three-workers.yaml

$ kubectl get sparkcluster
NAME   CURRENT STATE    AGE
prod   RunningHealthy   10s

$ kubectl delete sparkcluster prod
sparkcluster.spark.apache.org "prod" deleted
```

## Clean Up

Check the existing Spark applications and clusters. If exists, delete them.

```
$ kubectl get sparkapp
No resources found in default namespace.

$ kubectl get sparkcluster
No resources found in default namespace.
```

Remove HelmChart and CRDs.

```
$ helm uninstall spark-kubernetes-operator

$ kubectl delete crd sparkapplications.spark.apache.org

$ kubectl delete crd sparkclusters.spark.apache.org
```
