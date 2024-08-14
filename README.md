# Apache Spark K8s Operator

Apache Spark K8s Operator is a subproject of [Apache Spark](https://spark.apache.org/) and
aims to extend K8s resource manager to manage Apache Spark applications via
[Operator Pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/).

[![GitHub Actions Build](https://github.com/apache/spark-kubernetes-operator/actions/workflows/build_and_test.yml/badge.svg)](https://github.com/apache/spark-kubernetes-operator/actions/workflows/build_and_test.yml)

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

## Contributing

Please review the [Contribution to Spark guide](https://spark.apache.org/contributing.html)
for information on how to get started contributing to the project.
