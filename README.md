# Spark-Kubernetes-Operator

Welcome to the **Spark-Kubernetes-Operator**, a Kubernetes operator designed to simplify and
automate the management of Spark applications in Kubernetes environments.

## Project Status

As of Apr 1, 2024, Spark-Kubernetes-Operator is under Active Development.

- We are actively working on new features and improvements. We welcome contributions and
  feedback to make the operator even better. Check out the **Issues** section to see what's
  currently in progress or suggest new features.
- Current API Version: `v1alpha1`

## Key Features

- Deploy and monitor SparkApplications throughout its lifecycle
- Start / stop Spark Apps with simple yaml schema
- Spark version agnostic
- Full logging and metrics integration
- Flexible deployments and native integration with Kubernetes tooling

Please refer the [design](spark-operator-docs/architecture.md) section for architecture and 
design.

## Quickstart

[Getting started doc](./spark-operator-docs/getting_started.md) gives an example to install 
operator and run Spark Applications locally.

In addition, [SparkApplication](./spark-operator-docs/spark_application.md) section 
describes how to write your own apps, [Operations](./spark-operator-docs/operations.md) section 
describes how to install operator with custom config overriding.



## Contributing

You can learn more about how to contribute in the [Apache Spark website](https://spark.
apache.org/contributing.html). 

## License

The code in this repository is licensed under the [Apache Software License 2](./LICENSE).
