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

# Helm Chart for Apache Spark

[![Artifact Hub](https://img.shields.io/endpoint?url=https://artifacthub.io/badge/repository/spark-kubernetes-operator)](https://artifacthub.io/packages/search?repo=spark-kubernetes-operator)

[Apache Spark™](https://spark.apache.org/) is a unified analytics engine for large-scale data processing, and
Apache Spark™ K8s Operator is a subproject of Apache Spark and
aims to extend K8s resource manager to manage Apache Spark applications and clusters via
[Operator Pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/).

## Introduction

This chart will bootstrap an [Apache Spark K8s Operator](https://apache.github.io/spark-kubernetes-operator/) deployment on a [Kubernetes](http://kubernetes.io)
cluster using the [Helm](https://helm.sh) package manager. With this, you can launch Spark applications and clusters.

## Requirements

- Kubernetes 1.32+ cluster
- Helm 3.0+

## Features

- Support Apache Spark 3.5+
- Support `SparkApp` and `SparkCluster` CRDs
  - `sparkapplications.spark.apache.org` (v1)
  - `sparkclusters.spark.apache.org` (v1)
- Support HPA for SparkCluster
