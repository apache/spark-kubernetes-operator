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

# Configuration

## Configure Operator

Spark Operator supports different ways to configure the behavior:

* **spark-operator.properties** provided when deploying the operator. In addition to the
  [property file](../build-tools/helm/spark-kubernetes-operator/conf/spark-operator.
  properties), it is also possible to override or append config properties in helm [Values
  files](../build-tools/helm/spark-kubernetes-operator/values.yaml).
* **System Properties** : when provided as system properties (e.g. via -D options to the
  operator JVM), it overrides the values provided in property file.
* **Hot property loading** : when enabled, a 
  [configmap](https://kubernetes.io/docs/concepts/configuration/configmap/) would be created with 
  the operator in the same namespace. Operator can monitor updates performed on the configmap. Hot 
  properties reloading takes higher precedence comparing with default properties override.
    - An example use case: operator use hot properties to figure the list of namespace(s) to
      operate Spark applications. The hot properties config map can be updated and
      maintained by user or additional microservice to tune the operator behavior without
      rebooting it.
    - Please be advised that not all properties can be hot-loaded and honored at runtime.
      Refer the list of [supported properties](./config_properties.md) for more details.

To enable hot properties loading, update the **helm chart values file** with

```

operatorConfiguration:
  spark-operator.properties: |+
    spark.operator.dynamic.config.enabled=true
    # ... all other config overides...
  dynamicConfig:
    create: true

```

## Config Metrics Publishing Behavior

Spark Operator uses the same source & sink interface as Apache Spark. You may
use existing Spark metrics sink for both applications and the operator.
