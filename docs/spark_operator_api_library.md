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

# Use Spark Operator API Library in Your Java Application

In addition to direct YAML, you may also leverage our Java API library to create and manage Spark
workloads.

## Choosing the Right Dependency

We publish two variants of this library:

```groovy
implementation("org.apache.spark:spark-operator-api")
```

This library keeps your project free from a direct dependency on Fabric8 Kubernetes Client. It's
recommended if you only interact with our API at the YAML/CRD level (e.g. generating manifests,
using our higher-level abstractions). Use this if you don’t need direct access to Fabric8 model
objects like PodTemplateSpec, Deployment, etc.

If your application requires access on fabric8 models, it's recommended to use

```groovy
implementation("org.apache.spark:spark-operator-api-fabric8")
```

This library publishes the same core library, with Fabric8 declared as an API dependency. This
means Fabric8 model classes (Pod, PodTemplateSpec, Deployment, etc.) are available directly on
your classpath when you depend on our library. It's recommended if you want to compose or extend
Spark Operator API using Fabric8 types (for example, embedding a Fabric8 PodTemplateSpec into
Spark Operator CRD).

Do not depend on both `spark-operator-api` and `spark-operator-api-fabric8` at the same time. 
Choose one based on your use case.
