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

# Operator Probes

In Kubernetes world, the kubelet uses readiness probes to know when a container is ready to
start accepting traffic, and it uses liveness probes to know when to restart a container. Here
for Spark Operators, we provided those as below by default. You can override the values in
values.yaml if you use Helm Chart to deploy the Spark Operator.

```
ports:
- containerPort: 18080
  name: probe-port
livenessProbe:
  httpGet:
    port: probe-port
    path: /healthz
  initialDelaySeconds: 30
  periodSeconds: 10
readinessProbe:
  httpGet:
    port: probe-port
    path: /startup
  failureThreshold: 30
  periodSeconds: 10
```

## Operator Readiness Probe

A readiness probe helps to determine whether current instances can serve the traffic.
Therefore, Spark Operator's readiness probe has to make sure both operator has started and also
need to verify the existence of required rbac access.

## Operator Health(Liveness) Probe

A built-in health endpoint that serves as the information source for Operator liveness. Since
Java Operator SDK provides [runtimeInfo](https://javaoperatorsdk.io/docs/features#runtime-info)
to check the actual health of event sources. Spark Operator' s healthProbe will check:

* operator runtimeInfo health state
* Sentinel resources health state

### Operator Sentinel Resource

Learning
from [Apache Flink Operator](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/operations/health/#canary-resources),
a dummy spark application resource in any watched namespace can help Spark operator health
probe monitor.

Here is a Spark Sentinel resource example with the label `"spark.operator/sentinel": "true"`
and it will not result in creation of any other kubernetes resources. Controlled by
property `health.sentinel.resource.reconciliation.delay.seconds`, by default, the timeout to
reconcile the sentinel resources is 60 seconds. If the operator cannot reconcile these
resources within limited time, the operator health probe will return HTTP code 500 when kubelet
send the HTTP Get to the liveness endpoint, and the
kubelet will then kill the spark operator container and restart it.

```yaml
apiVersion: org.apache.spark/v1alpha1
kind: SparkApplication
metadata:
  name: spark-sentinel-resources
  labels:
    "spark.operator/sentinel": "true"
```
