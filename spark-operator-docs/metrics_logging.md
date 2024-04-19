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

# Metrics

Spark operator,
following [Apache Spark](https://spark.apache.org/docs/latest/monitoring.html#metrics),
has a configurable metrics system based on
the [Dropwizard Metrics Library](https://metrics.dropwizard.io/4.2.25/). Note that Spark Operator 
does not have Spark UI, MetricsServlet 
and PrometheusServlet from org.apache.spark.metrics.sink package are not supported. If you are 
interested in Prometheus metrics exporting, please take a look at below section `Forward Metrics to Prometheus`

## JVM Metrics

Spark Operator collects JVM metrics
via [Codahale JVM Metrics](https://javadoc.io/doc/com.codahale.metrics/metrics-jvm/latest/index.html)

- BufferPoolMetricSet
- FileDescriptorRatioGauge
- GarbageCollectorMetricSet
- MemoryUsageGaugeSet
- ThreadStatesGaugeSet

## Kubernetes Client Metrics

| Metrics Name                                              | Type       | Description                                                                                                              |
|-----------------------------------------------------------|------------|--------------------------------------------------------------------------------------------------------------------------|
| kubernetes.client.http.request                            | Meter      | Tracking the rates of HTTP request sent to the Kubernetes API Server                                                     |
| kubernetes.client.http.response                           | Meter      | Tracking the rates of HTTP response from the Kubernetes API Server                                                       |
| kubernetes.client.http.response.failed                    | Meter      | Tracking the rates of HTTP requests which have no response from the Kubernetes API Server                                |
| kubernetes.client.http.response.latency.nanos             | Histograms | Measures the statistical distribution of HTTP response latency from the Kubernetes API Server                            |
| kubernetes.client.http.response.<ResponseCode>            | Meter      | Tracking the rates of HTTP response based on response code from the Kubernetes API Server                                |
| kubernetes.client.http.request.<RequestMethod>            | Meter      | Tracking the rates of HTTP request based type of method to the Kubernetes API Server                                     |
| kubernetes.client.http.response.1xx                       | Meter      | Tracking the rates of HTTP Code 1xx responses (informational) received from the Kubernetes API Server per response code. |
| kubernetes.client.http.response.2xx                       | Meter      | Tracking the rates of HTTP Code 2xx responses (success) received from the Kubernetes API Server per response code.       |
| kubernetes.client.http.response.3xx                       | Meter      | Tracking the rates of HTTP Code 3xx responses (redirection) received from the Kubernetes API Server per response code.   |
| kubernetes.client.http.response.4xx                       | Meter      | Tracking the rates of HTTP Code 4xx responses (client error) received from the Kubernetes API Server per response code.  |
| kubernetes.client.http.response.5xx                       | Meter      | Tracking the rates of HTTP Code 5xx responses (server error) received from the Kubernetes API Server per response code.  |
| kubernetes.client.<ResourceName>.<Method>                 | Meter      | Tracking the rates of HTTP request for a combination of one Kubernetes resource and one http method                      |
| kubernetes.client.<NamespaceName>.<ResourceName>.<Method> | Meter      | Tracking the rates of HTTP request for a combination of one namespace-scoped Kubernetes resource and one http method     |

## Forward Metrics to Prometheus

In this section, we will show you how to forward spark operator metrics
to [Prometheus](https://prometheus.io).

* Modify the
  build-tools/helm/spark-kubernetes-operator/values.yaml file' s metrics properties section:

```properties
metrics.properties:|+
  spark.metrics.conf.operator.sink.prometheus.class=org.apache.spark.kubernetes.operator.metrics.
sink.PrometheusPullModelSink
```

* Install the Spark Operator

```bash
helm install spark-kubernetes-operator -f build-tools/helm/spark-kubernetes-operator/values.yaml build-tools/helm/spark-kubernetes-operator/
```

* Install Prometheus via Helm Chart

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install prometheus prometheus-community/prometheus
```

* Find and Annotate Spark Operator Pods

```bash
kubectl get pods -l app.kubernetes.io/name=spark-kubernetes-operator
NAME                                         READY   STATUS    RESTARTS   AGE
spark-kubernetes-operator-598cb5d569-bvvd2   1/1     Running   0          24m

kubectl annotate pods spark-kubernetes-operator-598cb5d569-bvvd2 prometheus.io/scrape=true
kubectl annotate pods spark-kubernetes-operator-598cb5d569-bvvd2 prometheus.io/path=/prometheus
kubectl annotate pods spark-kubernetes-operator-598cb5d569-bvvd2 prometheus.io/port=19090
```

* Check Metrics via Prometheus UI

```bash
kubectl get pods | grep "prometheus-server"                         
prometheus-server-654bc74fc9-8hgkb                   2/2     Running   0          59m

kubectl port-forward --address 0.0.0.0 pod/prometheus-server-654bc74fc9-8hgkb  8080:9090
```

open your browser with address `localhost:8080`. Click on Status Targets tab, you should be able
to find target as below.
[<img src="resources/prometheus.png">](resources/prometheus.png)
