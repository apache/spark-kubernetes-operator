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

### Compatibility

- Java 17, 21 and 24
- Kubernetes version compatibility:
    + k8s version >= 1.30 is recommended. Operator attempts to be API compatible as possible, but
      patch support will not be performed on k8s versions that reached EOL.
- Spark versions 3.5 or above.

### Spark Application Namespaces

By default, Spark applications are created in the same namespace as the operator deployment.
You many also configure the chart deployment to add necessary RBAC resources for
applications to enable them running in additional namespaces.

## Overriding configuration parameters during Helm install

Helm provides different ways to override the default installation parameters (contained
in `values.yaml`) for the Helm chart.

To override single parameters you can use `--set`, for example:

```
helm install --set image.repository=<my_registory>/spark-kubernetes-operator \
   -f build-tools/helm/spark-kubernetes-operator/values.yaml \
  build-tools/helm/spark-kubernetes-operator/
```

You can also provide multiple custom values file by using the `-f` flag, the latest takes
higher precedence:

```
helm install spark-kubernetes-operator \
   -f build-tools/helm/spark-kubernetes-operator/values.yaml \
   -f my_values.yaml \
   build-tools/helm/spark-kubernetes-operator/
```

The configurable parameters of the Helm chart and which default values as detailed in the
following table:

| Parameters                                                    | Description                                                                                                                                                                    | Default value                                                                                           |
|---------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| image.repository                                              | The image repository of spark-kubernetes-operator.                                                                                                                             | spark-kubernetes-operator                                                                               |
| image.pullPolicy                                              | The image pull policy of spark-kubernetes-operator.                                                                                                                            | IfNotPresent                                                                                            |
| image.tag                                                     | The image tag of spark-kubernetes-operator.                                                                                                                                    | 0.1.0-SNAPSHOT                                                                                          |
| image.digest                                                  | The image digest of spark-kubernetes-operator. If set then it takes precedence and the image tag will be ignored.                                                              |                                                                                                         |
| imagePullSecrets                                              | The image pull secrets of spark-kubernetes-operator.                                                                                                                           |                                                                                                         |
| operatorDeployment.replica                                    | Operator replica count. Must be 1 unless leader election is configured.                                                                                                        | 1                                                                                                       |
| operatorDeployment.strategy.type                              | Operator pod upgrade strategy. Must be Recreate unless leader election is configured.                                                                                          | Recreate                                                                                                |
| operatorDeployment.operatorPod.annotations                    | Custom annotations to be added to the operator pod                                                                                                                             |                                                                                                         |
| operatorDeployment.operatorPod.labels                         | Custom labels to be added to the operator pod                                                                                                                                  |                                                                                                         |
| operatorDeployment.operatorPod.nodeSelector                   | Custom nodeSelector to be added to the operator pod.                                                                                                                           |                                                                                                         |
| operatorDeployment.operatorPod.topologySpreadConstraints      | Custom topologySpreadConstraints to be added to the operator pod.                                                                                                              |                                                                                                         |
| operatorDeployment.operatorPod.dnsConfig                      | DNS configuration to be used by the operator pod.                                                                                                                              |                                                                                                         |
| operatorDeployment.operatorPod.volumes                        | Additional volumes to be added to the operator pod.                                                                                                                            |                                                                                                         |
| operatorDeployment.operatorPod.priorityClassName              | Priority class name to be used for the operator pod                                                                                                                            |                                                                                                         |
| operatorDeployment.operatorPod.securityContext                | Security context overrides for the operator pod                                                                                                                                |                                                                                                         |
| operatorDeployment.operatorContainer.jvmArgs                  | JVM arg override for the operator container.                                                                                                                                   | `"-Dfile.encoding=UTF8"`                                                                                |
| operatorDeployment.operatorContainer.env                      | Custom env to be added to the operator container.                                                                                                                              |                                                                                                         |
| operatorDeployment.operatorContainer.envFrom                  | Custom envFrom to be added to the operator container, e.g. for downward API.                                                                                                   |                                                                                                         |
| operatorDeployment.operatorContainer.probes                   | Probe config for the operator container.                                                                                                                                       |                                                                                                         |
| operatorDeployment.operatorContainer.securityContext          | Security context overrides for the operator container.                                                                                                                         | run as non root for baseline secuirty standard compliance                                               |
| operatorDeployment.operatorContainer.resources                | Resources for the operator container.                                                                                                                                          | memory 4Gi, ephemeral storage 2Gi and 1 cpu                                                             |
| operatorDeployment.additionalContainers                       | Additional containers to be added to the operator pod, e.g. sidecar.                                                                                                           |                                                                                                         |
| operatorRbac.serviceAccount.create                            | Whether to create service account for operator to use.                                                                                                                         | true                                                                                                    |
| operatorRbac.serviceAccount.name                              | Name of the operator Role.                                                                                                                                                     | `"spark-operator"`                                                                                      |
| operatorRbac.clusterRole.create                               | Whether to create ClusterRole for operator to use.                                                                                                                             | true                                                                                                    |
| operatorRbac.clusterRole.name                                 | Name of the operator ClusterRole.                                                                                                                                              | `"spark-operator-clusterrole"`                                                                          |
| operatorRbac.clusterRoleBinding.create                        | Whether to create ClusterRoleBinding for operator to use.                                                                                                                      | true                                                                                                    |
| operatorRbac.clusterRoleBinding.name                          | Name of the operator ClusterRoleBinding.                                                                                                                                       | `"spark-operator-clusterrolebinding"`                                                                   |
| operatorRbac.role.create                                      | Whether to create Role for operator to use in each workload namespace(s). At least one of `clusterRole.create` or `role.create` should be enabled                              | false                                                                                                   |
| operatorRbac.role.name                                        | Name of the operator Role                                                                                                                                                      | `"spark-operator-role"`                                                                                 |
| operatorRbac.roleBinding.create                               | Whether to create RoleBinding for operator to use. At least one of `clusterRoleBinding.create` or `roleBinding.create` should be enabled                                       | false                                                                                                   |
| operatorRbac.roleBinding.name                                 | Name of the operator RoleBinding in each workload namespace(s).                                                                                                                | `"spark-operator-rolebinding"`                                                                          |
| operatorRbac.roleBinding.roleRef                              | RoleRef for the created Operator RoleBinding. Override this when you want the created RoleBinding refer to ClusterRole / Role that's different from the default operator Role. | Refers to default `operatorRbac.role.name`                                                              |
| operatorRbac.configManagement.create                          | Enable this to create a Role for operator configuration management (hot property loading and leader election).                                                                 | true                                                                                                    |
| operatorRbac.configManagement.roleName                        | Role name for operator configuration management.                                                                                                                               | `spark-operator-config-role`                                                                            |
| operatorRbac.configManagement.roleBinding                     | RoleBinding name for operator configuration management.                                                                                                                        | `"spark-operator-config-monitor-role-binding"`                                                          |
| operatorRbac.labels                                           | Labels to be applied on all created `operatorRbac` resources.                                                                                                                  | `"app.kubernetes.io/component": "operator-rbac"`                                                        |
| workloadResources.namespaces.create                           | Whether to create dedicated namespaces for Spark workload.                                                                                                                     | true                                                                                                    |
| workloadResources.namespaces.overrideWatchedNamespaces        | When enabled, operator would by default only watch namespace(s) provided in data field.                                                                                        | true                                                                                                    |
| workloadResources.namespaces.data                             | List of namespaces to create for Spark workload. The chart namespace would be used if this is empty.                                                                           |                                                                                                         |
| workloadResources.clusterRole.create                          | When enabled, a ClusterRole would be created for Spark workload to use.                                                                                                        | true                                                                                                    |
| workloadResources.clusterRole.name                            | Name of the Spark workload ClusterRole.                                                                                                                                        | "spark-workload-clusterrole"                                                                            |
| workloadResources.role.create                                 | When enabled, a Role would be created in each namespace for Spark workload. At least one of `clusterRole.create` or `role.create` should be enabled.                           | false                                                                                                   |
| workloadResources.role.name                                   | Name for Spark workload Role.                                                                                                                                                  | "spark-workload-role"                                                                                   |
| workloadResources.roleBinding.create                          | When enabled, a RoleBinding would be created in each namespace for Spark workload. This shall be enabled unless access is configured from 3rd party.                           | true                                                                                                    |
| workloadResources.roleBinding.name                            | Name of the Spark workload RoleBinding.                                                                                                                                        | "spark-workload-rolebinding"                                                                            |
| workloadResources.serviceAccounts.create                      | Whether to create a service account for Spark workload.                                                                                                                        | true                                                                                                    |
| workloadResources.serviceAccounts.name                        | The name of Spark workload service account.                                                                                                                                    | `spark`                                                                                                 |
| workloadResources.labels                                      | Labels to be applied for all workload resources.                                                                                                                               | `"app.kubernetes.io/component": "spark-workload"`                                                       |
| workloadResources.annotations                                 | Annotations to be applied for all workload resources.                                                                                                                          | `"helm.sh/resource-policy": keep`                                                                       |
| workloadResources.sparkApplicationSentinel.create             | If enabled, sentinel resources will be created for operator to watch and reconcile for the health probe purpose.                                                               | false                                                                                                   |
| workloadResources.sparkApplicationSentinel.sentinelNamespaces | A list of namespaces where sentinel resources will be created in. Note that these namespaces have to be a subset of `workloadResources.namespaces.data`.                       |                                                                                                         |
| operatorConfiguration.append                                  | If set to true, below conf file & properties would be appended to default conf. Otherwise, they would override default properties.                                             | true                                                                                                    |
| operatorConfiguration.log4j2.properties                       | The default log4j2 configuration.                                                                                                                                              | Refer default [log4j2.properties](../build-tools/helm/spark-kubernetes-operator/conf/log4j2.properties) |
| operatorConfiguration.spark-operator.properties               | The default operator configuration.                                                                                                                                            |                                                                                                         |
| operatorConfiguration.metrics.properties                      | The default operator metrics (sink) configuration.                                                                                                                             |                                                                                                         |
| operatorConfiguration.dynamicConfig.create                    | If set to true, a config map would be created & watched by operator as source of truth for hot properties loading.                                                             | false                                                                                                   |
| operatorConfiguration.dynamicConfig.enable                    | If set to true, operator would honor the created config mapas source of truth for hot properties loading.                                                                      | false                                                                                                   |
| operatorConfiguration.dynamicConfig.annotations               | Annotations to be applied for the dynamicConfig resources.                                                                                                                     | `"helm.sh/resource-policy": keep`                                                                       |
| operatorConfiguration.dynamicConfig.data                      | Data field (key-value pairs) that acts as hot properties in the config map.                                                                                                    | `spark.kubernetes.operator.reconciler.intervalSeconds: "60"`                                            |

For more information check the [Helm documentation](https://helm.sh/docs/helm/helm_install/).

__Notice__: The pod resources should be set as your workload in different environments to
archive a matched K8s pod QoS. See
also [Pod Quality of Service Classes](https://kubernetes.io/docs/concepts/workloads/pods/pod-qos/#quality-of-service-classes).

## Operator Health(Liveness) Probe with Sentinel Resource

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
