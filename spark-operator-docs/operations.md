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

## Manage Your Spark Operator

The operator installation is managed by a helm chart. To install run:

```
helm install spark-kubernetes-operator \
  -f build-tools/helm/spark-kubernetes-operator/values.yaml \
  build-tools/helm/spark-kubernetes-operator/
```

Alternatively to install the operator (and also the helm chart) to a specific namespace:

```
helm install spark-kubernetes-operator \
  -f build-tools/helm/spark-kubernetes-operator/values.yaml \
   build-tools/helm/spark-kubernetes-operator/ \
   --namespace spark-system --create-namespace
```

Note that in this case you will need to update the namespace in the examples accordingly.

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

| Parameters                                               | Description                                                                                                                                              | Default value                                                                                           |
|----------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|
| image.repository                                         | The image repository of spark-kubernetes-operator.                                                                                                       | spark-kubernetes-operator                                                                               |
| image.pullPolicy                                         | The image pull policy of spark-kubernetes-operator.                                                                                                      | IfNotPresent                                                                                            |
| image.tag                                                | The image tag of spark-kubernetes-operator.                                                                                                              |                                                                                                         |
| image.digest                                             | The image digest of spark-kubernetes-operator. If set then it takes precedence and the image tag will be ignored.                                        |                                                                                                         |
| imagePullSecrets                                         | The image pull secrets of spark-kubernetes-operator.                                                                                                     |                                                                                                         |
| operatorDeployment.replica                               | Operator replica count. Must be 1 unless leader election is configured.                                                                                  | 1                                                                                                       |
| operatorDeployment.strategy.type                         | Operator pod upgrade strategy. Must be Recreate unless leader election is configured.                                                                    | Recreate                                                                                                |
| operatorDeployment.operatorPod.annotations               | Custom annotations to be added to the operator pod                                                                                                       |                                                                                                         |
| operatorDeployment.operatorPod.labels                    | Custom labels to be added to the operator pod                                                                                                            |                                                                                                         |
| operatorDeployment.operatorPod.nodeSelector              | Custom nodeSelector to be added to the operator pod.                                                                                                     |                                                                                                         |
| operatorDeployment.operatorPod.topologySpreadConstraints | Custom topologySpreadConstraints to be added to the operator pod.                                                                                        |                                                                                                         |
| operatorDeployment.operatorPod.dnsConfig                 | DNS configuration to be used by the operator pod.                                                                                                        |                                                                                                         |
| operatorDeployment.operatorPod.volumes                   | Additional volumes to be added to the operator pod.                                                                                                      |                                                                                                         |
| operatorDeployment.operatorPod.priorityClassName         | Priority class name to be used for the operator pod                                                                                                      |                                                                                                         |
| operatorDeployment.operatorPod.securityContext           | Security context overrides for the operator pod                                                                                                          |                                                                                                         |
| operatorDeployment.operatorContainer.jvmArgs             | JVM arg override for the operator container.                                                                                                             | `-XX:+UseG1GC -Xms3G -Xmx3G -Dfile.encoding=UTF8`                                                       |
| operatorDeployment.operatorContainer.env                 | Custom env to be added to the operator container.                                                                                                        |                                                                                                         |
| operatorDeployment.operatorContainer.envFrom             | Custom envFrom to be added to the operator container, e.g. for downward API.                                                                             |                                                                                                         |
| operatorDeployment.operatorContainer.probes              | Probe config for the operator container.                                                                                                                 |                                                                                                         |
| operatorDeployment.operatorContainer.securityContext     | Security context overrides for the operator container.                                                                                                   | run as non root for baseline secuirty standard compliance                                               |
| operatorDeployment.operatorContainer.resources           | Resources for the operator container.                                                                                                                    | memory 4Gi, ephemeral storage 2Gi and 1 cpu                                                             |  
| operatorDeployment.additionalContainers                  | Additional containers to be added to the operator pod, e.g. sidecar.                                                                                     |                                                                                                         |
| operatorRbac.serviceAccount.create                       | Whether to create service account for operator to use.                                                                                                   |                                                                                                         |
| operatorRbac.clusterRole.create                          | Whether to create ClusterRole for operator to use. If disabled, a role would be created in operator & app namespaces                                     | true                                                                                                    |
| operatorRbac.clusterRoleBinding.create                   | Whether to create ClusterRoleBinding for operator to use. If disabled, a rolebinding would be created in operator & app namespaces                       | true                                                                                                    |
| operatorRbac.clusterRole.configManagement.roleName       | Role name for operator configuration management (hot property loading and leader election)                                                               | `spark-operator-config-role`                                                                            |
| appResources.namespaces.create                           | Whether to create dedicated namespaces for Spark apps.                                                                                                   | `spark-operator-config-role-binding`                                                                    |
| appResources.namespaces.watchGivenNamespacesOnly         | When enabled, operator would by default only watch namespace(s) provided in data field.                                                                  | false                                                                                                   |
| appResources.namespaces.data                             | list of namespaces to create for apps                                                                                                                    |                                                                                                         |
| appResources.clusterRole.create                          | Enable a ClusterRole to be created for apps. If neither role nor clusterrole is enabled: Spark app would use the same access as operator.                | false                                                                                                   |
| appResources.role.create                                 | Enable a Role to be created in each app namespace for apps. If neither role nor clusterrole is enabled: Spark app would use the same access as operator. | false                                                                                                   |
| appResources.serviceAccounts.create                      | Whether to create a service account for apps                                                                                                             | true                                                                                                    | 
| appResources.serviceAccounts.name                        | The name of Spark app service account                                                                                                                    | `spark`                                                                                                 | 
| appResources.labels                                      | Labels to be applied for all app resources                                                                                                               | `"app.kubernetes.io/component": "spark-apps"`                                                           |
| appResources.annotations                                 | Annotations to be applied for all app resources                                                                                                          |                                                                                                         |
| appResources.sparkApplicationSentinel.create             | If enabled, sentinel resources will be created for operator to watch and reconcile for the health probe purpose.                                         | false                                                                                                   |
| appResources.sparkApplicationSentinel.sentinelNamespaces | A list of namespaces where sentinel resources will be created in. Note that these namespaces have to be a subset of appResources.namespaces.data         |                                                                                                         |
| operatorConfiguration.append                             | If set to true, below conf file & properties would be appended to default conf. Otherwise, they would override default properties                        | true                                                                                                    |
| operatorConfiguration.log4j2.properties                  | The default log4j2 configuration                                                                                                                         | Refer default [log4j2.properties](../build-tools/helm/spark-kubernetes-operator/conf/log4j2.properties) |
| operatorConfiguration.spark-operator.properties          | The default operator configuration                                                                                                                       |                                                                                                         |
| operatorConfiguration.metrics.properties                 | The default operator metrics (sink) configuration                                                                                                        |                                                                                                         |
| operatorConfiguration.dynamicConfig.create               | If set to true, a config map would be created & watched by operator as source of truth for hot properties loading.                                       | false                                                                                                   |
| operatorConfiguration.dynamicConfig.name                 | Name of the dynamic config map for hot property loading.                                                                                                 | spark-kubernetes-operator-dynamic-configuration                                                         |

For more information check the [Helm documentation](https://helm.sh/docs/helm/helm_install/).

__Notice__: The pod resources should be set as your workload in different environments to
archive a matched K8s pod QoS. See
also [Pod Quality of Service Classes](https://kubernetes.io/docs/concepts/workloads/pods/pod-qos/#quality-of-service-classes).

