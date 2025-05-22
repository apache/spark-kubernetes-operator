/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.k8s.operator;

import static org.apache.spark.k8s.operator.Constants.*;

import java.util.Collections;
import java.util.Optional;

import scala.Tuple2;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServiceSpec;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetSpec;
import io.fabric8.kubernetes.api.model.autoscaling.v2.HorizontalPodAutoscaler;
import io.fabric8.kubernetes.api.model.autoscaling.v2.HorizontalPodAutoscalerBuilder;
import io.fabric8.kubernetes.api.model.autoscaling.v2.HorizontalPodAutoscalerSpec;
import io.fabric8.kubernetes.api.model.autoscaling.v2.HorizontalPodAutoscalerSpecBuilder;
import io.fabric8.kubernetes.api.model.autoscaling.v2.MetricSpecBuilder;
import lombok.Getter;

import org.apache.spark.SparkConf;
import org.apache.spark.deploy.k8s.Config;
import org.apache.spark.k8s.operator.spec.ClusterSpec;
import org.apache.spark.k8s.operator.spec.MasterSpec;
import org.apache.spark.k8s.operator.spec.WorkerSpec;

/** Spark Cluster Resource Spec: Master Service, Master StatefulSet, Worker StatefulSet */
public class SparkClusterResourceSpec {
  @Getter private final Service masterService;
  @Getter private final Service workerService;
  @Getter private final StatefulSet masterStatefulSet;
  @Getter private final StatefulSet workerStatefulSet;
  @Getter private final Optional<HorizontalPodAutoscaler> horizontalPodAutoscaler;

  public SparkClusterResourceSpec(SparkCluster cluster, SparkConf conf) {
    String clusterNamespace = cluster.getMetadata().getNamespace();
    String clusterName = cluster.getMetadata().getName();
    String scheduler = conf.get(Config.KUBERNETES_SCHEDULER_NAME().key(), "default-scheduler");
    String namespace = conf.get(Config.KUBERNETES_NAMESPACE().key(), clusterNamespace);
    String image = conf.get(Config.CONTAINER_IMAGE().key(), "apache/spark:4.0.0");
    ClusterSpec spec = cluster.getSpec();
    String version = spec.getRuntimeVersions().getSparkVersion();
    StringBuilder options = new StringBuilder();
    for (Tuple2<String, String> t : conf.getAll()) {
      options.append(String.format("-D%s=\"%s\" ", t._1, t._2));
    }
    MasterSpec masterSpec = spec.getMasterSpec();
    WorkerSpec workerSpec = spec.getWorkerSpec();
    masterService =
        buildMasterService(
            clusterName,
            namespace,
            version,
            masterSpec.getServiceMetadata(),
            masterSpec.getServiceSpec());
    workerService =
        buildWorkerService(
            clusterName,
            namespace,
            version,
            workerSpec.getServiceMetadata(),
            workerSpec.getServiceSpec());
    masterStatefulSet =
        buildMasterStatefulSet(
            scheduler,
            clusterName,
            namespace,
            version,
            image,
            options.toString(),
            masterSpec.getStatefulSetMetadata(),
            masterSpec.getStatefulSetSpec());
    workerStatefulSet =
        buildWorkerStatefulSet(
            scheduler,
            clusterName,
            namespace,
            version,
            image,
            spec.getClusterTolerations().getInstanceConfig().getInitWorkers(),
            options.toString(),
            workerSpec.getStatefulSetMetadata(),
            workerSpec.getStatefulSetSpec());
    horizontalPodAutoscaler = buildHorizontalPodAutoscaler(clusterName, namespace, spec);
  }

  private static Service buildMasterService(
      String name, String namespace, String version, ObjectMeta metadata, ServiceSpec serviceSpec) {
    return new ServiceBuilder()
        .withNewMetadataLike(metadata)
        .withName(name + "-master-svc")
        .addToLabels(LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_MASTER_VALUE)
        .addToLabels(LABEL_SPARK_VERSION_NAME, version)
        .withNamespace(namespace)
        .endMetadata()
        .withNewSpecLike(serviceSpec)
        .withClusterIP("None")
        .withSelector(
            Collections.singletonMap(LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_MASTER_VALUE))
        .addNewPort()
        .withName("web")
        .withPort(8080)
        .withNewTargetPort("web")
        .endPort()
        .addNewPort()
        .withName("spark")
        .withPort(7077)
        .withNewTargetPort("spark")
        .endPort()
        .addNewPort()
        .withName("rest")
        .withPort(6066)
        .withNewTargetPort("rest")
        .endPort()
        .endSpec()
        .build();
  }

  private static Service buildWorkerService(
      String name, String namespace, String version, ObjectMeta metadata, ServiceSpec serviceSpec) {
    return new ServiceBuilder()
        .withNewMetadataLike(metadata)
        .withName(name + "-worker-svc")
        .addToLabels(LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_WORKER_VALUE)
        .addToLabels(LABEL_SPARK_VERSION_NAME, version)
        .withNamespace(namespace)
        .endMetadata()
        .withNewSpecLike(serviceSpec)
        .withClusterIP("None")
        .withSelector(
            Collections.singletonMap(LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_WORKER_VALUE))
        .addNewPort()
        .withName("web")
        .withPort(8081)
        .withNewTargetPort("web")
        .endPort()
        .endSpec()
        .build();
  }

  private static StatefulSet buildMasterStatefulSet(
      String scheduler,
      String name,
      String namespace,
      String version,
      String image,
      String options,
      ObjectMeta objectMeta,
      StatefulSetSpec statefulSetSpec) {
    var partialStatefulSet =
        new StatefulSetBuilder()
            .withNewMetadataLike(objectMeta)
            .withName(name + "-master")
            .addToLabels(LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_MASTER_VALUE)
            .addToLabels(LABEL_SPARK_VERSION_NAME, version)
            .withNamespace(namespace)
            .endMetadata()
            .withNewSpecLike(statefulSetSpec)
            .withPodManagementPolicy("Parallel")
            .withReplicas(1)
            .editOrNewSelector()
            .addToMatchLabels(LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_MASTER_VALUE)
            .endSelector()
            .editOrNewTemplate()
            .editOrNewMetadata()
            .addToLabels(LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_MASTER_VALUE)
            .addToLabels(LABEL_SPARK_VERSION_NAME, version)
            .endMetadata()
            .editOrNewSpec()
            .withSchedulerName(scheduler)
            .withTerminationGracePeriodSeconds(0L);
    if (!partialStatefulSet.hasMatchingContainer(p -> "master".equals(p.getName()))) {
      partialStatefulSet = partialStatefulSet.addNewContainer().withName("master").endContainer();
    }
    return partialStatefulSet
        .editMatchingContainer(p -> "master".equals(p.getName()))
        .withImage(image)
        .addNewEnv()
        .withName("SPARK_MASTER_OPTS")
        .withValue(options)
        .endEnv()
        .addToCommand("bash")
        .addToArgs(
            "-c",
            "/opt/spark/sbin/start-master.sh && while /opt/spark/sbin/spark-daemon.sh status "
                + "org.apache.spark.deploy.master.Master 1; do sleep 1; done")
        .addNewPort()
        .withName("web")
        .withContainerPort(8080)
        .endPort()
        .addNewPort()
        .withName("spark")
        .withContainerPort(7070)
        .endPort()
        .addNewPort()
        .withName("rest")
        .withContainerPort(6066)
        .endPort()
        .endContainer()
        .endSpec()
        .endTemplate()
        .endSpec()
        .build();
  }

  private static StatefulSet buildWorkerStatefulSet(
      String scheduler,
      String name,
      String namespace,
      String version,
      String image,
      int initWorkers,
      String options,
      ObjectMeta metadata,
      StatefulSetSpec statefulSetSpec) {
    var partialStatefulSet =
        new StatefulSetBuilder()
            .withNewMetadataLike(metadata)
            .withName(name + "-worker")
            .addToLabels(LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_WORKER_VALUE)
            .addToLabels(LABEL_SPARK_VERSION_NAME, version)
            .withNamespace(namespace)
            .endMetadata()
            .withNewSpecLike(statefulSetSpec)
            .withPodManagementPolicy("Parallel")
            .withReplicas(initWorkers)
            .withServiceName(name + "-worker-svc")
            .editOrNewSelector()
            .addToMatchLabels(LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_WORKER_VALUE)
            .endSelector()
            .editOrNewTemplate()
            .editOrNewMetadata()
            .addToLabels(LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_WORKER_VALUE)
            .addToLabels(LABEL_SPARK_VERSION_NAME, version)
            .endMetadata()
            .editOrNewSpec()
            .withSchedulerName(scheduler)
            .withTerminationGracePeriodSeconds(0L)
            .withNewDnsConfig()
            .withSearches(String.format("%s-worker-svc.%s.svc.cluster.local", name, namespace))
            .endDnsConfig();
    if (!partialStatefulSet.hasMatchingContainer(p -> "worker".equals(p.getName()))) {
      partialStatefulSet = partialStatefulSet.addNewContainer().withName("worker").endContainer();
    }
    return partialStatefulSet
        .editMatchingContainer(p -> "worker".equals(p.getName()))
        .withImage(image)
        .addNewEnv()
        .withName("SPARK_LOG_DIR")
        .withValue("/opt/spark/work/logs")
        .endEnv()
        .addNewEnv()
        .withName("SPARK_WORKER_OPTS")
        .withValue(options)
        .endEnv()
        .addToCommand("bash")
        .addToArgs(
            "-c",
            "/opt/spark/sbin/start-worker.sh spark://"
                + name
                + "-master-svc:7077 && while /opt/spark/sbin/spark-daemon.sh status "
                + "org.apache.spark.deploy.worker.Worker 1; do sleep 1; done")
        .addNewPort()
        .withName("web")
        .withContainerPort(8081)
        .endPort()
        .endContainer()
        .endSpec()
        .endTemplate()
        .endSpec()
        .build();
  }

  private static Optional<HorizontalPodAutoscaler> buildHorizontalPodAutoscaler(
      String clusterName, String namespace, ClusterSpec spec) {
    var instanceConfig = spec.getClusterTolerations().getInstanceConfig();
    if (instanceConfig.getMinWorkers() >= instanceConfig.getMaxWorkers()) {
      return Optional.empty();
    }
    HorizontalPodAutoscalerSpec horizontalPodAutoscalerSpec;
    if (spec.getWorkerSpec().getHorizontalPodAutoscalerSpec() != null) {
      horizontalPodAutoscalerSpec = spec.getWorkerSpec().getHorizontalPodAutoscalerSpec();
    } else {
      horizontalPodAutoscalerSpec =
          new HorizontalPodAutoscalerSpecBuilder()
              .addToMetrics(
                  new MetricSpecBuilder()
                      .withType("ContainerResource")
                      .withNewContainerResource()
                      .withName("cpu")
                      .withContainer("worker")
                      .withNewTarget()
                      .withType("Utilization")
                      .withAverageUtilization(30)
                      .endTarget()
                      .endContainerResource()
                      .build())
              .withNewBehavior()
              .withNewScaleUp()
              .addNewPolicy()
              .withType("Pods")
              .withValue(1)
              .withPeriodSeconds(60)
              .endPolicy()
              .endScaleUp()
              .withNewScaleDown()
              .addNewPolicy()
              .withType("Pods")
              .withValue(1)
              .withPeriodSeconds(600)
              .endPolicy()
              .endScaleDown()
              .endBehavior()
              .build();
    }
    return Optional.of(
        new HorizontalPodAutoscalerBuilder()
            .withNewMetadata()
            .withNamespace(namespace)
            .withName(clusterName + "-worker-hpa")
            .addToLabels(LABEL_SPARK_VERSION_NAME, spec.getRuntimeVersions().getSparkVersion())
            .endMetadata()
            .withNewSpecLike(horizontalPodAutoscalerSpec)
            .withNewScaleTargetRef()
            .withApiVersion("apps/v1")
            .withKind("StatefulSet")
            .withName(clusterName + "-worker")
            .endScaleTargetRef()
            .withMinReplicas(instanceConfig.getMinWorkers())
            .withMaxReplicas(instanceConfig.getMaxWorkers())
            .endSpec()
            .build());
  }
}
