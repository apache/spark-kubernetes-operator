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

import scala.Tuple2;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import lombok.Getter;

import org.apache.spark.SparkConf;
import org.apache.spark.deploy.k8s.Config;
import org.apache.spark.k8s.operator.spec.ClusterSpec;

/** Spark Cluster Resource Spec: Master Service, Master StatefulSet, Worker StatefulSet */
public class SparkClusterResourceSpec {
  @Getter private final Service masterService;
  @Getter private final Service workerService;
  @Getter private final StatefulSet masterStatefulSet;
  @Getter private final StatefulSet workerStatefulSet;

  public SparkClusterResourceSpec(SparkCluster cluster, SparkConf conf) {
    String clusterNamespace = cluster.getMetadata().getNamespace();
    String clusterName = cluster.getMetadata().getName();
    String scheduler = conf.get(Config.KUBERNETES_SCHEDULER_NAME().key(), "default-scheduler");
    String namespace = conf.get(Config.KUBERNETES_NAMESPACE().key(), clusterNamespace);
    String image = conf.get(Config.CONTAINER_IMAGE().key(), "spark:4.0.0-preview1");
    ClusterSpec spec = cluster.getSpec();
    StringBuilder options = new StringBuilder();
    for (Tuple2<String, String> t : conf.getAll()) {
      options.append(String.format("-D%s=\"%s\" ", t._1, t._2));
    }
    masterService = buildMasterService(clusterName, namespace);
    workerService = buildWorkerService(clusterName, namespace);
    masterStatefulSet =
        buildMasterStatefulSet(scheduler, clusterName, namespace, image, options.toString());
    workerStatefulSet =
        buildWorkerStatefulSet(
            scheduler, clusterName, namespace, image, spec.getInitWorkers(), options.toString());
  }

  private static Service buildMasterService(String name, String namespace) {
    return new ServiceBuilder()
        .withNewMetadata()
        .withName(name + "-master-svc")
        .addToLabels(LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_MASTER_VALUE)
        .withNamespace(namespace)
        .endMetadata()
        .withNewSpec()
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

  private static Service buildWorkerService(String name, String namespace) {
    return new ServiceBuilder()
        .withNewMetadata()
        .withName(name + "-worker-svc")
        .addToLabels(LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_WORKER_VALUE)
        .withNamespace(namespace)
        .endMetadata()
        .withNewSpec()
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
      String scheduler, String name, String namespace, String image, String options) {
    return new StatefulSetBuilder()
        .withNewMetadata()
        .withName(name + "-master")
        .addToLabels(LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_MASTER_VALUE)
        .withNamespace(namespace)
        .endMetadata()
        .withNewSpec()
        .withPodManagementPolicy("Parallel")
        .withReplicas(1)
        .withNewSelector()
        .addToMatchLabels(LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_MASTER_VALUE)
        .endSelector()
        .withNewTemplate()
        .withNewMetadata()
        .addToLabels(LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_MASTER_VALUE)
        .endMetadata()
        .withNewSpec()
        .withSchedulerName(scheduler)
        .withTerminationGracePeriodSeconds(0L)
        .addNewContainer()
        .withName("master")
        .withImage(image)
        .addNewEnv()
        .withName("SPARK_NO_DAEMONIZE")
        .withValue("1")
        .endEnv()
        .addNewEnv()
        .withName("SPARK_MASTER_OPTS")
        .withValue(options)
        .endEnv()
        .addToCommand("bash")
        .addToArgs("/opt/spark/sbin/start-master.sh")
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
      String image,
      int initWorkers,
      String options) {
    return new StatefulSetBuilder()
        .withNewMetadata()
        .withName(name + "-worker")
        .addToLabels(LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_WORKER_VALUE)
        .withNamespace(namespace)
        .endMetadata()
        .withNewSpec()
        .withPodManagementPolicy("Parallel")
        .withReplicas(initWorkers)
        .withServiceName(name + "-worker-svc")
        .withNewSelector()
        .addToMatchLabels(LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_WORKER_VALUE)
        .endSelector()
        .withNewTemplate()
        .withNewMetadata()
        .addToLabels(LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_WORKER_VALUE)
        .endMetadata()
        .withNewSpec()
        .withSchedulerName(scheduler)
        .withTerminationGracePeriodSeconds(0L)
        .withNewDnsConfig()
        .withSearches(String.format("%s-worker-svc.%s.svc.cluster.local", name, namespace))
        .endDnsConfig()
        .addNewContainer()
        .withName("worker")
        .withImage(image)
        .addNewEnv()
        .withName("SPARK_NO_DAEMONIZE")
        .withValue("1")
        .endEnv()
        .addNewEnv()
        .withName("SPARK_WORKER_OPTS")
        .withValue(options)
        .endEnv()
        .addToCommand("bash")
        .addToArgs("/opt/spark/sbin/start-worker.sh", "spark://" + name + "-master-svc:7077")
        .endContainer()
        .endSpec()
        .endTemplate()
        .endSpec()
        .build();
  }
}
