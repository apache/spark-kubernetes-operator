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
  @Getter private final StatefulSet masterStatefulSet;
  @Getter private final StatefulSet workerStatefulSet;

  public SparkClusterResourceSpec(SparkCluster cluster, SparkConf conf) {
    String clusterNamespace = cluster.getMetadata().getNamespace();
    String clusterName = cluster.getMetadata().getName();
    String namespace = conf.get(Config.KUBERNETES_NAMESPACE().key(), clusterNamespace);
    String image = conf.get(Config.CONTAINER_IMAGE().key(), "spark:4.0.0-preview1");
    ClusterSpec spec = cluster.getSpec();
    masterService = buildMasterService(clusterName, namespace);
    masterStatefulSet = buildMasterStatefulSet(clusterName, namespace, image);
    workerStatefulSet =
        buildWorkerStatefulSet(clusterName, namespace, image, spec.getInitWorkers());
  }

  private static Service buildMasterService(String name, String namespace) {
    return new ServiceBuilder()
        .withNewMetadata()
        .withName(name + "-svc")
        .addToLabels(LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_MASTER_VALUE)
        .withNamespace(namespace)
        .endMetadata()
        .withNewSpec()
        .withClusterIP("None")
        .withSelector(Collections.singletonMap("spark-role", "master"))
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

  private static StatefulSet buildMasterStatefulSet(String name, String namespace, String image) {
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
        .addNewContainer()
        .withName("master")
        .withImage(image)
        .addNewEnv()
        .withName("SPARK_NO_DAEMONIZE")
        .withValue("1")
        .endEnv()
        .addToCommand("bash")
        .addToArgs("/opt/spark/sbin/start-master.sh")
        .addNewPort()
        .withName("web")
        .withHostPort(8080)
        .withContainerPort(8080)
        .endPort()
        .addNewPort()
        .withName("spark")
        .withHostPort(7077)
        .withContainerPort(7070)
        .endPort()
        .addNewPort()
        .withName("rest")
        .withHostPort(6066)
        .withContainerPort(6066)
        .endPort()
        .endContainer()
        .endSpec()
        .endTemplate()
        .endSpec()
        .build();
  }

  private static StatefulSet buildWorkerStatefulSet(
      String name, String namespace, String image, int initWorkers) {
    return new StatefulSetBuilder()
        .withNewMetadata()
        .withName(name + "-worker")
        .addToLabels(LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_WORKER_VALUE)
        .withNamespace(namespace)
        .endMetadata()
        .withNewSpec()
        .withPodManagementPolicy("Parallel")
        .withReplicas(initWorkers)
        .withNewSelector()
        .addToMatchLabels(LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_WORKER_VALUE)
        .endSelector()
        .withNewTemplate()
        .withNewMetadata()
        .addToLabels(LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_WORKER_VALUE)
        .endMetadata()
        .withNewSpec()
        .addNewContainer()
        .withName("worker")
        .withImage(image)
        .addNewEnv()
        .withName("SPARK_NO_DAEMONIZE")
        .withValue("1")
        .endEnv()
        .addToCommand("bash")
        .addToArgs("/opt/spark/sbin/start-worker.sh", "spark://master:7077")
        .endContainer()
        .endSpec()
        .endTemplate()
        .endSpec()
        .build();
  }
}
