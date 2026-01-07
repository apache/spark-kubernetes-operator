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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceSpec;
import io.fabric8.kubernetes.api.model.ServiceSpecBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetSpec;
import io.fabric8.kubernetes.api.model.apps.StatefulSetSpecBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.k8s.operator.spec.ClusterSpec;
import org.apache.spark.k8s.operator.spec.ClusterTolerations;
import org.apache.spark.k8s.operator.spec.MasterSpec;
import org.apache.spark.k8s.operator.spec.RuntimeVersions;
import org.apache.spark.k8s.operator.spec.WorkerInstanceConfig;
import org.apache.spark.k8s.operator.spec.WorkerSpec;

class SparkClusterResourceSpecTest {
  SparkCluster cluster;
  ObjectMeta objectMeta;
  ClusterSpec clusterSpec;
  StatefulSetSpec statefulSetSpec;
  ServiceSpec serviceSpec;
  MasterSpec masterSpec;
  WorkerSpec workerSpec;
  RuntimeVersions runtimeVersions = new RuntimeVersions();
  SparkConf sparkConf = new SparkConf().set("spark.kubernetes.namespace", "other-namespace");
  ClusterTolerations clusterTolerations = new ClusterTolerations();
  static final String VERSION = "dev";

  @BeforeEach
  void setUp() {
    cluster = mock(SparkCluster.class);
    objectMeta = mock(ObjectMeta.class);
    clusterSpec = mock(ClusterSpec.class);
    serviceSpec = mock(ServiceSpec.class);
    masterSpec = mock(MasterSpec.class);
    workerSpec = mock(WorkerSpec.class);
    statefulSetSpec = mock(StatefulSetSpec.class);
    when(cluster.getMetadata()).thenReturn(objectMeta);
    when(cluster.getSpec()).thenReturn(clusterSpec);
    when(objectMeta.getNamespace()).thenReturn("my-namespace");
    when(objectMeta.getName()).thenReturn("cluster-name");
    when(clusterSpec.getClusterTolerations()).thenReturn(clusterTolerations);
    when(clusterSpec.getMasterSpec()).thenReturn(masterSpec);
    when(clusterSpec.getWorkerSpec()).thenReturn(workerSpec);
    when(clusterSpec.getRuntimeVersions()).thenReturn(runtimeVersions);
    runtimeVersions.setSparkVersion(VERSION);
    when(masterSpec.getStatefulSetSpec()).thenReturn(statefulSetSpec);
    when(masterSpec.getStatefulSetMetadata()).thenReturn(objectMeta);
    when(masterSpec.getServiceSpec()).thenReturn(serviceSpec);
    when(masterSpec.getServiceMetadata()).thenReturn(objectMeta);
    when(workerSpec.getStatefulSetSpec()).thenReturn(statefulSetSpec);
    when(workerSpec.getStatefulSetMetadata()).thenReturn(objectMeta);
    when(workerSpec.getServiceSpec()).thenReturn(serviceSpec);
    when(workerSpec.getServiceMetadata()).thenReturn(objectMeta);
  }

  @Test
  void testMasterService() {
    Service service1 = new SparkClusterResourceSpec(cluster, new SparkConf()).getMasterService();
    assertEquals("my-namespace", service1.getMetadata().getNamespace());
    assertEquals("cluster-name-master-svc", service1.getMetadata().getName());
    assertEquals(VERSION, service1.getMetadata().getLabels().get(LABEL_SPARK_VERSION_NAME));

    Service service2 = new SparkClusterResourceSpec(cluster, sparkConf).getMasterService();
    assertEquals("other-namespace", service2.getMetadata().getNamespace());
    assertEquals(VERSION, service1.getMetadata().getLabels().get(LABEL_SPARK_VERSION_NAME));
  }

  @Test
  void testWorkerService() {
    Service service1 = new SparkClusterResourceSpec(cluster, new SparkConf()).getWorkerService();
    assertEquals("my-namespace", service1.getMetadata().getNamespace());
    assertEquals("cluster-name-worker-svc", service1.getMetadata().getName());
    assertEquals(VERSION, service1.getMetadata().getLabels().get(LABEL_SPARK_VERSION_NAME));

    Service service2 = new SparkClusterResourceSpec(cluster, sparkConf).getMasterService();
    assertEquals("other-namespace", service2.getMetadata().getNamespace());
  }

  @Test
  void testWorkerServiceWithTemplate() {
    ObjectMeta objectMeta1 =
        new ObjectMetaBuilder()
            .withNamespace("foo")
            .withName("bar")
            .addToLabels("foo", "bar")
            .build();
    ServiceSpec serviceSpec1 = new ServiceSpecBuilder().withExternalName("foo").build();
    WorkerSpec workerSpec1 = mock(WorkerSpec.class);
    when(workerSpec1.getServiceSpec()).thenReturn(serviceSpec1);
    when(workerSpec1.getServiceMetadata()).thenReturn(objectMeta1);
    when(clusterSpec.getWorkerSpec()).thenReturn(workerSpec1);

    Service service1 = new SparkClusterResourceSpec(cluster, new SparkConf()).getWorkerService();
    assertEquals("my-namespace", service1.getMetadata().getNamespace());
    assertEquals("cluster-name-worker-svc", service1.getMetadata().getName());
    assertEquals("bar", service1.getMetadata().getLabels().get("foo"));
    assertEquals(VERSION, service1.getMetadata().getLabels().get(LABEL_SPARK_VERSION_NAME));
    assertEquals("foo", service1.getSpec().getExternalName());
    assertEquals(
        Map.of(
            LABEL_SPARK_CLUSTER_NAME,
            "cluster-name",
            LABEL_SPARK_ROLE_NAME,
            LABEL_SPARK_ROLE_WORKER_VALUE),
        service1.getSpec().getSelector());
  }

  @Test
  void testMasterServiceWithTemplate() {
    ObjectMeta objectMeta1 =
        new ObjectMetaBuilder()
            .withNamespace("foo")
            .withName("bar")
            .addToLabels("foo", "bar")
            .build();
    ServiceSpec serviceSpec1 = new ServiceSpecBuilder().withExternalName("foo").build();
    MasterSpec masterSpec1 = mock(MasterSpec.class);
    when(masterSpec1.getServiceSpec()).thenReturn(serviceSpec1);
    when(masterSpec1.getServiceMetadata()).thenReturn(objectMeta1);
    when(clusterSpec.getMasterSpec()).thenReturn(masterSpec1);

    Service service1 = new SparkClusterResourceSpec(cluster, new SparkConf()).getMasterService();
    assertEquals("my-namespace", service1.getMetadata().getNamespace());
    assertEquals("cluster-name-master-svc", service1.getMetadata().getName());
    assertEquals("bar", service1.getMetadata().getLabels().get("foo"));
    assertEquals(VERSION, service1.getMetadata().getLabels().get(LABEL_SPARK_VERSION_NAME));
    assertEquals("foo", service1.getSpec().getExternalName());
    assertEquals(
        Map.of(
            LABEL_SPARK_CLUSTER_NAME,
            "cluster-name",
            LABEL_SPARK_ROLE_NAME,
            LABEL_SPARK_ROLE_MASTER_VALUE),
        service1.getSpec().getSelector());
  }

  @Test
  void testMasterStatefulSet() {
    SparkClusterResourceSpec spec1 = new SparkClusterResourceSpec(cluster, new SparkConf());
    StatefulSet statefulSet1 = spec1.getMasterStatefulSet();
    assertEquals("my-namespace", statefulSet1.getMetadata().getNamespace());
    assertEquals("cluster-name-master", statefulSet1.getMetadata().getName());
    assertEquals(VERSION, statefulSet1.getMetadata().getLabels().get(LABEL_SPARK_VERSION_NAME));
    assertEquals(
        VERSION,
        statefulSet1
            .getSpec()
            .getTemplate()
            .getMetadata()
            .getLabels()
            .get(LABEL_SPARK_VERSION_NAME));

    SparkClusterResourceSpec spec2 = new SparkClusterResourceSpec(cluster, sparkConf);
    StatefulSet statefulSet2 = spec2.getMasterStatefulSet();
    assertEquals("other-namespace", statefulSet2.getMetadata().getNamespace());
    assertEquals(
        "cluster-name",
        statefulSet2
            .getSpec()
            .getTemplate()
            .getMetadata()
            .getLabels()
            .get(LABEL_SPARK_CLUSTER_NAME));
    assertEquals(
        LABEL_SPARK_ROLE_MASTER_VALUE,
        statefulSet2.getSpec().getTemplate().getMetadata().getLabels().get(LABEL_SPARK_ROLE_NAME));
  }

  @Test
  void testMasterStatefulSetWithTemplate() {
    ObjectMeta objectMeta1 =
        new ObjectMetaBuilder()
            .withNamespace("foo")
            .withName("bar")
            .addToLabels("foo", "bar")
            .build();
    StatefulSetSpec statefulSetSpec1 =
        new StatefulSetSpecBuilder()
            .withNewTemplate()
            .withNewSpec()
            .addNewInitContainer()
            .withName("init-foo")
            .endInitContainer()
            .addNewContainer()
            .withName("sidecar-foo")
            .endContainer()
            .endSpec()
            .endTemplate()
            .build();
    MasterSpec masterSpec1 = mock(MasterSpec.class);
    when(masterSpec1.getStatefulSetMetadata()).thenReturn(objectMeta1);
    when(masterSpec1.getStatefulSetSpec()).thenReturn(statefulSetSpec1);
    when(clusterSpec.getMasterSpec()).thenReturn(masterSpec1);
    SparkClusterResourceSpec spec1 = new SparkClusterResourceSpec(cluster, new SparkConf());
    StatefulSet statefulSet1 = spec1.getMasterStatefulSet();
    assertEquals("my-namespace", statefulSet1.getMetadata().getNamespace());
    assertEquals("cluster-name-master", statefulSet1.getMetadata().getName());
    assertEquals("bar", statefulSet1.getMetadata().getLabels().get("foo"));
    assertEquals(VERSION, statefulSet1.getMetadata().getLabels().get(LABEL_SPARK_VERSION_NAME));
    assertEquals(1, statefulSet1.getSpec().getTemplate().getSpec().getInitContainers().size());
    assertEquals(2, statefulSet1.getSpec().getTemplate().getSpec().getContainers().size());
    assertEquals(
        VERSION,
        statefulSet1
            .getSpec()
            .getTemplate()
            .getMetadata()
            .getLabels()
            .get(LABEL_SPARK_VERSION_NAME));
  }

  @Test
  void testWorkerStatefulSet() {
    SparkClusterResourceSpec spec = new SparkClusterResourceSpec(cluster, new SparkConf());
    StatefulSet statefulSet = spec.getWorkerStatefulSet();
    assertEquals("my-namespace", statefulSet.getMetadata().getNamespace());
    assertEquals("cluster-name-worker", statefulSet.getMetadata().getName());
    assertEquals(VERSION, statefulSet.getMetadata().getLabels().get(LABEL_SPARK_VERSION_NAME));
    assertEquals(
        VERSION,
        statefulSet
            .getSpec()
            .getTemplate()
            .getMetadata()
            .getLabels()
            .get(LABEL_SPARK_VERSION_NAME));

    SparkClusterResourceSpec spec2 = new SparkClusterResourceSpec(cluster, sparkConf);
    StatefulSet statefulSet2 = spec2.getWorkerStatefulSet();
    assertEquals("other-namespace", statefulSet2.getMetadata().getNamespace());
    assertEquals(
        "cluster-name",
        statefulSet2
            .getSpec()
            .getTemplate()
            .getMetadata()
            .getLabels()
            .get(LABEL_SPARK_CLUSTER_NAME));
    assertEquals(
        LABEL_SPARK_ROLE_WORKER_VALUE,
        statefulSet2.getSpec().getTemplate().getMetadata().getLabels().get(LABEL_SPARK_ROLE_NAME));
  }

  @Test
  void testWorkerStatefulSetWithTemplate() {
    ObjectMeta objectMeta1 =
        new ObjectMetaBuilder()
            .withNamespace("foo")
            .withName("bar")
            .addToLabels("foo", "bar")
            .build();
    StatefulSetSpec statefulSetSpec1 =
        new StatefulSetSpecBuilder()
            .withNewTemplate()
            .withNewSpec()
            .addNewInitContainer()
            .withName("init-foo")
            .endInitContainer()
            .addNewContainer()
            .withName("sidecar-foo")
            .endContainer()
            .endSpec()
            .endTemplate()
            .build();
    WorkerSpec workerSpec1 = mock(WorkerSpec.class);
    when(workerSpec1.getStatefulSetMetadata()).thenReturn(objectMeta1);
    when(workerSpec1.getStatefulSetSpec()).thenReturn(statefulSetSpec1);
    when(clusterSpec.getWorkerSpec()).thenReturn(workerSpec1);
    SparkClusterResourceSpec spec = new SparkClusterResourceSpec(cluster, new SparkConf());
    StatefulSet statefulSet = spec.getWorkerStatefulSet();
    assertEquals("my-namespace", statefulSet.getMetadata().getNamespace());
    assertEquals("cluster-name-worker", statefulSet.getMetadata().getName());
    assertEquals(VERSION, statefulSet.getMetadata().getLabels().get(LABEL_SPARK_VERSION_NAME));
    assertEquals(
        VERSION,
        statefulSet
            .getSpec()
            .getTemplate()
            .getMetadata()
            .getLabels()
            .get(LABEL_SPARK_VERSION_NAME));
  }

  @Test
  void testEmptyHorizontalPodAutoscalerByDefault() {
    SparkClusterResourceSpec spec = new SparkClusterResourceSpec(cluster, new SparkConf());
    assertEquals(Optional.empty(), spec.getHorizontalPodAutoscaler());
  }

  @Test
  void testHorizontalPodAutoscaler() {
    var instanceConfig = new WorkerInstanceConfig();
    instanceConfig.setInitWorkers(1);
    instanceConfig.setMinWorkers(1);
    instanceConfig.setMaxWorkers(3);
    var clusterTolerations = new ClusterTolerations();
    clusterTolerations.setInstanceConfig(instanceConfig);
    when(clusterSpec.getClusterTolerations()).thenReturn(clusterTolerations);

    SparkClusterResourceSpec spec = new SparkClusterResourceSpec(cluster, new SparkConf());
    assertTrue(spec.getHorizontalPodAutoscaler().isPresent());
    var hpa = spec.getHorizontalPodAutoscaler().get();
    assertEquals("autoscaling/v2", hpa.getApiVersion());
    assertEquals("HorizontalPodAutoscaler", hpa.getKind());
    assertEquals("my-namespace", hpa.getMetadata().getNamespace());
    assertEquals("cluster-name-worker-hpa", hpa.getMetadata().getName());
    assertEquals(VERSION, hpa.getMetadata().getLabels().get(LABEL_SPARK_VERSION_NAME));
    assertEquals(1, hpa.getSpec().getMinReplicas());
    assertEquals(3, hpa.getSpec().getMaxReplicas());
    assertEquals(1, hpa.getSpec().getMetrics().size());
    assertEquals("worker", hpa.getSpec().getMetrics().get(0).getContainerResource().getContainer());
  }

  @Test
  void testPodDisruptionBudgetCreation() {
    var instanceConfig = new WorkerInstanceConfig();
    instanceConfig.setMinWorkers(1);
    var clusterTolerations = new ClusterTolerations();
    clusterTolerations.setInstanceConfig(instanceConfig);
    when(clusterSpec.getClusterTolerations()).thenReturn(clusterTolerations);

    SparkClusterResourceSpec spec = new SparkClusterResourceSpec(cluster, new SparkConf());
    assertTrue(spec.getPodDisruptionBudget().isPresent());
    var pdb = spec.getPodDisruptionBudget().get();
    assertEquals("policy/v1", pdb.getApiVersion());
    assertEquals("PodDisruptionBudget", pdb.getKind());
    assertEquals("my-namespace", pdb.getMetadata().getNamespace());
    assertEquals("cluster-name-worker-pdb", pdb.getMetadata().getName());
    assertEquals(VERSION, pdb.getMetadata().getLabels().get(LABEL_SPARK_VERSION_NAME));
    assertEquals(1, pdb.getSpec().getMinAvailable().getIntVal());
    assertEquals(
        Map.of(
            LABEL_SPARK_CLUSTER_NAME,
            "cluster-name",
            LABEL_SPARK_ROLE_NAME,
            LABEL_SPARK_ROLE_WORKER_VALUE),
        pdb.getSpec().getSelector().getMatchLabels());
  }

  @Test
  void testPodDisruptionBudgetAbsence() {
    var instanceConfig = new WorkerInstanceConfig();
    instanceConfig.setMinWorkers(0);
    var clusterTolerations = new ClusterTolerations();
    clusterTolerations.setInstanceConfig(instanceConfig);
    when(clusterSpec.getClusterTolerations()).thenReturn(clusterTolerations);

    SparkClusterResourceSpec spec = new SparkClusterResourceSpec(cluster, new SparkConf());
    assertTrue(spec.getPodDisruptionBudget().isEmpty());
  }
}
