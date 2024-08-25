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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import org.apache.spark.k8s.operator.spec.WorkerSpec;

class SparkClusterResourceSpecTest {
  SparkCluster cluster;
  ObjectMeta objectMeta;
  ClusterSpec clusterSpec;
  StatefulSetSpec statefulSetSpec;
  ServiceSpec serviceSpec;
  MasterSpec masterSpec;
  WorkerSpec workerSpec;
  SparkConf sparkConf = new SparkConf().set("spark.kubernetes.namespace", "other-namespace");
  ClusterTolerations clusterTolerations = new ClusterTolerations();

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
    when(masterSpec.getMasterStatefulSetSpec()).thenReturn(statefulSetSpec);
    when(masterSpec.getMasterStatefulSetMetadata()).thenReturn(objectMeta);
    when(masterSpec.getMasterServiceSpec()).thenReturn(serviceSpec);
    when(masterSpec.getMasterServiceMetadata()).thenReturn(objectMeta);
    when(workerSpec.getWorkerStatefulSetSpec()).thenReturn(statefulSetSpec);
    when(workerSpec.getWorkerStatefulSetMetadata()).thenReturn(objectMeta);
    when(workerSpec.getWorkerServiceSpec()).thenReturn(serviceSpec);
    when(workerSpec.getWorkerServiceMetadata()).thenReturn(objectMeta);
  }

  @Test
  void testMasterService() {
    Service service1 = new SparkClusterResourceSpec(cluster, new SparkConf()).getMasterService();
    assertEquals("my-namespace", service1.getMetadata().getNamespace());
    assertEquals("cluster-name-master-svc", service1.getMetadata().getName());

    Service service2 = new SparkClusterResourceSpec(cluster, sparkConf).getMasterService();
    assertEquals("other-namespace", service2.getMetadata().getNamespace());
  }

  @Test
  void testWorkerService() {
    Service service1 = new SparkClusterResourceSpec(cluster, new SparkConf()).getWorkerService();
    assertEquals("my-namespace", service1.getMetadata().getNamespace());
    assertEquals("cluster-name-worker-svc", service1.getMetadata().getName());

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
    when(workerSpec1.getWorkerServiceSpec()).thenReturn(serviceSpec1);
    when(workerSpec1.getWorkerServiceMetadata()).thenReturn(objectMeta1);
    when(clusterSpec.getWorkerSpec()).thenReturn(workerSpec1);

    Service service1 = new SparkClusterResourceSpec(cluster, new SparkConf()).getWorkerService();
    assertEquals("my-namespace", service1.getMetadata().getNamespace());
    assertEquals("cluster-name-worker-svc", service1.getMetadata().getName());
    assertEquals("bar", service1.getMetadata().getLabels().get("foo"));
    assertEquals("foo", service1.getSpec().getExternalName());
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
    when(masterSpec1.getMasterServiceSpec()).thenReturn(serviceSpec1);
    when(masterSpec1.getMasterServiceMetadata()).thenReturn(objectMeta1);
    when(clusterSpec.getMasterSpec()).thenReturn(masterSpec1);

    Service service1 = new SparkClusterResourceSpec(cluster, new SparkConf()).getMasterService();
    assertEquals("my-namespace", service1.getMetadata().getNamespace());
    assertEquals("cluster-name-master-svc", service1.getMetadata().getName());
    assertEquals("bar", service1.getMetadata().getLabels().get("foo"));
    assertEquals("foo", service1.getSpec().getExternalName());
  }

  @Test
  void testMasterStatefulSet() {
    SparkClusterResourceSpec spec1 = new SparkClusterResourceSpec(cluster, new SparkConf());
    StatefulSet statefulSet1 = spec1.getMasterStatefulSet();
    assertEquals("my-namespace", statefulSet1.getMetadata().getNamespace());
    assertEquals("cluster-name-master", statefulSet1.getMetadata().getName());

    SparkClusterResourceSpec spec2 = new SparkClusterResourceSpec(cluster, sparkConf);
    StatefulSet statefulSet2 = spec2.getMasterStatefulSet();
    assertEquals("other-namespace", statefulSet2.getMetadata().getNamespace());
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
    when(masterSpec1.getMasterStatefulSetMetadata()).thenReturn(objectMeta1);
    when(masterSpec1.getMasterStatefulSetSpec()).thenReturn(statefulSetSpec1);
    when(clusterSpec.getMasterSpec()).thenReturn(masterSpec1);
    SparkClusterResourceSpec spec1 = new SparkClusterResourceSpec(cluster, new SparkConf());
    StatefulSet statefulSet1 = spec1.getMasterStatefulSet();
    assertEquals("my-namespace", statefulSet1.getMetadata().getNamespace());
    assertEquals("cluster-name-master", statefulSet1.getMetadata().getName());
    assertEquals("bar", statefulSet1.getMetadata().getLabels().get("foo"));
    assertEquals(1, statefulSet1.getSpec().getTemplate().getSpec().getInitContainers().size());
    assertEquals(2, statefulSet1.getSpec().getTemplate().getSpec().getContainers().size());
  }

  @Test
  void testWorkerStatefulSet() {
    SparkClusterResourceSpec spec = new SparkClusterResourceSpec(cluster, new SparkConf());
    StatefulSet statefulSet = spec.getWorkerStatefulSet();
    assertEquals("my-namespace", statefulSet.getMetadata().getNamespace());
    assertEquals("cluster-name-worker", statefulSet.getMetadata().getName());

    SparkClusterResourceSpec spec2 = new SparkClusterResourceSpec(cluster, sparkConf);
    StatefulSet statefulSet2 = spec2.getWorkerStatefulSet();
    assertEquals("other-namespace", statefulSet2.getMetadata().getNamespace());
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
    when(workerSpec1.getWorkerStatefulSetMetadata()).thenReturn(objectMeta1);
    when(workerSpec1.getWorkerStatefulSetSpec()).thenReturn(statefulSetSpec1);
    when(clusterSpec.getWorkerSpec()).thenReturn(workerSpec1);
    SparkClusterResourceSpec spec = new SparkClusterResourceSpec(cluster, new SparkConf());
    StatefulSet statefulSet = spec.getWorkerStatefulSet();
    assertEquals("my-namespace", statefulSet.getMetadata().getNamespace());
    assertEquals("cluster-name-worker", statefulSet.getMetadata().getName());
  }
}
