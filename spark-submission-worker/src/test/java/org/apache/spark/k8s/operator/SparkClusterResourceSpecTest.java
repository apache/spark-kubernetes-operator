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
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.k8s.operator.spec.ClusterSpec;
import org.apache.spark.k8s.operator.spec.ClusterTolerations;

class SparkClusterResourceSpecTest {
  SparkCluster cluster;
  ObjectMeta objectMeta;
  ClusterSpec clusterSpec;
  SparkConf sparkConf = new SparkConf().set("spark.kubernetes.namespace", "other-namespace");
  ClusterTolerations clusterTolerations = new ClusterTolerations();

  @BeforeEach
  void setUp() {
    cluster = mock(SparkCluster.class);
    objectMeta = mock(ObjectMeta.class);
    clusterSpec = mock(ClusterSpec.class);
    when(cluster.getMetadata()).thenReturn(objectMeta);
    when(cluster.getSpec()).thenReturn(clusterSpec);
    when(objectMeta.getNamespace()).thenReturn("my-namespace");
    when(objectMeta.getName()).thenReturn("cluster-name");
    when(clusterSpec.getClusterTolerations()).thenReturn(clusterTolerations);
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
  void testWorkerStatefulSet() {
    SparkClusterResourceSpec spec = new SparkClusterResourceSpec(cluster, new SparkConf());
    StatefulSet statefulSet = spec.getWorkerStatefulSet();
    assertEquals("my-namespace", statefulSet.getMetadata().getNamespace());
    assertEquals("cluster-name-worker", statefulSet.getMetadata().getName());

    SparkClusterResourceSpec spec2 = new SparkClusterResourceSpec(cluster, sparkConf);
    StatefulSet statefulSet2 = spec2.getWorkerStatefulSet();
    assertEquals("other-namespace", statefulSet2.getMetadata().getNamespace());
  }
}
