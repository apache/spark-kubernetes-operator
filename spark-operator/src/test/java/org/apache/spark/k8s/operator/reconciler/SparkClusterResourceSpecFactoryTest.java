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

package org.apache.spark.k8s.operator.reconciler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import org.junit.jupiter.api.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.SparkClusterResourceSpec;
import org.apache.spark.k8s.operator.SparkClusterSubmissionWorker;

class SparkClusterResourceSpecFactoryTest {

  @Test
  void testNamespace() {
    SparkCluster cluster = new SparkCluster();
    String namespace = "test-namespace";
    cluster.setMetadata(
        new ObjectMetaBuilder().withNamespace(namespace).withName("bar-cluster").build());
    SparkClusterSubmissionWorker mockWorker = mock(SparkClusterSubmissionWorker.class);
    when(mockWorker.getResourceSpec(any(), any()))
        .thenReturn(new SparkClusterResourceSpec(cluster, new SparkConf()));
    SparkClusterResourceSpec spec =
        SparkClusterResourceSpecFactory.buildResourceSpec(cluster, mockWorker);
    verify(mockWorker).getResourceSpec(eq(cluster), any());
    assertEquals(namespace, spec.getMasterService().getMetadata().getNamespace());
    assertEquals(namespace, spec.getWorkerService().getMetadata().getNamespace());
    assertEquals(namespace, spec.getMasterStatefulSet().getMetadata().getNamespace());
    assertEquals(namespace, spec.getWorkerStatefulSet().getMetadata().getNamespace());
  }

  @Test
  void testOwnerReference() {
    SparkCluster cluster = new SparkCluster();
    cluster.setMetadata(
        new ObjectMetaBuilder().withNamespace("test-namespace").withName("my-cluster").build());
    SparkClusterSubmissionWorker mockWorker = mock(SparkClusterSubmissionWorker.class);
    when(mockWorker.getResourceSpec(any(), any()))
        .thenReturn(new SparkClusterResourceSpec(cluster, new SparkConf()));
    SparkClusterResourceSpec spec =
        SparkClusterResourceSpecFactory.buildResourceSpec(cluster, mockWorker);
    verify(mockWorker).getResourceSpec(eq(cluster), any());
    assertEquals(1, spec.getMasterService().getMetadata().getOwnerReferences().size());
    OwnerReference owner = spec.getMasterService().getMetadata().getOwnerReferences().get(0);
    assertEquals("SparkCluster", owner.getKind());
    assertEquals("my-cluster", owner.getName());

    // All resources share the same owner
    assertEquals(owner, spec.getWorkerService().getMetadata().getOwnerReferences().get(0));
    assertEquals(owner, spec.getMasterStatefulSet().getMetadata().getOwnerReferences().get(0));
    assertEquals(owner, spec.getWorkerStatefulSet().getMetadata().getOwnerReferences().get(0));
  }
}
