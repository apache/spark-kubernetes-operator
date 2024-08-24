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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Collections;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.spec.ClusterSpec;
import org.apache.spark.k8s.operator.spec.ClusterTolerations;

class SparkClusterSubmissionWorkerTest {
  SparkCluster cluster;
  ObjectMeta objectMeta;
  ClusterSpec clusterSpec;
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
  void testGetResourceSpec() {
    SparkClusterSubmissionWorker worker = new SparkClusterSubmissionWorker();
    SparkClusterResourceSpec spec = worker.getResourceSpec(cluster, Collections.emptyMap());
    // SparkClusterResourceSpecTest will cover the detail information of easy resources
    assertNotNull(spec.getMasterService());
    assertNotNull(spec.getMasterStatefulSet());
    assertNotNull(spec.getWorkerStatefulSet());
  }
}
