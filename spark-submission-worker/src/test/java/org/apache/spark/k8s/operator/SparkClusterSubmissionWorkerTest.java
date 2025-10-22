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

import java.util.HashMap;
import java.util.Map;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.spec.ClusterSpec;
import org.apache.spark.k8s.operator.spec.ClusterTolerations;
import org.apache.spark.k8s.operator.spec.MasterSpec;
import org.apache.spark.k8s.operator.spec.RuntimeVersions;
import org.apache.spark.k8s.operator.spec.WorkerSpec;

class SparkClusterSubmissionWorkerTest {
  SparkCluster cluster;
  ObjectMeta objectMeta;
  ClusterSpec clusterSpec;
  ClusterTolerations clusterTolerations = new ClusterTolerations();
  MasterSpec masterSpec;
  WorkerSpec workerSpec;
  RuntimeVersions runtimeVersions;

  @BeforeEach
  void setUp() {
    cluster = mock(SparkCluster.class);
    objectMeta = mock(ObjectMeta.class);
    clusterSpec = mock(ClusterSpec.class);
    masterSpec = mock(MasterSpec.class);
    workerSpec = mock(WorkerSpec.class);
    runtimeVersions = mock(RuntimeVersions.class);
    when(cluster.getMetadata()).thenReturn(objectMeta);
    when(cluster.getSpec()).thenReturn(clusterSpec);
    when(objectMeta.getNamespace()).thenReturn("my-namespace");
    when(objectMeta.getName()).thenReturn("cluster-name");
    when(clusterSpec.getClusterTolerations()).thenReturn(clusterTolerations);
    when(clusterSpec.getMasterSpec()).thenReturn(masterSpec);
    when(clusterSpec.getWorkerSpec()).thenReturn(workerSpec);
    when(clusterSpec.getRuntimeVersions()).thenReturn(runtimeVersions);
    when(runtimeVersions.getSparkVersion()).thenReturn("dev");
    Map<String, String> sparkConf = new HashMap<>();
    sparkConf.put("spark.kubernetes.container.image", "apache/spark:{{SPARK_VERSION}}");
    when(clusterSpec.getSparkConf()).thenReturn(sparkConf);
  }

  @Test
  void testGetResourceSpec() {
    SparkClusterSubmissionWorker worker = new SparkClusterSubmissionWorker();
    SparkClusterResourceSpec spec = worker.getResourceSpec(cluster, Map.of());
    // SparkClusterResourceSpecTest will cover the detail information of easy resources
    assertNotNull(spec.getMasterService());
    assertNotNull(spec.getMasterStatefulSet());
    assertNotNull(spec.getWorkerStatefulSet());
    assertNotNull(spec.getHorizontalPodAutoscaler());
  }

  @Test
  void supportSparkVersionPlaceHolder() {
    SparkClusterSubmissionWorker worker = new SparkClusterSubmissionWorker();
    SparkClusterResourceSpec spec = worker.getResourceSpec(cluster, Map.of());
    assertEquals(
        "apache/spark:dev",
        spec.getMasterStatefulSet()
            .getSpec()
            .getTemplate()
            .getSpec()
            .getContainers()
            .get(0)
            .getImage());
  }
}
