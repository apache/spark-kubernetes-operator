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

package org.apache.spark.k8s.operator.context;

import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.SparkClusterSubmissionWorker;
import org.apache.spark.k8s.operator.kueue.KueuePodSetFlavor;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadFactory;
import org.apache.spark.k8s.operator.spec.ClusterSpec;
import org.apache.spark.k8s.operator.spec.ClusterTolerations;
import org.apache.spark.k8s.operator.spec.RuntimeVersions;
import org.apache.spark.k8s.operator.spec.WorkerInstanceConfig;
import org.apache.spark.k8s.operator.utils.ModelUtils;

class SparkClusterContextTest {

  @Test
  void kueuePodSetFlavorsAreAppliedToStatefulSets() {
    SparkCluster cluster = buildCluster();
    String clusterSpec = asJson(cluster.getSpec());
    SparkClusterContext context =
        new SparkClusterContext(cluster, mock(Context.class), new SparkClusterSubmissionWorker());
    Toleration spot = new Toleration("NoSchedule", "spot", "Exists", null, null);
    Toleration gpu = new Toleration("NoSchedule", "gpu", "Exists", null, null);

    // The master StatefulSet spec is built before the admission to check whether it exists
    Assertions.assertEquals(
        List.of(), podSpec(context.getMasterStatefulSetSpec()).getTolerations());

    context.setKueuePodSetFlavors(
        Map.of(
            KueueWorkloadFactory.PODSET_MASTER,
            new KueuePodSetFlavor(Map.of("pool", "cpu"), List.of(spot)),
            KueueWorkloadFactory.PODSET_WORKER,
            new KueuePodSetFlavor(Map.of("pool", "gpu"), List.of(gpu))));

    PodSpec master = podSpec(context.getMasterStatefulSetSpec());
    Assertions.assertEquals(Map.of("pool", "cpu"), master.getNodeSelector());
    Assertions.assertEquals(List.of(spot), master.getTolerations());
    PodSpec worker = podSpec(context.getWorkerStatefulSetSpec());
    Assertions.assertEquals(Map.of("pool", "gpu"), worker.getNodeSelector());
    Assertions.assertEquals(List.of(gpu), worker.getTolerations());
    // The flavors are applied to the built StatefulSets only, so the SparkCluster keeps the pod
    // templates which the pod sets of its Kueue Workload are hashed from
    Assertions.assertEquals(clusterSpec, asJson(cluster.getSpec()));
  }

  private static String asJson(Object value) {
    try {
      return ModelUtils.objectMapper.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  private static PodSpec podSpec(StatefulSet statefulSet) {
    return statefulSet.getSpec().getTemplate().getSpec();
  }

  private static SparkCluster buildCluster() {
    SparkCluster cluster = new SparkCluster();
    cluster.setMetadata(
        new ObjectMetaBuilder()
            .withName("cluster1")
            .withNamespace("default")
            .withLabels(Map.of(Constants.LABEL_QUEUE_NAME, "test-queue"))
            .build());
    cluster.setSpec(
        ClusterSpec.builder()
            .runtimeVersions(RuntimeVersions.builder().sparkVersion("4.2.0").build())
            .clusterTolerations(
                ClusterTolerations.builder()
                    .instanceConfig(
                        WorkerInstanceConfig.builder()
                            .initWorkers(1)
                            .minWorkers(1)
                            .maxWorkers(1)
                            .build())
                    .build())
            .build());
    return cluster;
  }
}
