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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.SparkClusterSubmissionWorker;
import org.apache.spark.k8s.operator.config.SparkOperatorConf;
import org.apache.spark.k8s.operator.kueue.KueuePodSetFlavor;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadFactory;
import org.apache.spark.k8s.operator.kueue.v1beta2.Workload;
import org.apache.spark.k8s.operator.spec.ClusterSpec;
import org.apache.spark.k8s.operator.spec.ClusterTolerations;
import org.apache.spark.k8s.operator.spec.RuntimeVersions;
import org.apache.spark.k8s.operator.spec.WorkerInstanceConfig;
import org.apache.spark.k8s.operator.utils.ModelUtils;
import org.apache.spark.k8s.operator.utils.TestUtils;

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
            new KueuePodSetFlavor(
                Map.of("pool", "cpu"),
                List.of(spot),
                Map.of("team", "a"),
                Map.of("provisioning", "pr-master")),
            KueueWorkloadFactory.PODSET_WORKER,
            new KueuePodSetFlavor(
                Map.of("pool", "gpu"),
                List.of(gpu),
                Map.of("team", "a"),
                Map.of("provisioning", "pr-worker"))));

    PodSpec master = podSpec(context.getMasterStatefulSetSpec());
    Assertions.assertEquals(Map.of("pool", "cpu"), master.getNodeSelector());
    Assertions.assertEquals(List.of(spot), master.getTolerations());
    PodSpec worker = podSpec(context.getWorkerStatefulSetSpec());
    Assertions.assertEquals(Map.of("pool", "gpu"), worker.getNodeSelector());
    Assertions.assertEquals(List.of(gpu), worker.getTolerations());
    // The labels which the StatefulSets select their pods with are kept
    ObjectMeta masterMetadata =
        context.getMasterStatefulSetSpec().getSpec().getTemplate().getMetadata();
    Assertions.assertEquals("a", masterMetadata.getLabels().get("team"));
    Assertions.assertEquals("pr-master", masterMetadata.getAnnotations().get("provisioning"));
    Assertions.assertEquals(
        Constants.LABEL_SPARK_ROLE_MASTER_VALUE,
        masterMetadata.getLabels().get(Constants.LABEL_SPARK_ROLE_NAME));
    ObjectMeta workerMetadata =
        context.getWorkerStatefulSetSpec().getSpec().getTemplate().getMetadata();
    Assertions.assertEquals("a", workerMetadata.getLabels().get("team"));
    Assertions.assertEquals("pr-worker", workerMetadata.getAnnotations().get("provisioning"));
    Assertions.assertEquals(
        Constants.LABEL_SPARK_ROLE_WORKER_VALUE,
        workerMetadata.getLabels().get(Constants.LABEL_SPARK_ROLE_NAME));
    // The flavors are applied to the built StatefulSets only, so the SparkCluster keeps the pod
    // templates which the pod sets of its Kueue Workload are hashed from
    Assertions.assertEquals(clusterSpec, asJson(cluster.getSpec()));
  }

  @Test
  @SuppressWarnings("unchecked")
  void cachedKueueWorkloadIsReadOnlyWithKueueIntegration() {
    SparkCluster cluster = buildCluster();
    Context<SparkCluster> josdkContext = mock(Context.class);
    Workload workload = KueueWorkloadFactory.buildWorkload(cluster);
    when(josdkContext.getSecondaryResource(Workload.class)).thenReturn(Optional.of(workload));
    SparkClusterContext context =
        new SparkClusterContext(cluster, josdkContext, new SparkClusterSubmissionWorker());

    // Without the integration, no Workload informer is registered to read from
    Assertions.assertEquals(Optional.empty(), context.getCachedKueueWorkload());
    verifyNoInteractions(josdkContext);

    TestUtils.setConfigKey(SparkOperatorConf.KUEUE_ENABLED, true);
    try {
      Assertions.assertEquals(Optional.of(workload), context.getCachedKueueWorkload());
    } finally {
      TestUtils.setConfigKey(SparkOperatorConf.KUEUE_ENABLED, false);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void readyPodsAreCountedPerSparkRoleOnce() {
    Context<SparkCluster> josdkContext = mock(Context.class);
    when(josdkContext.getSecondaryResourcesAsStream(Pod.class))
        .thenAnswer(
            i ->
                Stream.of(
                    pod(Constants.LABEL_SPARK_ROLE_MASTER_VALUE, true),
                    pod(Constants.LABEL_SPARK_ROLE_WORKER_VALUE, true),
                    pod(Constants.LABEL_SPARK_ROLE_WORKER_VALUE, false),
                    // e.g. a client which carries the cluster label to reach the workers
                    pod(null, true)));
    SparkClusterContext context =
        new SparkClusterContext(buildCluster(), josdkContext, new SparkClusterSubmissionWorker());

    Map<String, Long> expected =
        Map.of(
            Constants.LABEL_SPARK_ROLE_MASTER_VALUE, 1L,
            Constants.LABEL_SPARK_ROLE_WORKER_VALUE, 1L);
    Assertions.assertEquals(expected, context.countReadyPodsByRole());
    // A reconciliation which checks them again shares the count
    Assertions.assertEquals(expected, context.countReadyPodsByRole());
    verify(josdkContext).getSecondaryResourcesAsStream(Pod.class);
  }

  private static Pod pod(String role, boolean ready) {
    return new PodBuilder()
        .withNewMetadata()
        .withName("pod")
        .withLabels(role == null ? Map.of() : Map.of(Constants.LABEL_SPARK_ROLE_NAME, role))
        .endMetadata()
        .withNewStatus()
        .withPhase("Running")
        .addNewCondition()
        .withType("Ready")
        .withStatus(String.valueOf(ready))
        .endCondition()
        .endStatus()
        .build();
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
