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

package org.apache.spark.k8s.operator.reconciler.reconcilesteps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodListBuilder;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.autoscaling.v2.HorizontalPodAutoscalerBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudgetBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.event.EventRecord;
import io.javaoperatorsdk.operator.api.event.EventType;
import io.javaoperatorsdk.operator.api.event.ResourceEventRecorder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.config.SparkOperatorConf;
import org.apache.spark.k8s.operator.context.SparkClusterContext;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadFactory;
import org.apache.spark.k8s.operator.kueue.v1beta2.Workload;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.spec.ClusterSpec;
import org.apache.spark.k8s.operator.spec.ClusterTolerations;
import org.apache.spark.k8s.operator.spec.RuntimeVersions;
import org.apache.spark.k8s.operator.spec.WorkerInstanceConfig;
import org.apache.spark.k8s.operator.status.ClusterState;
import org.apache.spark.k8s.operator.status.ClusterStateSummary;
import org.apache.spark.k8s.operator.utils.EventUtils;
import org.apache.spark.k8s.operator.utils.SparkClusterStatusRecorder;
import org.apache.spark.k8s.operator.utils.TestUtils;

@EnableKubernetesMockClient(crud = true)
class ClusterCleanUpStepTest {
  private KubernetesClient kubernetesClient;

  private final SparkClusterContext mockContext = mock(SparkClusterContext.class);
  private final SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
  private final ResourceEventRecorder eventRecorder = mock(ResourceEventRecorder.class);

  @BeforeEach
  void enableKueue() {
    TestUtils.setConfigKey(SparkOperatorConf.KUEUE_ENABLED, true);
  }

  @AfterEach
  void disableKueue() {
    TestUtils.setConfigKey(SparkOperatorConf.KUEUE_ENABLED, false);
  }

  @Test
  void failedClusterReleasesEverythingByNameAndTerminates() {
    SparkCluster cluster = buildCluster(ClusterStateSummary.SchedulingFailure);
    stubContext(cluster, kubernetesClient);
    List<HasMetadata> resources =
        List.of(
            new StatefulSetBuilder().withMetadata(metadata("cluster1-master")).build(),
            new StatefulSetBuilder().withMetadata(metadata("cluster1-worker")).build(),
            new ServiceBuilder().withMetadata(metadata("cluster1-master-svc")).build(),
            new ServiceBuilder().withMetadata(metadata("cluster1-worker-svc")).build(),
            new NetworkPolicyBuilder().withMetadata(metadata("cluster1-worker")).build(),
            new HorizontalPodAutoscalerBuilder()
                .withMetadata(metadata("cluster1-worker-hpa"))
                .build(),
            new PodDisruptionBudgetBuilder().withMetadata(metadata("cluster1-worker-pdb")).build());
    resources.forEach(resource -> kubernetesClient.resource(resource).create());
    kubernetesClient.resource(KueueWorkloadFactory.buildWorkload(cluster)).create();

    Assertions.assertEquals(
        ReconcileProgress.completeAndImmediateRequeue(),
        new ClusterCleanUpStep().reconcile(mockContext, recorder));

    // Unlike a suspended cluster, the Services and the NetworkPolicy are not applied again
    for (HasMetadata resource : resources) {
      Assertions.assertNull(
          kubernetesClient.resource(resource).get(), resource.getMetadata().getName());
    }
    Assertions.assertNull(getWorkload());
    // By name, so that neither a spec which cannot be built nor one which no longer asks for the
    // HorizontalPodAutoscaler holds them
    verify(mockContext, never()).getMasterStatefulSetSpec();
    verify(mockContext, never()).getHorizontalPodAutoscalerSpec();
    ClusterState state = captureAppendedState();
    Assertions.assertEquals(ClusterStateSummary.ResourceReleased, state.getCurrentStateSummary());
    Assertions.assertEquals(
        Constants.CLUSTER_RELEASED_AFTER_SCHEDULING_FAILURE_MESSAGE, state.getMessage());
  }

  @Test
  void stuckPodsDoNotHoldTheFailedCluster() {
    SparkCluster cluster = buildCluster(ClusterStateSummary.SchedulingFailure);
    KubernetesClient client = spy(kubernetesClient);
    // Pods stuck in terminating, e.g. on a lost node, past their timeout
    stubPodList(
        client,
        terminatingPod("cluster1-master-0", Constants.LABEL_SPARK_ROLE_MASTER_VALUE),
        terminatingPod("cluster1-worker-0", Constants.LABEL_SPARK_ROLE_WORKER_VALUE));
    stubContext(cluster, client);
    kubernetesClient.resource(KueueWorkloadFactory.buildWorkload(cluster)).create();

    // Unlike a suspended cluster, which may be resumed, nothing creates pods of the same names
    // again, so the cluster terminates without waiting for them to be gone
    Assertions.assertEquals(
        ReconcileProgress.completeAndImmediateRequeue(),
        new ClusterCleanUpStep().reconcile(mockContext, recorder));

    Assertions.assertNull(getWorkload());
    Assertions.assertEquals(
        ClusterStateSummary.ResourceReleased, captureAppendedState().getCurrentStateSummary());
  }

  @ParameterizedTest
  @CsvSource({"403, 1", "500, 1", "503, 0"})
  void failedReleaseIsRetried(int code, int events) {
    SparkCluster cluster = buildCluster(ClusterStateSummary.SchedulingFailure);
    KubernetesClient mockClient = mock(KubernetesClient.class);
    when(mockClient.apps()).thenThrow(new KubernetesClientException("failed", code, null));
    stubContext(cluster, mockClient);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        new ClusterCleanUpStep().reconcile(mockContext, recorder));

    // It stays in SchedulingFailure, which reports the failure to request the resources, so the
    // failure to release them is reported unless it may clear on its own
    verify(mockClient, never()).resources(Workload.class);
    verifyNoInteractions(recorder);
    ArgumentCaptor<EventRecord> event = ArgumentCaptor.forClass(EventRecord.class);
    verify(eventRecorder, times(events)).record(event.capture());
    for (EventRecord record : event.getAllValues()) {
      Assertions.assertEquals(EventType.WARNING, record.type());
      Assertions.assertEquals(EventUtils.REASON_CLUSTER_RELEASE_FAILED, record.reason());
    }
  }

  @Test
  void otherStatesProceed() {
    for (ClusterStateSummary summary : ClusterStateSummary.values()) {
      if (summary == ClusterStateSummary.SchedulingFailure) {
        continue;
      }
      SparkCluster cluster = buildCluster(summary);
      KubernetesClient mockClient = mock(KubernetesClient.class);
      stubContext(cluster, mockClient);

      Assertions.assertEquals(
          ReconcileProgress.proceed(), new ClusterCleanUpStep().reconcile(mockContext, recorder));

      verifyNoInteractions(mockClient);
    }
    verifyNoInteractions(recorder);
  }

  private void stubContext(SparkCluster cluster, KubernetesClient client) {
    when(recorder.appendNewStateAndPersist(any(), any())).thenReturn(true);
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(client);
  }

  /**
   * Stubs the LIST of the master and worker pods of cluster1 on the given spy to return the given
   * pods, since the mock server does not evaluate set-based label selectors.
   */
  @SuppressWarnings("unchecked")
  private static void stubPodList(KubernetesClient client, Pod... items) {
    MixedOperation<Pod, PodList, PodResource> pods = mock(MixedOperation.class);
    NonNamespaceOperation<Pod, PodList, PodResource> namespacedPods =
        mock(NonNamespaceOperation.class);
    FilterWatchListDeletable<Pod, PodList, PodResource> labeledPods =
        mock(FilterWatchListDeletable.class);
    FilterWatchListDeletable<Pod, PodList, PodResource> rolePods =
        mock(FilterWatchListDeletable.class);
    doReturn(pods).when(client).pods();
    when(pods.inNamespace("default")).thenReturn(namespacedPods);
    when(namespacedPods.withLabel(Constants.LABEL_SPARK_CLUSTER_NAME, "cluster1"))
        .thenReturn(labeledPods);
    when(labeledPods.withLabelIn(
            Constants.LABEL_SPARK_ROLE_NAME,
            Constants.LABEL_SPARK_ROLE_MASTER_VALUE,
            Constants.LABEL_SPARK_ROLE_WORKER_VALUE))
        .thenReturn(rolePods);
    when(rolePods.list()).thenReturn(new PodListBuilder().addToItems(items).build());
  }

  private ClusterState captureAppendedState() {
    ArgumentCaptor<ClusterState> captor = ArgumentCaptor.forClass(ClusterState.class);
    verify(recorder).appendNewStateAndPersist(eq(mockContext), captor.capture());
    verify(recorder, never()).persistStatus(any(), any());
    return captor.getValue();
  }

  private Workload getWorkload() {
    return kubernetesClient
        .resources(Workload.class)
        .inNamespace("default")
        .withName("sparkcluster-cluster1")
        .get();
  }

  private static SparkCluster buildCluster(ClusterStateSummary summary) {
    SparkCluster cluster = new SparkCluster();
    cluster.setMetadata(
        new ObjectMetaBuilder()
            .withName("cluster1")
            .withNamespace("default")
            .withUid("cluster-uid")
            .withLabels(Map.of(Constants.LABEL_QUEUE_NAME, "cluster-queue"))
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
    cluster.setStatus(cluster.getStatus().appendNewState(new ClusterState(summary, "")));
    return cluster;
  }

  private static ObjectMeta metadata(String name) {
    return new ObjectMetaBuilder().withName(name).withNamespace("default").build();
  }

  /** Builds a pod which is still terminating 6 minutes past its deletion. */
  private static Pod terminatingPod(String name, String role) {
    return new PodBuilder()
        .withNewMetadata()
        .withName(name)
        .withNamespace("default")
        .withDeletionTimestamp(Instant.now().minus(Duration.ofMinutes(6)).toString())
        .addToLabels(Constants.LABEL_SPARK_CLUSTER_NAME, "cluster1")
        .addToLabels(Constants.LABEL_SPARK_ROLE_NAME, role)
        .endMetadata()
        .build();
  }
}
