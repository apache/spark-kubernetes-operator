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

import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_CLUSTER_NAME;
import static org.apache.spark.k8s.operator.utils.TestUtils.setConfigKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.fabric8.kubernetes.api.model.ConditionBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.api.config.informer.InformerEventSourceConfiguration;
import io.javaoperatorsdk.operator.api.event.EventRecord;
import io.javaoperatorsdk.operator.api.event.EventType;
import io.javaoperatorsdk.operator.api.event.ResourceEventRecorder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;

import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.SparkClusterSubmissionWorker;
import org.apache.spark.k8s.operator.config.SparkOperatorConf;
import org.apache.spark.k8s.operator.context.SparkClusterContext;
import org.apache.spark.k8s.operator.kueue.v1beta2.Workload;
import org.apache.spark.k8s.operator.kueue.v1beta2.WorkloadStatus;
import org.apache.spark.k8s.operator.metrics.healthcheck.SentinelManager;
import org.apache.spark.k8s.operator.reconciler.reconcilesteps.ClusterReconcileStep;
import org.apache.spark.k8s.operator.status.ClusterState;
import org.apache.spark.k8s.operator.status.ClusterStateSummary;
import org.apache.spark.k8s.operator.status.ClusterStatus;
import org.apache.spark.k8s.operator.utils.EventUtils;
import org.apache.spark.k8s.operator.utils.SparkClusterStatusRecorder;

class SparkClusterReconcilerTest {
  private final SparkClusterStatusRecorder mockRecorder = mock(SparkClusterStatusRecorder.class);
  private final SentinelManager<SparkCluster> mockSentinelManager = mock(SentinelManager.class);
  private final KubernetesClient mockClient = mock(KubernetesClient.class);
  private final Context<SparkCluster> mockContext = mock(Context.class);
  private final ResourceEventRecorder mockEventRecorder = mock(ResourceEventRecorder.class);
  private final SparkClusterSubmissionWorker mockWorker = mock(SparkClusterSubmissionWorker.class);
  SparkCluster cluster = new SparkCluster();
  SparkClusterReconciler reconciler =
      new SparkClusterReconciler(mockWorker, mockRecorder, mockSentinelManager);

  @BeforeEach
  void beforeEach() {
    when(mockContext.getClient()).thenReturn(mockClient);
    doNothing().when(mockRecorder).removeCachedStatus(any(SparkCluster.class));
    doAnswer(
            invocation -> {
              cluster.setStatus(invocation.getArgument(1));
              return null;
            })
        .when(mockRecorder)
        .persistStatus(any(SparkClusterContext.class), any(ClusterStatus.class));
    doAnswer(
            invocation -> {
              ClusterStatus updatedStatus =
                  cluster.getStatus().appendNewState(invocation.getArgument(1));
              cluster.setStatus(updatedStatus);
              return true;
            })
        .when(mockRecorder)
        .appendNewStateAndPersist(any(SparkClusterContext.class), any(ClusterState.class));
  }

  @SuppressWarnings("PMD.UnusedLocalVariable")
  @Test
  void testCleanupRunningCluster() {
    try (MockedConstruction<SparkClusterContext> mockClusterContext =
        mockConstruction(
            SparkClusterContext.class,
            (mock, context) -> {
              when(mock.getResource()).thenReturn(cluster);
              when(mock.getClient()).thenReturn(mockClient);
            })) {
      // delete running cluster
      cluster.setStatus(cluster.getStatus().appendNewState(
          new ClusterState(ClusterStateSummary.RunningHealthy, "")));
      DeleteControl deleteControl = reconciler.cleanup(cluster, mockContext);
      // Cluster in RunningHealthy state - cleanup completes and allows deletion
      assertTrue(deleteControl.isRemoveFinalizer());
    }
  }

  @SuppressWarnings("PMD.UnusedLocalVariable")
  @Test
  void testCleanupClusterResourceReleased() {
    try (MockedConstruction<SparkClusterContext> mockClusterContext =
        mockConstruction(
            SparkClusterContext.class,
            (mock, context) -> {
              when(mock.getResource()).thenReturn(cluster);
              when(mock.getClient()).thenReturn(mockClient);
            })) {
      // delete cluster that has already released resources
      cluster.setStatus(cluster.getStatus().appendNewState(
          new ClusterState(ClusterStateSummary.ResourceReleased, "")));
      DeleteControl deleteControl = reconciler.cleanup(cluster, mockContext);
      assertTrue(deleteControl.isRemoveFinalizer());
    }
  }

  @SuppressWarnings("PMD.UnusedLocalVariable")
  @Test
  void testCleanupFailedCluster() {
    try (MockedConstruction<SparkClusterContext> mockClusterContext =
        mockConstruction(
            SparkClusterContext.class,
            (mock, context) -> {
              when(mock.getResource()).thenReturn(cluster);
              when(mock.getClient()).thenReturn(mockClient);
            })) {
      // delete failed cluster
      cluster.setStatus(
          cluster.getStatus().appendNewState(new ClusterState(ClusterStateSummary.Failed, "")));
      DeleteControl deleteControl = reconciler.cleanup(cluster, mockContext);
      // Failed cluster cleanup completes and allows deletion
      assertTrue(deleteControl.isRemoveFinalizer());
    }
  }

  @Test
  void testGetReconcileStepsForSubmittedCluster() {
    // Submitted state should include ClusterInitStep
    cluster.setStatus(
        cluster.getStatus().appendNewState(new ClusterState(ClusterStateSummary.Submitted, "")));
    List<ClusterReconcileStep> steps = reconciler.getReconcileSteps(cluster);
    assertEquals(3, steps.size());
    assertEquals(
        "ClusterValidateStep", steps.get(0).getClass().getSimpleName());
    assertEquals(
        "ClusterTerminatedStep", steps.get(1).getClass().getSimpleName());
    assertEquals(
        "ClusterInitStep", steps.get(2).getClass().getSimpleName());
  }

  @Test
  void testGetReconcileStepsForRunningHealthyCluster() {
    // RunningHealthy state should include ClusterSuspendStep to observe spec.suspend
    cluster.setStatus(cluster.getStatus().appendNewState(
        new ClusterState(ClusterStateSummary.RunningHealthy, "")));
    List<ClusterReconcileStep> steps = reconciler.getReconcileSteps(cluster);
    assertEquals(3, steps.size());
    assertEquals(
        "ClusterValidateStep", steps.get(0).getClass().getSimpleName());
    assertEquals(
        "ClusterTerminatedStep", steps.get(1).getClass().getSimpleName());
    assertEquals(
        "ClusterSuspendStep", steps.get(2).getClass().getSimpleName());
  }

  @Test
  void testGetReconcileStepsForSuspendedCluster() {
    // Suspended state should include ClusterSuspendStep to release resources or resume
    cluster.setStatus(cluster.getStatus().appendNewState(
        new ClusterState(ClusterStateSummary.Suspended, "")));
    List<ClusterReconcileStep> steps = reconciler.getReconcileSteps(cluster);
    assertEquals(3, steps.size());
    assertEquals(
        "ClusterValidateStep", steps.get(0).getClass().getSimpleName());
    assertEquals(
        "ClusterTerminatedStep", steps.get(1).getClass().getSimpleName());
    assertEquals(
        "ClusterSuspendStep", steps.get(2).getClass().getSimpleName());
  }

  @Test
  void testGetReconcileStepsForFailedCluster() {
    // Failed state should include ClusterUnknownStateStep
    cluster.setStatus(
        cluster.getStatus().appendNewState(new ClusterState(ClusterStateSummary.Failed, "")));
    List<ClusterReconcileStep> steps = reconciler.getReconcileSteps(cluster);
    assertEquals(3, steps.size());
    assertEquals(
        "ClusterValidateStep", steps.get(0).getClass().getSimpleName());
    assertEquals(
        "ClusterTerminatedStep", steps.get(1).getClass().getSimpleName());
    assertEquals(
        "ClusterUnknownStateStep", steps.get(2).getClass().getSimpleName());
  }

  @Test
  void testGetReconcileStepsForResourceReleasedCluster() {
    // ResourceReleased state should include ClusterUnknownStateStep
    cluster.setStatus(cluster.getStatus().appendNewState(
        new ClusterState(ClusterStateSummary.ResourceReleased, "")));
    List<ClusterReconcileStep> steps = reconciler.getReconcileSteps(cluster);
    assertEquals(3, steps.size());
    assertEquals(
        "ClusterValidateStep", steps.get(0).getClass().getSimpleName());
    assertEquals(
        "ClusterTerminatedStep", steps.get(1).getClass().getSimpleName());
    assertEquals(
        "ClusterUnknownStateStep", steps.get(2).getClass().getSimpleName());
  }

  @SuppressWarnings("PMD.UnusedLocalVariable")
  @Test
  void testReconcileFailedClusterDoesNotLoop() throws Exception {
    try (MockedConstruction<SparkClusterContext> mockClusterContext =
        mockConstruction(
            SparkClusterContext.class,
            (mock, context) -> {
              when(mock.getResource()).thenReturn(cluster);
              when(mock.getClient()).thenReturn(mockClient);
            })) {
      cluster.setStatus(cluster.getStatus().appendNewState(
          new ClusterState(ClusterStateSummary.SchedulingFailure, "")));

      // SchedulingFailure moves to Failed once.
      reconciler.reconcile(cluster, mockContext);
      assertEquals(
          ClusterStateSummary.Failed,
          cluster.getStatus().getCurrentState().getCurrentStateSummary());
      int historySize = cluster.getStatus().getStateTransitionHistory().size();
      verify(mockRecorder, times(1)).persistStatus(any(), any());

      // A Failed cluster neither appends a new state nor requeues immediately.
      for (int i = 0; i < 3; i++) {
        var control = reconciler.reconcile(cluster, mockContext);
        assertTrue(control.getScheduleDelay().isEmpty());
      }
      assertEquals(historySize, cluster.getStatus().getStateTransitionHistory().size());
      verify(mockRecorder, times(1)).persistStatus(any(), any());
    }
  }

  @Test
  void updateErrorStatusWarnsWithReconcileError() {
    when(mockContext.eventRecorder()).thenReturn(mockEventRecorder);
    var failure = new RuntimeException("request failed", new IllegalStateException("bad config"));

    var control = reconciler.updateErrorStatus(cluster, mockContext, failure);

    assertThat(control.getResource()).isEmpty();
    var event = captureRecordedEvent();
    assertThat(event.type()).isEqualTo(EventType.WARNING);
    assertThat(event.reason()).isEqualTo(EventUtils.REASON_RECONCILE_ERROR);
    // The full prefix, so a kind copy-paste from the app reconciler would fail here.
    assertThat(event.message())
        .startsWith("Spark Cluster Reconciliation failed.")
        .contains("RuntimeException: request failed")
        .contains("caused by: IllegalStateException: bad config");
  }

  @Test
  void updateErrorStatusPublishesOnlyOnTheFirstAttempt() {
    when(mockContext.eventRecorder()).thenReturn(mockEventRecorder);
    var retryInfo = mock(RetryInfo.class);
    when(retryInfo.getAttemptCount()).thenReturn(1);
    when(mockContext.getRetryInfo()).thenReturn(Optional.of(retryInfo));

    reconciler.updateErrorStatus(cluster, mockContext, new RuntimeException("boom"));

    verify(mockEventRecorder, never()).record(any(EventRecord.class));
  }

  @Test
  void cleanupFailureWarnsWithCleanupError() {
    when(mockContext.eventRecorder()).thenReturn(mockEventRecorder);
    var failure = new IllegalStateException("cannot build spec");
    try (MockedConstruction<SparkClusterContext> ignored =
        mockConstruction(
            SparkClusterContext.class,
            (mock, ctx) -> when(mock.getResource()).thenThrow(failure))) {

      assertThatThrownBy(() -> reconciler.cleanup(cluster, mockContext)).isSameAs(failure);
    }

    var event = captureRecordedEvent();
    assertThat(event.reason()).isEqualTo(EventUtils.REASON_CLEANUP_ERROR);
    assertThat(event.message())
        .startsWith("Spark Cluster Cleanup failed, the resource cannot finish deleting.")
        .contains("IllegalStateException: cannot build spec");
  }

  private EventRecord captureRecordedEvent() {
    ArgumentCaptor<EventRecord> captor = ArgumentCaptor.forClass(EventRecord.class);
    verify(mockEventRecorder, times(1)).record(captor.capture());
    return captor.getValue();
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void podInformerMapsPodsBySparkClusterNameLabel() {
    EventSourceContext<SparkCluster> eventSourceContext = mock(EventSourceContext.class);
    List<InformerEventSourceConfiguration<?>> configs = new ArrayList<>();
    try (MockedConstruction<InformerEventSource> ignored =
        mockConstruction(
            InformerEventSource.class,
            (mock, ctx) ->
                configs.add((InformerEventSourceConfiguration<?>) ctx.arguments().get(0)))) {
      reconciler.prepareEventSources(eventSourceContext);
      InformerEventSourceConfiguration<Pod> podConfig =
          (InformerEventSourceConfiguration<Pod>) configs.get(0);
      assertEquals(LABEL_SPARK_CLUSTER_NAME, podConfig.getInformerConfig().getLabelSelector());
      Pod pod = new Pod();
      pod.setMetadata(
          new ObjectMetaBuilder()
              .withName("cluster-1-master-0")
              .withNamespace("default")
              .withLabels(Map.of(LABEL_SPARK_CLUSTER_NAME, "cluster-1"))
              .build());
      assertEquals(
          Set.of(new ResourceID("cluster-1", "default")),
          podConfig.getSecondaryToPrimaryMapper().toPrimaryResourceIDs(pod));
    }
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  void kueueWorkloadInformerIsRegisteredOnlyWhenEnabled() {
    EventSourceContext<SparkCluster> eventSourceContext = mock(EventSourceContext.class);
    List<InformerEventSourceConfiguration<?>> configs = new ArrayList<>();
    try (MockedConstruction<InformerEventSource> ignored =
        mockConstruction(
            InformerEventSource.class,
            (mock, ctx) ->
                configs.add((InformerEventSourceConfiguration<?>) ctx.arguments().get(0)))) {
      assertEquals(1, reconciler.prepareEventSources(eventSourceContext).size());
      assertEquals(List.of(Pod.class), configs.stream().map(c -> c.getResourceClass()).toList());

      configs.clear();
      setConfigKey(SparkOperatorConf.KUEUE_ENABLED, true);
      assertEquals(2, reconciler.prepareEventSources(eventSourceContext).size());
      assertEquals(
          List.of(Pod.class, Workload.class),
          configs.stream().map(c -> c.getResourceClass()).toList());

      InformerEventSourceConfiguration<Workload> workloadConfig =
          (InformerEventSourceConfiguration<Workload>) configs.get(1);
      assertEquals(LABEL_SPARK_CLUSTER_NAME, workloadConfig.getInformerConfig().getLabelSelector());
      Workload workload = new Workload();
      workload.setMetadata(
          new ObjectMetaBuilder()
              .withName("workload-1")
              .withNamespace("default")
              .withLabels(Map.of(LABEL_SPARK_CLUSTER_NAME, "cluster-1"))
              .build());
      assertEquals(
          Set.of(new ResourceID("cluster-1", "default")),
          workloadConfig.getSecondaryToPrimaryMapper().toPrimaryResourceIDs(workload));

      // Only an admission change or a deletion of a Workload triggers a reconciliation
      InformerConfiguration<Workload> informerConfig = workloadConfig.getInformerConfig();
      Workload admitted = new Workload();
      admitted.setMetadata(workload.getMetadata());
      admitted.setStatus(
          WorkloadStatus.builder()
              .conditions(
                  List.of(new ConditionBuilder().withType("Admitted").withStatus("True").build()))
              .build());
      assertFalse(informerConfig.getOnAddFilter().accept(workload));
      assertTrue(informerConfig.getOnUpdateFilter().accept(admitted, workload));
      assertFalse(informerConfig.getOnUpdateFilter().accept(workload, workload));
      assertNull(informerConfig.getOnDeleteFilter());
    } finally {
      setConfigKey(SparkOperatorConf.KUEUE_ENABLED, false);
    }
  }
}
