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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import java.util.List;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.SparkClusterSubmissionWorker;
import org.apache.spark.k8s.operator.context.SparkClusterContext;
import org.apache.spark.k8s.operator.metrics.healthcheck.SentinelManager;
import org.apache.spark.k8s.operator.reconciler.reconcilesteps.ClusterReconcileStep;
import org.apache.spark.k8s.operator.status.ClusterState;
import org.apache.spark.k8s.operator.status.ClusterStateSummary;
import org.apache.spark.k8s.operator.status.ClusterStatus;
import org.apache.spark.k8s.operator.utils.SparkClusterStatusRecorder;

class SparkClusterReconcilerTest {
  private final SparkClusterStatusRecorder mockRecorder = mock(SparkClusterStatusRecorder.class);
  private final SentinelManager<SparkCluster> mockSentinelManager = mock(SentinelManager.class);
  private final KubernetesClient mockClient = mock(KubernetesClient.class);
  private final Context<SparkCluster> mockContext = mock(Context.class);
  private final SparkClusterSubmissionWorker mockWorker = mock(SparkClusterSubmissionWorker.class);
  SparkCluster cluster = new SparkCluster();
  SparkClusterReconciler reconciler =
      new SparkClusterReconciler(mockWorker, mockRecorder, mockSentinelManager);

  @BeforeEach
  public void beforeEach() {
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
              return null;
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
    // RunningHealthy state should not have additional steps beyond validation and termination check
    cluster.setStatus(cluster.getStatus().appendNewState(
        new ClusterState(ClusterStateSummary.RunningHealthy, "")));
    List<ClusterReconcileStep> steps = reconciler.getReconcileSteps(cluster);
    assertEquals(2, steps.size());
    assertEquals(
        "ClusterValidateStep", steps.get(0).getClass().getSimpleName());
    assertEquals(
        "ClusterTerminatedStep", steps.get(1).getClass().getSimpleName());
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
}
