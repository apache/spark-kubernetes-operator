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

import static org.apache.spark.k8s.operator.utils.TestUtils.setConfigKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.config.SparkOperatorConf;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.status.ExecutorState;
import org.apache.spark.k8s.operator.utils.DeletedExecutorPods;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;
import org.apache.spark.k8s.operator.utils.TestUtils;

class AppExecutorStateStepTest {
  private SparkAppContext mockContext;
  private SparkAppStatusRecorder mockRecorder;
  private SparkApplication app;
  private DeletedExecutorPods deletedExecutorPods;
  private AppExecutorStateStep step;

  @BeforeEach
  void setUp() {
    // The executor states are off by default; most tests here check them
    setConfigKey(SparkOperatorConf.EXECUTOR_STATE_ENABLED, true);
    mockContext = mock(SparkAppContext.class);
    mockRecorder = mock(SparkAppStatusRecorder.class);
    app = TestUtils.createMockApp("default");
    app.getSpec().getSparkConf().put("spark.dynamicAllocation.enabled", "true");
    app.setStatus(
        new ApplicationStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.RunningHealthy, "")));
    deletedExecutorPods = new DeletedExecutorPods();
    step = new AppExecutorStateStep(deletedExecutorPods);
    when(mockContext.getResource()).thenReturn(app);
    when(mockRecorder.persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class)))
        .thenAnswer(
            invocation -> {
              app.setStatus(invocation.getArgument(1));
              return true;
            });
  }

  @AfterEach
  void tearDown() {
    setConfigKey(SparkOperatorConf.EXECUTOR_STATE_ENABLED, false);
  }

  private static Pod executorPod(String name, String phase) {
    return new PodBuilder()
        .withNewMetadata()
        .withName(name)
        .withNamespace("default")
        .addToLabels(Constants.LABEL_SPARK_APPLICATION_NAME, "sentinel")
        .addToLabels(Constants.LABEL_SPARK_ROLE_NAME, Constants.LABEL_SPARK_ROLE_EXECUTOR_VALUE)
        .addToLabels(Constants.LABEL_SPARK_APP_ID, "sentinel-0")
        .endMetadata()
        .withNewStatus()
        .withPhase(phase)
        .endStatus()
        .build();
  }

  @Test
  void recordsAChangedExecutorState() {
    when(mockContext.getExecutorsForApplication())
        .thenReturn(Set.of(executorPod("app-exec-1", "Running")));

    assertEquals(ReconcileProgress.proceed(), step.reconcile(mockContext, mockRecorder));

    assertEquals(Map.of("app-exec-1", ExecutorState.RUNNING), app.getStatus().getExecutorState());
    assertEquals(
        ApplicationStateSummary.RunningHealthy,
        app.getStatus().getCurrentState().getCurrentStateSummary());
  }

  @Test
  void skipsTheStatusUpdateWhenNothingChanged() {
    app.setStatus(
        app.getStatus()
            .withExecutorState(Map.of("app-exec-1", ExecutorState.RUNNING))
            .withExecutorCounts(1, 0, 1L));
    when(mockContext.getExecutorsForApplication())
        .thenReturn(Set.of(executorPod("app-exec-1", "Running")));

    assertEquals(ReconcileProgress.proceed(), step.reconcile(mockContext, mockRecorder));

    verify(mockRecorder, never())
        .persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class));
  }

  @Test
  void skipsASuspendedApplication() {
    app.setStatus(
        app.getStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.Suspended, "")));
    when(mockContext.getExecutorsForApplication())
        .thenReturn(Set.of(executorPod("app-exec-1", "Running")));

    assertEquals(ReconcileProgress.proceed(), step.reconcile(mockContext, mockRecorder));

    verify(mockRecorder, never())
        .persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class));
    assertEquals(Map.of(), app.getStatus().getExecutorState());
  }

  @Test
  void requeuesWhenTheStatusUpdateFails() {
    when(mockRecorder.persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class)))
        .thenReturn(false);
    when(mockContext.getExecutorsForApplication())
        .thenReturn(Set.of(executorPod("app-exec-1", "Pending")));

    assertEquals(
        ReconcileProgress.completeAndImmediateRequeue(),
        step.reconcile(mockContext, mockRecorder));
  }

  @Test
  void recordsAndForgetsTheFinalStateOfADeletedExecutor() {
    app.setStatus(app.getStatus().withExecutorState(Map.of("app-exec-1", ExecutorState.RUNNING)));
    deletedExecutorPods.record(executorPod("app-exec-1", "Succeeded"));
    when(mockContext.getExecutorsForApplication()).thenReturn(Set.of());

    assertEquals(ReconcileProgress.proceed(), step.reconcile(mockContext, mockRecorder));

    assertEquals(
        Map.of("app-exec-1", ExecutorState.COMPLETED), app.getStatus().getExecutorState());
    assertTrue(deletedExecutorPods.get(ResourceID.fromResource(app)).isEmpty());
  }

  @Test
  void keepsADeletedExecutorWhenTheStatusUpdateFails() {
    when(mockRecorder.persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class)))
        .thenReturn(false);
    deletedExecutorPods.record(executorPod("app-exec-1", "Failed"));
    when(mockContext.getExecutorsForApplication()).thenReturn(Set.of());

    assertEquals(
        ReconcileProgress.completeAndImmediateRequeue(),
        step.reconcile(mockContext, mockRecorder));

    assertEquals(1, deletedExecutorPods.get(ResourceID.fromResource(app)).size());
  }

  @Test
  void forgetsTheDeletedExecutorsOfASuspendedApplication() {
    app.setStatus(
        app.getStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.Suspended, "")));
    deletedExecutorPods.record(executorPod("app-exec-1", "Succeeded"));

    assertEquals(ReconcileProgress.proceed(), step.reconcile(mockContext, mockRecorder));

    assertTrue(deletedExecutorPods.get(ResourceID.fromResource(app)).isEmpty());
    assertEquals(Map.of(), app.getStatus().getExecutorState());
  }

  private static Pod executorPod(String name, String executorId, String phase) {
    Pod pod = executorPod(name, phase);
    pod.getMetadata().getLabels().put(Constants.LABEL_SPARK_EXECUTOR_ID, executorId);
    return pod;
  }

  @Test
  void recordsTheActiveExecutorsAndTheHighestExecutorId() {
    when(mockContext.getExecutorsForApplication())
        .thenReturn(
            Set.of(
                executorPod("app-exec-1", "1", "Running"),
                executorPod("app-exec-2", "2", "Pending"),
                executorPod("app-exec-3", "3", "Succeeded")));

    assertEquals(ReconcileProgress.proceed(), step.reconcile(mockContext, mockRecorder));

    assertEquals(2, app.getStatus().getActiveExecutors());
    assertEquals(3L, app.getStatus().getMaxExecutorId());
  }

  private static Pod readyExecutorPod(String name, String executorId) {
    return new PodBuilder(executorPod(name, executorId, "Running"))
        .editStatus()
        .addNewCondition()
        .withType("Ready")
        .withStatus("True")
        .endCondition()
        .endStatus()
        .build();
  }

  @Test
  void recordsTheReadyExecutors() {
    Pod deleting = readyExecutorPod("app-exec-3", "3");
    deleting.getMetadata().setDeletionTimestamp("2026-11-06T00:00:00Z");
    when(mockContext.getExecutorsForApplication())
        .thenReturn(
            Set.of(
                readyExecutorPod("app-exec-1", "1"),
                executorPod("app-exec-2", "2", "Running"),
                deleting,
                executorPod("app-exec-4", "4", "Pending"),
                executorPod("app-exec-5", "5", "Succeeded")));

    step.reconcile(mockContext, mockRecorder);

    assertEquals(4, app.getStatus().getActiveExecutors());
    assertEquals(1, app.getStatus().getReadyExecutors());
  }

  @Test
  void writesTheStatusWhenOnlyTheReadyExecutorsChanged() {
    when(mockContext.getExecutorsForApplication())
        .thenReturn(Set.of(executorPod("app-exec-1", "1", "Running")));
    step.reconcile(mockContext, mockRecorder);
    assertEquals(0, app.getStatus().getReadyExecutors());

    when(mockContext.getExecutorsForApplication())
        .thenReturn(Set.of(readyExecutorPod("app-exec-1", "1")));
    step.reconcile(mockContext, mockRecorder);

    assertEquals(1, app.getStatus().getReadyExecutors());
    verify(mockRecorder, times(2))
        .persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class));
  }

  @Test
  void keepsTheHighestExecutorIdWhenItsExecutorIsGone() {
    when(mockContext.getExecutorsForApplication())
        .thenReturn(
            Set.of(
                executorPod("app-exec-1", "1", "Running"),
                executorPod("app-exec-5", "5", "Running")));
    step.reconcile(mockContext, mockRecorder);
    assertEquals(2, app.getStatus().getActiveExecutors());
    assertEquals(5L, app.getStatus().getMaxExecutorId());

    // Dynamic Allocation removed the newest executor, and later all of them
    when(mockContext.getExecutorsForApplication())
        .thenReturn(Set.of(executorPod("app-exec-1", "1", "Running")));
    step.reconcile(mockContext, mockRecorder);
    assertEquals(1, app.getStatus().getActiveExecutors());
    assertEquals(5L, app.getStatus().getMaxExecutorId());

    when(mockContext.getExecutorsForApplication()).thenReturn(Set.of());
    step.reconcile(mockContext, mockRecorder);
    assertEquals(0, app.getStatus().getActiveExecutors());
    assertEquals(5L, app.getStatus().getMaxExecutorId());
  }

  @Test
  void leavesAnApplicationWithoutExecutorsUnwritten() {
    when(mockContext.getExecutorsForApplication()).thenReturn(Set.of());

    assertEquals(ReconcileProgress.proceed(), step.reconcile(mockContext, mockRecorder));

    verify(mockRecorder, never())
        .persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class));
    assertNull(app.getStatus().getActiveExecutors());
    assertNull(app.getStatus().getReadyExecutors());
    assertNull(app.getStatus().getMaxExecutorId());
  }

  @Test
  void writesTheStatusWhenOnlyTheActiveExecutorsChanged() {
    app.setStatus(
        app.getStatus()
            .withExecutorState(Map.of("app-exec-1", ExecutorState.RUNNING))
            .withExecutorCounts(2, 0, 1L));
    when(mockContext.getExecutorsForApplication())
        .thenReturn(Set.of(executorPod("app-exec-1", "1", "Running")));

    step.reconcile(mockContext, mockRecorder);

    assertEquals(1, app.getStatus().getActiveExecutors());
    verify(mockRecorder).persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class));
  }

  @Test
  void countsEveryActiveAndReadyExecutorBeyondTheExecutorStateLimit() {
    setConfigKey(SparkOperatorConf.MAX_TRACKED_EXECUTOR_PER_APP, 2);
    try {
      when(mockContext.getExecutorsForApplication())
          .thenReturn(
              Set.of(
                  readyExecutorPod("app-exec-1", "1"),
                  readyExecutorPod("app-exec-2", "2"),
                  readyExecutorPod("app-exec-3", "3"),
                  executorPod("app-exec-4", "4", "Pending")));

      step.reconcile(mockContext, mockRecorder);

      assertEquals(2, app.getStatus().getExecutorState().size());
      assertEquals(4, app.getStatus().getActiveExecutors());
      assertEquals(3, app.getStatus().getReadyExecutors());
      assertEquals(4L, app.getStatus().getMaxExecutorId());
    } finally {
      setConfigKey(SparkOperatorConf.MAX_TRACKED_EXECUTOR_PER_APP, 1000);
    }
  }

  @Test
  void recordsOnlyTheCountAndTheIdWithoutExecutorStates() {
    setConfigKey(SparkOperatorConf.EXECUTOR_STATE_ENABLED, false);
    when(mockContext.getExecutorsForApplication())
        .thenReturn(Set.of(executorPod("app-exec-1", "1", "Pending")));
    step.reconcile(mockContext, mockRecorder);
    assertTrue(app.getStatus().getExecutorState().isEmpty());
    assertEquals(1, app.getStatus().getActiveExecutors());
    assertEquals(1L, app.getStatus().getMaxExecutorId());
    verify(mockRecorder).persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class));
    clearInvocations(mockRecorder);

    // The executor starts running: nothing the status records changed, so it is not written
    when(mockContext.getExecutorsForApplication())
        .thenReturn(Set.of(executorPod("app-exec-1", "1", "Running")));
    step.reconcile(mockContext, mockRecorder);
    verify(mockRecorder, never())
        .persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class));
  }

  @Test
  void clearsTheExecutorStatesOnceTheyAreDisabled() {
    app.setStatus(
        app.getStatus()
            .withExecutorState(Map.of("app-exec-1", ExecutorState.RUNNING))
            .withExecutorCounts(1, 0, 1L));
    when(mockContext.getExecutorsForApplication())
        .thenReturn(Set.of(executorPod("app-exec-1", "1", "Running")));
    setConfigKey(SparkOperatorConf.EXECUTOR_STATE_ENABLED, false);
    step.reconcile(mockContext, mockRecorder);
    assertTrue(app.getStatus().getExecutorState().isEmpty());
    assertEquals(1, app.getStatus().getActiveExecutors());
  }

  @Test
  void recordsNoExecutorCountsWhenTheyAreDisabled() {
    setConfigKey(SparkOperatorConf.EXECUTOR_COUNTS_ENABLED, false);
    try {
      when(mockContext.getExecutorsForApplication())
          .thenReturn(
              Set.of(
                  readyExecutorPod("app-exec-1", "1"), executorPod("app-exec-2", "2", "Pending")));

      step.reconcile(mockContext, mockRecorder);

      assertNull(app.getStatus().getActiveExecutors());
      assertNull(app.getStatus().getReadyExecutors());
      // The highest executor ID and the executor states have their own scope
      assertEquals(2L, app.getStatus().getMaxExecutorId());
      assertEquals(2, app.getStatus().getExecutorState().size());
    } finally {
      setConfigKey(SparkOperatorConf.EXECUTOR_COUNTS_ENABLED, true);
    }
  }

  @Test
  void clearsTheExecutorCountsOnceTheyAreDisabled() {
    app.setStatus(
        app.getStatus()
            .withExecutorState(Map.of("app-exec-1", ExecutorState.RUNNING))
            .withExecutorCounts(1, 1, 1L));
    setConfigKey(SparkOperatorConf.EXECUTOR_COUNTS_ENABLED, false);
    try {
      when(mockContext.getExecutorsForApplication())
          .thenReturn(Set.of(readyExecutorPod("app-exec-1", "1")));

      step.reconcile(mockContext, mockRecorder);

      verify(mockRecorder).persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class));
      assertNull(app.getStatus().getActiveExecutors());
      assertNull(app.getStatus().getReadyExecutors());
      assertEquals(1L, app.getStatus().getMaxExecutorId());

      // Once cleared, nothing is written again while the executors are unchanged
      clearInvocations(mockRecorder);
      step.reconcile(mockContext, mockRecorder);
      verify(mockRecorder, never())
          .persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class));
    } finally {
      setConfigKey(SparkOperatorConf.EXECUTOR_COUNTS_ENABLED, true);
    }
  }

  @Test
  void writesNothingWithoutDynamicAllocationWhenTheCountsAreDisabled() {
    app.getSpec().getSparkConf().put("spark.dynamicAllocation.enabled", "false");
    setConfigKey(SparkOperatorConf.EXECUTOR_COUNTS_ENABLED, false);
    try {
      when(mockContext.getExecutorsForApplication())
          .thenReturn(Set.of(readyExecutorPod("app-exec-1", "1")));

      step.reconcile(mockContext, mockRecorder);

      verify(mockRecorder, never())
          .persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class));
      assertNull(app.getStatus().getActiveExecutors());
    } finally {
      setConfigKey(SparkOperatorConf.EXECUTOR_COUNTS_ENABLED, true);
    }
  }

  @Test
  void recordsOnlyTheExecutorCountsWithoutDynamicAllocation() {
    app.getSpec().getSparkConf().put("spark.dynamicAllocation.enabled", "false");
    when(mockContext.getExecutorsForApplication())
        .thenReturn(
            Set.of(readyExecutorPod("app-exec-1", "1"), executorPod("app-exec-3", "3", "Pending")));
    Pod deleted = executorPod("app-exec-2", "2", "Succeeded");
    deleted.getMetadata().getLabels().put(Constants.LABEL_SPARK_APPLICATION_NAME, "sentinel");
    deletedExecutorPods.record(deleted);

    assertEquals(ReconcileProgress.proceed(), step.reconcile(mockContext, mockRecorder));

    assertEquals(2, app.getStatus().getActiveExecutors());
    assertEquals(1, app.getStatus().getReadyExecutors());
    assertTrue(app.getStatus().getExecutorState().isEmpty());
    assertNull(app.getStatus().getMaxExecutorId());
    // The deleted executors of the application are not kept in memory
    assertTrue(deletedExecutorPods.get(ResourceID.fromResource(app)).isEmpty());

    // An unchanged count is not written again
    clearInvocations(mockRecorder);
    step.reconcile(mockContext, mockRecorder);
    verify(mockRecorder, never())
        .persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class));
  }

  @Test
  void leavesAnApplicationWithoutDynamicAllocationAndExecutorsUnwritten() {
    app.getSpec().getSparkConf().put("spark.dynamicAllocation.enabled", "false");
    when(mockContext.getExecutorsForApplication()).thenReturn(Set.of());

    step.reconcile(mockContext, mockRecorder);

    verify(mockRecorder, never())
        .persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class));
    assertNull(app.getStatus().getActiveExecutors());
    assertNull(app.getStatus().getReadyExecutors());
  }
}
