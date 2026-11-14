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

package org.apache.spark.k8s.operator.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.SortedMap;
import java.util.TreeMap;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.spec.RestartConfig;
import org.apache.spark.k8s.operator.spec.RestartPolicy;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.status.ExecutorState;

class ExecutorStateUtilsTest {

  private SparkApplication app;

  @BeforeEach
  void setUp() {
    app = TestUtils.createMockApp("default");
    app.setStatus(
        new ApplicationStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.RunningHealthy, "")));
  }

  private static Pod executorPod(String name, String appId, String executorId, String phase) {
    PodBuilder builder =
        new PodBuilder()
            .withNewMetadata()
            .withName(name)
            .addToLabels(Constants.LABEL_SPARK_ROLE_NAME, Constants.LABEL_SPARK_ROLE_EXECUTOR_VALUE)
            .addToLabels(Constants.LABEL_SPARK_APP_ID, appId)
            .endMetadata();
    if (executorId != null) {
      builder
          .editMetadata()
          .addToLabels(Constants.LABEL_SPARK_EXECUTOR_ID, executorId)
          .endMetadata();
    }
    if (phase != null) {
      builder.withNewStatus().withPhase(phase).endStatus();
    }
    return builder.build();
  }

  private Pod currentExecutorPod(String name, String executorId, String phase) {
    return executorPod(name, ExecutorStateUtils.currentAttemptAppId(app), executorId, phase);
  }

  private void setState(ApplicationStateSummary state, Map<String, ExecutorState> executors) {
    app.setStatus(
        app.getStatus()
            .appendNewState(new ApplicationState(state, ""))
            .withExecutorState(executors));
  }

  @Test
  void fromPodPhaseMapsEachPhase() {
    assertEquals(ExecutorState.PENDING,
        ExecutorStateUtils.fromPodPhase(executorPod("e", "a", "1", "Pending")));
    assertEquals(ExecutorState.RUNNING,
        ExecutorStateUtils.fromPodPhase(executorPod("e", "a", "1", "Running")));
    assertEquals(ExecutorState.COMPLETED,
        ExecutorStateUtils.fromPodPhase(executorPod("e", "a", "1", "Succeeded")));
    assertEquals(ExecutorState.FAILED,
        ExecutorStateUtils.fromPodPhase(executorPod("e", "a", "1", "Failed")));
    assertEquals(ExecutorState.UNKNOWN,
        ExecutorStateUtils.fromPodPhase(executorPod("e", "a", "1", "Unknown")));
    assertEquals(ExecutorState.UNKNOWN,
        ExecutorStateUtils.fromPodPhase(executorPod("e", "a", "1", null)));
  }

  @Test
  void currentAttemptAppIdFollowsTheAttempt() {
    assertEquals("sentinel-0", ExecutorStateUtils.currentAttemptAppId(app));
    RestartConfig restartConfig = new RestartConfig();
    restartConfig.setRestartPolicy(RestartPolicy.Always);
    restartConfig.setMaxRestartAttempts(1L);
    app.setStatus(
        app.getStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.Failed, ""))
            .terminateOrRestart(restartConfig, "", false));
    assertEquals("sentinel-1", ExecutorStateUtils.currentAttemptAppId(app));
    app.getSpec().getSparkConf().put("spark.app.id", "configured");
    assertEquals("configured", ExecutorStateUtils.currentAttemptAppId(app));
  }

  @Test
  void observeRecordsTheExecutorsOfTheCurrentAttempt() {
    SortedMap<String, ExecutorState> executorState =
        ExecutorStateUtils.observe(
            app,
            List.of(
                currentExecutorPod("app-exec-1", "1", "Running"),
                currentExecutorPod("app-exec-2", "2", "Pending"),
                executorPod("old-exec-1", "sentinel-previous", "1", "Running")),
            1000);
    assertEquals(
        Map.of("app-exec-1", ExecutorState.RUNNING, "app-exec-2", ExecutorState.PENDING),
        executorState);
  }

  @Test
  void observeMatchesAConfiguredAppId() {
    app.getSpec().getSparkConf().put("spark.app.id", "configured");
    SortedMap<String, ExecutorState> executorState =
        ExecutorStateUtils.observe(
            app,
            List.of(
                executorPod("app-exec-1", "configured", "1", "Running"),
                executorPod("app-exec-2", "sentinel-0", "2", "Running")),
            1000);
    assertEquals(Map.of("app-exec-1", ExecutorState.RUNNING), executorState);
  }

  @Test
  void observeKeepsRecordingNewExecutorsOfALongRunningApplication() {
    // The ids of a long-running application pass the limit, unlike with an id-based limit
    setState(
        ApplicationStateSummary.RunningHealthy,
        Map.of(
            "app-exec-1", ExecutorState.COMPLETED,
            "app-exec-2", ExecutorState.FAILED,
            "app-exec-3", ExecutorState.RUNNING));
    assertEquals(
        Map.of(
            "app-exec-2", ExecutorState.FAILED,
            "app-exec-3", ExecutorState.RUNNING,
            "app-exec-1001", ExecutorState.PENDING),
        ExecutorStateUtils.observe(
            app,
            List.of(
                currentExecutorPod("app-exec-3", "3", "Running"),
                currentExecutorPod("app-exec-1001", "1001", "Pending")),
            3));
  }

  @Test
  void observeIsStableAcrossReconciliationsAtTheLimit() {
    List<Pod> pods =
        List.of(
            currentExecutorPod("app-exec-1", "1", "Running"),
            currentExecutorPod("app-exec-2", "2", "Running"),
            currentExecutorPod("app-exec-3", "3", "Running"));
    SortedMap<String, ExecutorState> first = ExecutorStateUtils.observe(app, pods, 2);
    setState(ApplicationStateSummary.RunningHealthy, first);
    assertEquals(first, ExecutorStateUtils.observe(app, pods, 2));
    assertEquals(List.of("app-exec-1", "app-exec-2"), List.copyOf(first.keySet()));
  }

  @Test
  void enforceCapEvictsTheExecutorsWhichAreGoneOldestFirst() {
    SortedMap<String, ExecutorState> executorState =
        new TreeMap<>(
            Map.of(
                "app-exec-9", ExecutorState.COMPLETED,
                "app-exec-10", ExecutorState.UNKNOWN,
                "app-exec-11", ExecutorState.FAILED,
                "app-exec-12", ExecutorState.RUNNING,
                "app-exec-13", ExecutorState.PENDING));
    ExecutorStateUtils.enforceCap(app, executorState, 3);
    // By executor id, so app-exec-9 is older than app-exec-10, unlike by name
    assertEquals(
        Map.of(
            "app-exec-11", ExecutorState.FAILED,
            "app-exec-12", ExecutorState.RUNNING,
            "app-exec-13", ExecutorState.PENDING),
        executorState);
  }

  @Test
  void enforceCapDropsTheNewestLiveExecutorsWhenTheyExceedTheLimit() {
    SortedMap<String, ExecutorState> executorState =
        new TreeMap<>(
            Map.of(
                "app-exec-1", ExecutorState.RUNNING,
                "app-exec-2", ExecutorState.RUNNING,
                "app-exec-3", ExecutorState.PENDING,
                "app-exec-4", ExecutorState.COMPLETED));
    ExecutorStateUtils.enforceCap(app, executorState, 2);
    assertEquals(
        Map.of("app-exec-1", ExecutorState.RUNNING, "app-exec-2", ExecutorState.RUNNING),
        executorState);
  }

  @Test
  void enforceCapKeepsTheLiveExecutorsWhenEvictingTheGoneOnesIsEnough() {
    SortedMap<String, ExecutorState> executorState =
        new TreeMap<>(
            Map.of(
                "app-exec-1", ExecutorState.RUNNING,
                "app-exec-2", ExecutorState.PENDING,
                "app-exec-3", ExecutorState.COMPLETED));
    ExecutorStateUtils.enforceCap(app, executorState, 2);
    assertEquals(
        Map.of("app-exec-1", ExecutorState.RUNNING, "app-exec-2", ExecutorState.PENDING),
        executorState);
  }

  @Test
  void enforceCapBreaksTiesByName() {
    SortedMap<String, ExecutorState> executorState =
        new TreeMap<>(Map.of("exec-b", ExecutorState.COMPLETED, "exec-a", ExecutorState.FAILED));
    ExecutorStateUtils.enforceCap(app, executorState, 1);
    assertEquals(Map.of("exec-b", ExecutorState.COMPLETED), executorState);
  }

  @Test
  void enforceCapDoesNothingWithinOrWithoutALimit() {
    Map<String, ExecutorState> executors =
        Map.of("app-exec-1", ExecutorState.COMPLETED, "app-exec-2", ExecutorState.RUNNING);
    for (int limit : new int[] {2, 0, -1}) {
      SortedMap<String, ExecutorState> executorState = new TreeMap<>(executors);
      ExecutorStateUtils.enforceCap(app, executorState, limit);
      assertEquals(executors, executorState);
    }
  }

  @Test
  void executorIdFromPodNameReadsTheTrailingDigits() {
    assertEquals(12L, ExecutorStateUtils.executorIdFromPodName("pi-0-exec-12"));
    assertEquals(0L, ExecutorStateUtils.executorIdFromPodName("exec-a"));
    assertEquals(0L, ExecutorStateUtils.executorIdFromPodName(""));
    assertEquals(
        Long.MAX_VALUE, ExecutorStateUtils.executorIdFromPodName("exec-99999999999999999999"));
  }

  @Test
  void observeRecordsTheFinalStateOfADeletedExecutorRatherThanUnknown() {
    setState(
        ApplicationStateSummary.RunningHealthy,
        Map.of("app-exec-1", ExecutorState.RUNNING, "app-exec-2", ExecutorState.PENDING));
    String appId = ExecutorStateUtils.currentAttemptAppId(app);
    assertEquals(
        Map.of(
            "app-exec-1", ExecutorState.COMPLETED,
            "app-exec-2", ExecutorState.FAILED,
            // Created and deleted between two reconciliations
            "app-exec-3", ExecutorState.FAILED),
        ExecutorStateUtils.observe(
            app,
            List.of(),
            List.of(
                new DeletedExecutorPods.DeletedExecutor(
                    "app-exec-1", appId, ExecutorState.COMPLETED),
                new DeletedExecutorPods.DeletedExecutor("app-exec-2", appId, ExecutorState.FAILED),
                new DeletedExecutorPods.DeletedExecutor(
                    "app-exec-3", appId, ExecutorState.FAILED),
                new DeletedExecutorPods.DeletedExecutor(
                    "old-exec-1", "sentinel-previous", ExecutorState.FAILED)),
            1000));
  }

  @Test
  void observeKeepsAFailedDeletedExecutorAfterTheApplicationSucceeded() {
    // Without the final state of its pod, a missing executor is COMPLETED after the success
    setState(ApplicationStateSummary.Succeeded, Map.of("app-exec-1", ExecutorState.PENDING));
    assertEquals(
        Map.of("app-exec-1", ExecutorState.FAILED),
        ExecutorStateUtils.observe(
            app,
            List.of(),
            List.of(
                new DeletedExecutorPods.DeletedExecutor(
                    "app-exec-1",
                    ExecutorStateUtils.currentAttemptAppId(app),
                    ExecutorState.FAILED)),
            1000));
  }

  @Test
  void observeKeepsAMissingExecutorAsUnknownWhileTheDriverRuns() {
    setState(
        ApplicationStateSummary.RunningHealthy,
        Map.of(
            "app-exec-1", ExecutorState.RUNNING,
            "app-exec-2", ExecutorState.PENDING,
            "app-exec-3", ExecutorState.FAILED,
            "app-exec-4", ExecutorState.COMPLETED));
    assertEquals(
        Map.of(
            "app-exec-1", ExecutorState.UNKNOWN,
            "app-exec-2", ExecutorState.UNKNOWN,
            "app-exec-3", ExecutorState.FAILED,
            "app-exec-4", ExecutorState.COMPLETED),
        ExecutorStateUtils.observe(app, List.of(), 1000));
  }

  @Test
  void observeCompletesAMissingExecutorAfterTheApplicationSucceeded() {
    setState(ApplicationStateSummary.Succeeded, Map.of("app-exec-1", ExecutorState.RUNNING));
    assertEquals(
        Map.of("app-exec-1", ExecutorState.COMPLETED),
        ExecutorStateUtils.observe(app, List.of(), 1000));

    RestartConfig noRetryConfig = new RestartConfig();
    noRetryConfig.setRestartPolicy(RestartPolicy.Never);
    app.setStatus(app.getStatus().terminateOrRestart(noRetryConfig, "", false));
    assertEquals(
        ApplicationStateSummary.ResourceReleased,
        app.getStatus().getCurrentState().getCurrentStateSummary());
    assertEquals(
        Map.of("app-exec-1", ExecutorState.COMPLETED),
        ExecutorStateUtils.observe(app, List.of(), 1000));
  }

  @Test
  void observeFailsAMissingExecutorOtherwise() {
    setState(ApplicationStateSummary.Failed, Map.of("app-exec-1", ExecutorState.UNKNOWN));
    assertEquals(
        Map.of("app-exec-1", ExecutorState.FAILED),
        ExecutorStateUtils.observe(app, List.of(), 1000));
  }

  @Test
  void observeUpdatesARecordedExecutorWhosePodIsPresent() {
    setState(ApplicationStateSummary.RunningHealthy, Map.of("app-exec-1", ExecutorState.FAILED));
    assertEquals(
        Map.of("app-exec-1", ExecutorState.RUNNING),
        ExecutorStateUtils.observe(
            app, List.of(currentExecutorPod("app-exec-1", "1", "Running")), 1000));
  }

  @Test
  void observeDoesNotChangeTheStatus() {
    setState(ApplicationStateSummary.RunningHealthy, Map.of("app-exec-1", ExecutorState.RUNNING));
    ExecutorStateUtils.observe(
        app, List.of(currentExecutorPod("app-exec-2", "2", "Running")), 1000);
    assertEquals(Map.of("app-exec-1", ExecutorState.RUNNING), app.getStatus().getExecutorState());
  }

  @Test
  void hasSucceededLooksAtTheLatestStoppingState() {
    assertFalse(ExecutorStateUtils.hasSucceeded(app.getStatus()));
    setState(ApplicationStateSummary.Succeeded, Map.of());
    assertTrue(ExecutorStateUtils.hasSucceeded(app.getStatus()));
  }

  @Test
  void countsThePendingAndRunningExecutorPodsOfTheCurrentAttempt() {
    List<Pod> pods =
        List.of(
            currentExecutorPod("app-exec-1", "1", "Running"),
            currentExecutorPod("app-exec-2", "2", "Pending"),
            currentExecutorPod("app-exec-3", "3", "Succeeded"),
            currentExecutorPod("app-exec-4", "4", "Failed"),
            currentExecutorPod("app-exec-5", "5", "Unknown"),
            currentExecutorPod("app-exec-6", "6", null),
            executorPod("app-exec-7", "sentinel-previous", "7", "Running"));
    assertEquals(2, ExecutorStateUtils.countActiveExecutors(app, pods));
    assertEquals(0, ExecutorStateUtils.countActiveExecutors(app, List.of()));
  }

  private static Pod withReady(Pod pod, String ready) {
    return new PodBuilder(pod)
        .editStatus()
        .addNewCondition()
        .withType("Ready")
        .withStatus(ready)
        .endCondition()
        .endStatus()
        .build();
  }

  @Test
  void countsTheRunningAndReadyExecutorPodsOfTheCurrentAttempt() {
    Pod deleting = withReady(currentExecutorPod("app-exec-4", "4", "Running"), "True");
    deleting.getMetadata().setDeletionTimestamp("2026-11-06T00:00:00Z");
    List<Pod> pods =
        List.of(
            withReady(currentExecutorPod("app-exec-1", "1", "Running"), "True"),
            withReady(currentExecutorPod("app-exec-2", "2", "Running"), "False"),
            currentExecutorPod("app-exec-3", "3", "Running"),
            deleting,
            currentExecutorPod("app-exec-5", "5", "Pending"),
            withReady(currentExecutorPod("app-exec-6", "6", "Succeeded"), "False"),
            withReady(executorPod("app-exec-7", "sentinel-previous", "7", "Running"), "True"));
    assertEquals(1, ExecutorStateUtils.countReadyExecutors(app, pods));
    assertEquals(0, ExecutorStateUtils.countReadyExecutors(app, List.of()));
  }

  @Test
  void findsTheHighestExecutorIdOfTheCurrentAttempt() {
    List<Pod> pods =
        List.of(
            currentExecutorPod("app-exec-3", "3", "Running"),
            currentExecutorPod("app-exec-12", "12", "Succeeded"),
            executorPod("app-exec-40", "sentinel-previous", "40", "Running"));
    assertEquals(OptionalLong.of(12), ExecutorStateUtils.highestExecutorId(app, pods));
    assertEquals(OptionalLong.empty(), ExecutorStateUtils.highestExecutorId(app, List.of()));
  }

  @Test
  void readsTheExecutorIdFromTheLabelThenThePodName() {
    assertEquals(
        OptionalLong.of(7),
        ExecutorStateUtils.executorId(currentExecutorPod("app-exec-9", "7", "Running")));
    // A missing or invalid label falls back to the trailing digits of the pod name
    assertEquals(
        OptionalLong.of(9),
        ExecutorStateUtils.executorId(currentExecutorPod("app-exec-9", null, "Running")));
    assertEquals(
        OptionalLong.of(9),
        ExecutorStateUtils.executorId(currentExecutorPod("app-exec-9", "x", "Running")));
    assertEquals(
        OptionalLong.of(9),
        ExecutorStateUtils.executorId(currentExecutorPod("app-exec-9", "0", "Running")));
    // A name without trailing digits or with an overflowing number has no id
    assertEquals(
        OptionalLong.empty(),
        ExecutorStateUtils.executorId(currentExecutorPod("app-exec", null, "Running")));
    assertEquals(
        OptionalLong.empty(),
        ExecutorStateUtils.executorId(
            currentExecutorPod("app-exec-99999999999999999999", null, "Running")));
  }
}
