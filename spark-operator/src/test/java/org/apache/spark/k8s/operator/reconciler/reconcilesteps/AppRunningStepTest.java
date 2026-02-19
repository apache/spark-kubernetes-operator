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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.spec.ApplicationTolerations;
import org.apache.spark.k8s.operator.spec.ExecutorInstanceConfig;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;

class AppRunningStepTest {
  private SparkAppContext mockContext;
  private SparkAppStatusRecorder mockRecorder;
  private SparkApplication app;
  private ApplicationSpec appSpec;
  private ApplicationStatus appStatus;
  private AppRunningStep appRunningStep;

  @BeforeEach
  void setUp() {
    mockContext = mock(SparkAppContext.class);
    mockRecorder = mock(SparkAppStatusRecorder.class);
    app = new SparkApplication();
    appSpec = new ApplicationSpec();
    appStatus = new ApplicationStatus();
    appRunningStep = new AppRunningStep();

    app.setSpec(appSpec);
    app.setStatus(appStatus);

    when(mockContext.getResource()).thenReturn(app);

    // Mock the recorder to actually update the app status when called
    when(mockRecorder.persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class)))
        .thenAnswer(
            invocation -> {
              ApplicationStatus newStatus = invocation.getArgument(1);
              app.setStatus(newStatus);
              return true;
            });
  }

  private Pod createReadyExecutorPod(String name) {
    return new PodBuilder()
        .withNewMetadata()
        .withName(name)
        .endMetadata()
        .withNewStatus()
        .withPhase("Running")
        .addNewCondition()
        .withType("Ready")
        .withStatus("True")
        .endCondition()
        .addNewContainerStatus()
        .withName("executor")
        .withReady(true)
        .withNewState()
        .withNewRunning()
        .endRunning()
        .endState()
        .endContainerStatus()
        .endStatus()
        .build();
  }

  private Set<Pod> createExecutorPods(int count) {
    Set<Pod> executors = new HashSet<>();
    for (int k = 0; k < count; k++) {
      executors.add(createReadyExecutorPod("executor-" + k));
    }
    return executors;
  }

  @Test
  void runningWithPartialCapacityDynamicAllocationDisabled() {
    // Dynamic allocation disabled, executors between min and max
    Map<String, String> sparkConf = new HashMap<>();
    sparkConf.put("spark.dynamicAllocation.enabled", "false");
    appSpec.setSparkConf(sparkConf);

    ExecutorInstanceConfig instanceConfig =
        ExecutorInstanceConfig.builder().initExecutors(2).minExecutors(2).maxExecutors(10).build();
    ApplicationTolerations tolerations =
        ApplicationTolerations.builder().instanceConfig(instanceConfig).build();
    appSpec.setApplicationTolerations(tolerations);

    // Current state: RunningHealthy with 5 executors (between min and max)
    appStatus =
        appStatus.appendNewState(
            new ApplicationState(ApplicationStateSummary.RunningHealthy, "Previous state"));
    app.setStatus(appStatus);

    when(mockContext.getSparkConf()).thenReturn(sparkConf);
    when(mockContext.getExecutorsForApplication()).thenReturn(createExecutorPods(5));

    appRunningStep.reconcile(mockContext, mockRecorder);

    ApplicationStateSummary currentState =
        app.getStatus().getCurrentState().getCurrentStateSummary();
    assertEquals(ApplicationStateSummary.RunningWithPartialCapacity, currentState);
  }

  @Test
  void runningHealthyDynamicAllocationEnabled() {
    // Dynamic allocation enabled, executors between min and max
    Map<String, String> sparkConf = new HashMap<>();
    sparkConf.put("spark.dynamicAllocation.enabled", "true");
    appSpec.setSparkConf(sparkConf);

    ExecutorInstanceConfig instanceConfig =
        ExecutorInstanceConfig.builder().initExecutors(2).minExecutors(2).maxExecutors(10).build();
    ApplicationTolerations tolerations =
        ApplicationTolerations.builder().instanceConfig(instanceConfig).build();
    appSpec.setApplicationTolerations(tolerations);

    // Current state: DriverReady with 5 executors
    appStatus =
        appStatus.appendNewState(
            new ApplicationState(ApplicationStateSummary.DriverReady, "Driver ready"));
    app.setStatus(appStatus);

    when(mockContext.getSparkConf()).thenReturn(sparkConf);
    when(mockContext.getExecutorsForApplication()).thenReturn(createExecutorPods(5));

    appRunningStep.reconcile(mockContext, mockRecorder);

    ApplicationStateSummary currentState =
        app.getStatus().getCurrentState().getCurrentStateSummary();
    assertEquals(ApplicationStateSummary.RunningHealthy, currentState);
  }

  @Test
  void runningHealthyExecutorsAtMaxCapacity() {
    // Dynamic allocation disabled, executors at max capacity
    Map<String, String> sparkConf = new HashMap<>();
    sparkConf.put("spark.dynamicAllocation.enabled", "false");
    appSpec.setSparkConf(sparkConf);

    ExecutorInstanceConfig instanceConfig =
        ExecutorInstanceConfig.builder().initExecutors(2).minExecutors(2).maxExecutors(10).build();
    ApplicationTolerations tolerations =
        ApplicationTolerations.builder().instanceConfig(instanceConfig).build();
    appSpec.setApplicationTolerations(tolerations);

    appStatus =
        appStatus.appendNewState(
            new ApplicationState(ApplicationStateSummary.DriverReady, "Driver ready"));
    app.setStatus(appStatus);

    when(mockContext.getSparkConf()).thenReturn(sparkConf);
    when(mockContext.getExecutorsForApplication()).thenReturn(createExecutorPods(10));

    appRunningStep.reconcile(mockContext, mockRecorder);

    ApplicationStateSummary currentState =
        app.getStatus().getCurrentState().getCurrentStateSummary();
    assertEquals(ApplicationStateSummary.RunningHealthy, currentState);
  }

  @Test
  void runningHealthyMaxExecutorsZero() {
    // maxExecutors is 0 (special case, should always be healthy)
    Map<String, String> sparkConf = new HashMap<>();
    sparkConf.put("spark.dynamicAllocation.enabled", "false");
    appSpec.setSparkConf(sparkConf);

    ExecutorInstanceConfig instanceConfig =
        ExecutorInstanceConfig.builder().initExecutors(2).minExecutors(2).maxExecutors(0).build();
    ApplicationTolerations tolerations =
        ApplicationTolerations.builder().instanceConfig(instanceConfig).build();
    appSpec.setApplicationTolerations(tolerations);

    appStatus =
        appStatus.appendNewState(
            new ApplicationState(ApplicationStateSummary.DriverReady, "Driver ready"));
    app.setStatus(appStatus);

    when(mockContext.getSparkConf()).thenReturn(sparkConf);
    when(mockContext.getExecutorsForApplication()).thenReturn(createExecutorPods(5));

    appRunningStep.reconcile(mockContext, mockRecorder);

    ApplicationStateSummary currentState =
        app.getStatus().getCurrentState().getCurrentStateSummary();
    assertEquals(ApplicationStateSummary.RunningHealthy, currentState);
  }

  @Test
  void runningWithBelowThresholdExecutors() {
    // Executors below minimum threshold
    Map<String, String> sparkConf = new HashMap<>();
    sparkConf.put("spark.dynamicAllocation.enabled", "false");
    appSpec.setSparkConf(sparkConf);

    ExecutorInstanceConfig instanceConfig =
        ExecutorInstanceConfig.builder().initExecutors(5).minExecutors(5).maxExecutors(10).build();
    ApplicationTolerations tolerations =
        ApplicationTolerations.builder().instanceConfig(instanceConfig).build();
    appSpec.setApplicationTolerations(tolerations);

    appStatus =
        appStatus.appendNewState(
            new ApplicationState(ApplicationStateSummary.RunningHealthy, "Previous state"));
    app.setStatus(appStatus);

    when(mockContext.getSparkConf()).thenReturn(sparkConf);
    when(mockContext.getExecutorsForApplication()).thenReturn(createExecutorPods(3));

    appRunningStep.reconcile(mockContext, mockRecorder);

    ApplicationStateSummary currentState =
        app.getStatus().getCurrentState().getCurrentStateSummary();
    assertEquals(ApplicationStateSummary.RunningWithBelowThresholdExecutors, currentState);
  }

  @Test
  void initializedBelowThresholdExecutorsDuringStartup() {
    // During startup phase with fewer executors than initExecutors
    Map<String, String> sparkConf = new HashMap<>();
    appSpec.setSparkConf(sparkConf);

    ExecutorInstanceConfig instanceConfig =
        ExecutorInstanceConfig.builder().initExecutors(5).minExecutors(2).maxExecutors(10).build();
    ApplicationTolerations tolerations =
        ApplicationTolerations.builder().instanceConfig(instanceConfig).build();
    appSpec.setApplicationTolerations(tolerations);

    // DriverReady is a starting state
    appStatus =
        appStatus.appendNewState(
            new ApplicationState(ApplicationStateSummary.DriverReady, "Driver ready"));
    app.setStatus(appStatus);

    when(mockContext.getSparkConf()).thenReturn(sparkConf);
    when(mockContext.getExecutorsForApplication()).thenReturn(createExecutorPods(3));

    appRunningStep.reconcile(mockContext, mockRecorder);

    ApplicationStateSummary currentState =
        app.getStatus().getCurrentState().getCurrentStateSummary();
    assertEquals(ApplicationStateSummary.InitializedBelowThresholdExecutors, currentState);
  }

  @Test
  void runningWithPartialCapacityDuringStartupWithEnoughExecutors() {
    // During startup phase with enough executors
    Map<String, String> sparkConf = new HashMap<>();
    appSpec.setSparkConf(sparkConf);

    ExecutorInstanceConfig instanceConfig =
        ExecutorInstanceConfig.builder().initExecutors(5).minExecutors(2).maxExecutors(10).build();
    ApplicationTolerations tolerations =
        ApplicationTolerations.builder().instanceConfig(instanceConfig).build();
    appSpec.setApplicationTolerations(tolerations);

    appStatus =
        appStatus.appendNewState(
            new ApplicationState(ApplicationStateSummary.DriverReady, "Driver ready"));
    app.setStatus(appStatus);

    when(mockContext.getSparkConf()).thenReturn(sparkConf);
    when(mockContext.getExecutorsForApplication()).thenReturn(createExecutorPods(5));

    appRunningStep.reconcile(mockContext, mockRecorder);

    ApplicationStateSummary currentState =
        app.getStatus().getCurrentState().getCurrentStateSummary();
    assertEquals(ApplicationStateSummary.RunningWithPartialCapacity, currentState);
  }

  @Test
  void runningHealthyNoExecutorConfig() {
    // No executor instance config (should be healthy)
    Map<String, String> sparkConf = new HashMap<>();
    appSpec.setSparkConf(sparkConf);

    ApplicationTolerations tolerations =
        ApplicationTolerations.builder().instanceConfig(null).build();
    appSpec.setApplicationTolerations(tolerations);

    appStatus =
        appStatus.appendNewState(
            new ApplicationState(ApplicationStateSummary.DriverReady, "Driver ready"));
    app.setStatus(appStatus);

    when(mockContext.getSparkConf()).thenReturn(sparkConf);
    when(mockContext.getExecutorsForApplication()).thenReturn(createExecutorPods(3));

    appRunningStep.reconcile(mockContext, mockRecorder);

    ApplicationStateSummary currentState =
        app.getStatus().getCurrentState().getCurrentStateSummary();
    assertEquals(ApplicationStateSummary.RunningHealthy, currentState);
  }

  @Test
  void runningHealthyInitExecutorsZero() {
    // initExecutors is 0 (should be healthy)
    Map<String, String> sparkConf = new HashMap<>();
    appSpec.setSparkConf(sparkConf);

    ExecutorInstanceConfig instanceConfig =
        ExecutorInstanceConfig.builder().initExecutors(0).minExecutors(0).maxExecutors(10).build();
    ApplicationTolerations tolerations =
        ApplicationTolerations.builder().instanceConfig(instanceConfig).build();
    appSpec.setApplicationTolerations(tolerations);

    appStatus =
        appStatus.appendNewState(
            new ApplicationState(ApplicationStateSummary.DriverReady, "Driver ready"));
    app.setStatus(appStatus);

    when(mockContext.getSparkConf()).thenReturn(sparkConf);
    when(mockContext.getExecutorsForApplication()).thenReturn(createExecutorPods(3));

    appRunningStep.reconcile(mockContext, mockRecorder);

    ApplicationStateSummary currentState =
        app.getStatus().getCurrentState().getCurrentStateSummary();
    assertEquals(ApplicationStateSummary.RunningHealthy, currentState);
  }

  @Test
  void runningWithPartialCapacityDoesNotAppendDuplicateStates() {
    // Verify that executor count changes within partial capacity range don't create duplicate
    // states
    Map<String, String> sparkConf = new HashMap<>();
    sparkConf.put("spark.dynamicAllocation.enabled", "false");
    appSpec.setSparkConf(sparkConf);

    ExecutorInstanceConfig instanceConfig =
        ExecutorInstanceConfig.builder().initExecutors(2).minExecutors(2).maxExecutors(10).build();
    ApplicationTolerations tolerations =
        ApplicationTolerations.builder().instanceConfig(instanceConfig).build();
    appSpec.setApplicationTolerations(tolerations);

    // Start with DriverReady state
    appStatus =
        appStatus.appendNewState(
            new ApplicationState(ApplicationStateSummary.DriverReady, "Driver ready"));
    app.setStatus(appStatus);

    when(mockContext.getSparkConf()).thenReturn(sparkConf);

    // First reconcile: 5 executors (between min and max) -> should transition to
    // RunningWithPartialCapacity
    when(mockContext.getExecutorsForApplication()).thenReturn(createExecutorPods(5));
    appRunningStep.reconcile(mockContext, mockRecorder);

    ApplicationStateSummary currentState =
        app.getStatus().getCurrentState().getCurrentStateSummary();
    assertEquals(ApplicationStateSummary.RunningWithPartialCapacity, currentState);
    int stateCountAfterFirst = app.getStatus().getStateTransitionHistory().size();

    // Mock driver pod for subsequent reconciles that don't change state
    Pod mockDriverPod =
        new PodBuilder()
            .withNewMetadata()
            .withName("driver")
            .endMetadata()
            .withNewStatus()
            .withPhase("Running")
            .addNewCondition()
            .withType("Ready")
            .withStatus("True")
            .endCondition()
            .endStatus()
            .build();
    when(mockContext.getDriverPod()).thenReturn(Optional.of(mockDriverPod));

    // Second reconcile: 6 executors (still between min and max) -> should NOT append new state
    when(mockContext.getExecutorsForApplication()).thenReturn(createExecutorPods(6));
    appRunningStep.reconcile(mockContext, mockRecorder);

    currentState = app.getStatus().getCurrentState().getCurrentStateSummary();
    assertEquals(ApplicationStateSummary.RunningWithPartialCapacity, currentState);
    int stateCountAfterSecond = app.getStatus().getStateTransitionHistory().size();
    assertEquals(
        stateCountAfterFirst,
        stateCountAfterSecond,
        "State count should not increase when state doesn't change");

    // Third reconcile: 4 executors (still between min and max) -> should NOT append new state
    when(mockContext.getExecutorsForApplication()).thenReturn(createExecutorPods(4));
    appRunningStep.reconcile(mockContext, mockRecorder);

    currentState = app.getStatus().getCurrentState().getCurrentStateSummary();
    assertEquals(ApplicationStateSummary.RunningWithPartialCapacity, currentState);
    int stateCountAfterThird = app.getStatus().getStateTransitionHistory().size();
    assertEquals(
        stateCountAfterFirst,
        stateCountAfterThird,
        "State count should not increase when state doesn't change");

    // Fourth reconcile: 9 executors (still between min and max) -> should NOT append new state
    when(mockContext.getExecutorsForApplication()).thenReturn(createExecutorPods(9));
    appRunningStep.reconcile(mockContext, mockRecorder);

    currentState = app.getStatus().getCurrentState().getCurrentStateSummary();
    assertEquals(ApplicationStateSummary.RunningWithPartialCapacity, currentState);
    int stateCountAfterFourth = app.getStatus().getStateTransitionHistory().size();
    assertEquals(
        stateCountAfterFirst,
        stateCountAfterFourth,
        "State count should not increase when state doesn't change");

    // Fifth reconcile: 10 executors (at max) -> should transition to RunningHealthy
    when(mockContext.getExecutorsForApplication()).thenReturn(createExecutorPods(10));
    appRunningStep.reconcile(mockContext, mockRecorder);

    currentState = app.getStatus().getCurrentState().getCurrentStateSummary();
    assertEquals(ApplicationStateSummary.RunningHealthy, currentState);
    int stateCountAfterFifth = app.getStatus().getStateTransitionHistory().size();
    assertEquals(
        stateCountAfterFirst + 1,
        stateCountAfterFifth,
        "State count should increase by 1 when transitioning to RunningHealthy");
  }
}
