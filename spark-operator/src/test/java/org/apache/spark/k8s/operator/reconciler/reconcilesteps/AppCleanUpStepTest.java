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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.utils.ReconcilerUtils;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;
import org.apache.spark.k8s.operator.utils.SparkAppStatusUtils;

@SuppressWarnings("PMD.NcssCount")
class AppCleanUpStepTest {
  @Test
  void enableForceDelete() {
    AppCleanUpStep appCleanUpStep = new AppCleanUpStep();
    SparkApplication app = new SparkApplication();
    app.getStatus()
        .getCurrentState()
        .setLastTransitionTime(Instant.now().minusSeconds(5).toString());
    app.getSpec()
        .getApplicationTolerations()
        .getApplicationTimeoutConfig()
        .setForceTerminationGracePeriodMillis(3000L);
    assertTrue(appCleanUpStep.enableForceDelete(app));
  }

  @Test
  void routineCleanupForRunningAppExpectNoAction() {
    SparkAppStatusRecorder mockRecorder = mock(SparkAppStatusRecorder.class);
    AppCleanUpStep routineCheck = new AppCleanUpStep();
    for (ApplicationStateSummary stateSummary : ApplicationStateSummary.values()) {
      if (!stateSummary.isStopping() && !stateSummary.isTerminated()) {
        ApplicationStatus status = prepareApplicationStatus(stateSummary);
        ApplicationSpec spec = ApplicationSpec.builder().build();
        SparkApplication mockApp = mock(SparkApplication.class);
        when(mockApp.getStatus()).thenReturn(status);
        when(mockApp.getSpec()).thenReturn(spec);
        SparkAppContext mockAppContext = mock(SparkAppContext.class);
        when(mockAppContext.getResource()).thenReturn(mockApp);
        ReconcileProgress progress = routineCheck.reconcile(mockAppContext, mockRecorder);
        Assertions.assertEquals(ReconcileProgress.proceed(), progress);
        verify(mockAppContext).getResource();
        verify(mockApp).getSpec();
        verify(mockApp).getStatus();
        verifyNoMoreInteractions(mockAppContext, mockRecorder, mockApp);
      }
    }
  }

  @Test
  void onDemandCleanupForRunningAppExpectDelete() {
    SparkAppStatusRecorder mockRecorder = mock(SparkAppStatusRecorder.class);
    AppCleanUpStep cleanUpWithReason = new AppCleanUpStep(SparkAppStatusUtils::appCancelled);
    for (ApplicationStateSummary stateSummary : ApplicationStateSummary.values()) {
      if (!stateSummary.isStopping() && !stateSummary.isTerminated()) {
        ApplicationStatus status = prepareApplicationStatus(stateSummary);
        ApplicationSpec spec = ApplicationSpec.builder().build();
        SparkApplication mockApp = mock(SparkApplication.class);
        when(mockApp.getStatus()).thenReturn(status);
        when(mockApp.getSpec()).thenReturn(spec);
        SparkAppContext mockAppContext = mock(SparkAppContext.class);
        when(mockAppContext.getResource()).thenReturn(mockApp);
        KubernetesClient mockClient = mock(KubernetesClient.class);
        when(mockAppContext.getClient()).thenReturn(mockClient);
        Pod driverPod = mock(Pod.class);
        when(mockAppContext.getDriverPod()).thenReturn(Optional.of(driverPod));
        when(mockAppContext.getDriverPreResourcesSpec()).thenReturn(Collections.emptyList());
        when(mockAppContext.getDriverResourcesSpec()).thenReturn(Collections.emptyList());

        try (MockedStatic<ReconcilerUtils> utils = Mockito.mockStatic(ReconcilerUtils.class)) {
          ReconcileProgress progress = cleanUpWithReason.reconcile(mockAppContext, mockRecorder);
          utils.verify(() -> ReconcilerUtils.deleteResourceIfExists(mockClient, driverPod, false));
          Assertions.assertEquals(
              ReconcileProgress.completeAndRequeueAfter(Duration.ofMillis(2000)), progress);
        }

        verify(mockAppContext).getResource();
        verify(mockApp, times(2)).getSpec();
        verify(mockApp, times(2)).getStatus();
        verify(mockAppContext).getClient();
        verify(mockAppContext).getDriverPod();
        ArgumentCaptor<ApplicationState> captor = ArgumentCaptor.forClass(ApplicationState.class);
        verify(mockRecorder).appendNewStateAndPersist(eq(mockAppContext), captor.capture());
        ApplicationState appState = captor.getValue();
        Assertions.assertEquals(
            ApplicationStateSummary.ResourceReleased, appState.getCurrentStateSummary());
        Assertions.assertEquals(Constants.APP_CANCELLED_MESSAGE, appState.getMessage());
        verifyNoMoreInteractions(mockAppContext, mockRecorder, mockApp, mockClient, driverPod);
      }
    }
  }

  @Test
  void routineCleanupForTerminatedAppExpectNoAction() {
    SparkAppStatusRecorder mockRecorder = mock(SparkAppStatusRecorder.class);
    AppCleanUpStep routineCheck = new AppCleanUpStep();
    for (ApplicationStateSummary stateSummary : ApplicationStateSummary.values()) {
      if (stateSummary.isTerminated()) {
        ApplicationStatus status = prepareApplicationStatus(stateSummary);
        SparkApplication mockApp = mock(SparkApplication.class);
        ApplicationSpec spec = ApplicationSpec.builder().build();
        when(mockApp.getStatus()).thenReturn(status);
        SparkAppContext mockAppContext = mock(SparkAppContext.class);
        when(mockAppContext.getResource()).thenReturn(mockApp);
        when(mockApp.getSpec()).thenReturn(spec);
        ReconcileProgress progress = routineCheck.reconcile(mockAppContext, mockRecorder);
        Assertions.assertEquals(ReconcileProgress.completeAndNoRequeue(), progress);
        verify(mockAppContext).getResource();
        verify(mockApp).getSpec();
        verify(mockApp).getStatus();
        verify(mockRecorder).removeCachedStatus(mockApp);
        verifyNoMoreInteractions(mockAppContext, mockRecorder, mockApp);
      }
    }
  }

  @Test
  void onDemandCleanupForTerminatedAppExpectNoAction() {
    SparkAppStatusRecorder mockRecorder = mock(SparkAppStatusRecorder.class);
    AppCleanUpStep routineCheck = new AppCleanUpStep();
    ApplicationStatus status = prepareApplicationStatus(ApplicationStateSummary.ResourceReleased);
    SparkApplication mockApp = mock(SparkApplication.class);
    ApplicationSpec spec = ApplicationSpec.builder().build();
    when(mockApp.getStatus()).thenReturn(status);
    SparkAppContext mockAppContext = mock(SparkAppContext.class);
    when(mockAppContext.getResource()).thenReturn(mockApp);
    when(mockApp.getSpec()).thenReturn(spec);
    ReconcileProgress progress = routineCheck.reconcile(mockAppContext, mockRecorder);
    Assertions.assertEquals(ReconcileProgress.completeAndNoRequeue(), progress);
    verify(mockAppContext).getResource();
    verify(mockApp).getSpec();
    verify(mockApp).getStatus();
    verify(mockRecorder).removeCachedStatus(mockApp);
    verifyNoMoreInteractions(mockAppContext, mockRecorder, mockApp);
  }

  @Test
  void onDemandCleanupForTerminatedAppExpectDelete() {
    SparkAppStatusRecorder mockRecorder = mock(SparkAppStatusRecorder.class);
    AppCleanUpStep cleanUpWithReason = new AppCleanUpStep(SparkAppStatusUtils::appCancelled);
    ApplicationStatus status =
        prepareApplicationStatus(ApplicationStateSummary.TerminatedWithoutReleaseResources);
    SparkApplication mockApp = mock(SparkApplication.class);
    ApplicationSpec spec = ApplicationSpec.builder().build();
    when(mockApp.getStatus()).thenReturn(status);
    SparkAppContext mockAppContext = mock(SparkAppContext.class);
    when(mockAppContext.getResource()).thenReturn(mockApp);
    when(mockApp.getSpec()).thenReturn(spec);
    KubernetesClient mockClient = mock(KubernetesClient.class);
    when(mockAppContext.getClient()).thenReturn(mockClient);
    Pod driverPod = mock(Pod.class);
    when(mockAppContext.getDriverPod()).thenReturn(Optional.of(driverPod));
    when(mockAppContext.getDriverPreResourcesSpec()).thenReturn(Collections.emptyList());
    when(mockAppContext.getDriverResourcesSpec()).thenReturn(Collections.emptyList());

    try (MockedStatic<ReconcilerUtils> utils = Mockito.mockStatic(ReconcilerUtils.class)) {
      ReconcileProgress progress = cleanUpWithReason.reconcile(mockAppContext, mockRecorder);
      utils.verify(() -> ReconcilerUtils.deleteResourceIfExists(mockClient, driverPod, false));
      Assertions.assertEquals(
          ReconcileProgress.completeAndRequeueAfter(Duration.ofMillis(2000)), progress);
    }

    verify(mockAppContext).getResource();
    verify(mockApp, times(2)).getSpec();
    verify(mockApp, times(2)).getStatus();
    verify(mockAppContext).getClient();
    verify(mockAppContext).getDriverPod();
    ArgumentCaptor<ApplicationState> captor = ArgumentCaptor.forClass(ApplicationState.class);
    verify(mockRecorder).appendNewStateAndPersist(eq(mockAppContext), captor.capture());
    ApplicationState appState = captor.getValue();
    Assertions.assertEquals(
        ApplicationStateSummary.ResourceReleased, appState.getCurrentStateSummary());
    Assertions.assertEquals(Constants.APP_CANCELLED_MESSAGE, appState.getMessage());
    verifyNoMoreInteractions(mockAppContext, mockRecorder, mockApp, mockClient, driverPod);
  }

  @Test
  void cleanupForAppExpectDeleteWithRecompute() {
    SparkAppStatusRecorder mockRecorder = mock(SparkAppStatusRecorder.class);
    AppCleanUpStep cleanUpWithReason = new AppCleanUpStep(SparkAppStatusUtils::appCancelled);
    ApplicationStatus status1 = prepareApplicationStatus(ApplicationStateSummary.SchedulingFailure);
    ApplicationStatus status2 =
        prepareApplicationStatus(
            ApplicationStateSummary.SchedulingFailure,
            ApplicationStateSummary.TerminatedWithoutReleaseResources);
    SparkApplication mockApp1 = mock(SparkApplication.class);
    SparkApplication mockApp2 = mock(SparkApplication.class);
    ApplicationSpec spec = ApplicationSpec.builder().build();
    when(mockApp1.getStatus()).thenReturn(status1);
    when(mockApp2.getStatus()).thenReturn(status2);
    SparkAppContext mockAppContext1 = mock(SparkAppContext.class);
    SparkAppContext mockAppContext2 = mock(SparkAppContext.class);
    when(mockAppContext1.getResource()).thenReturn(mockApp1);
    when(mockAppContext2.getResource()).thenReturn(mockApp2);
    when(mockApp1.getSpec()).thenReturn(spec);
    when(mockApp2.getSpec()).thenReturn(spec);
    KubernetesClient mockClient = mock(KubernetesClient.class);
    when(mockAppContext1.getClient()).thenReturn(mockClient);
    Pod driverPod = mock(Pod.class);
    Pod driverPodSpec = mock(Pod.class);
    ConfigMap resource1 = mock(ConfigMap.class);
    ConfigMap resource2 = mock(ConfigMap.class);
    when(mockAppContext1.getDriverPod()).thenReturn(Optional.of(driverPod));
    when(mockAppContext1.getDriverPodSpec()).thenReturn(driverPodSpec);
    when(mockAppContext1.getDriverPreResourcesSpec())
        .thenReturn(Collections.singletonList(resource1));
    when(mockAppContext1.getDriverResourcesSpec()).thenReturn(Collections.singletonList(resource2));
    when(mockAppContext2.getDriverPod()).thenReturn(Optional.of(driverPod));
    when(mockAppContext2.getDriverPodSpec()).thenReturn(driverPodSpec);
    when(mockAppContext2.getDriverPreResourcesSpec())
        .thenReturn(Collections.singletonList(resource1));
    when(mockAppContext2.getDriverResourcesSpec()).thenReturn(Collections.singletonList(resource2));

    try (MockedStatic<ReconcilerUtils> utils = Mockito.mockStatic(ReconcilerUtils.class)) {
      ReconcileProgress progress1 = cleanUpWithReason.reconcile(mockAppContext1, mockRecorder);
      ReconcileProgress progress2 = cleanUpWithReason.reconcile(mockAppContext2, mockRecorder);
      utils.verify(() -> ReconcilerUtils.deleteResourceIfExists(mockClient, resource1, false));
      utils.verify(() -> ReconcilerUtils.deleteResourceIfExists(mockClient, driverPodSpec, false));
      utils.verify(() -> ReconcilerUtils.deleteResourceIfExists(mockClient, resource2, false));
      Assertions.assertEquals(
          ReconcileProgress.completeAndRequeueAfter(Duration.ofMillis(2000)), progress1);
      Assertions.assertEquals(
          ReconcileProgress.completeAndRequeueAfter(Duration.ofMillis(2000)), progress2);
    }

    verify(mockAppContext1).getResource();
    verify(mockApp1, times(2)).getSpec();
    verify(mockApp1, times(2)).getStatus();
    verify(mockAppContext1, times(3)).getClient();
    verify(mockAppContext1).getDriverPreResourcesSpec();
    verify(mockAppContext1).getDriverPodSpec();
    verify(mockAppContext1).getDriverResourcesSpec();
    verify(mockAppContext2).getResource();
    verify(mockApp2, times(2)).getSpec();
    verify(mockApp2, times(2)).getStatus();
    verify(mockAppContext2, times(3)).getClient();
    verify(mockAppContext2).getDriverPreResourcesSpec();
    verify(mockAppContext2).getDriverPodSpec();
    verify(mockAppContext2).getDriverResourcesSpec();
    ArgumentCaptor<ApplicationState> captor = ArgumentCaptor.forClass(ApplicationState.class);
    verify(mockRecorder).appendNewStateAndPersist(eq(mockAppContext1), captor.capture());
    verify(mockRecorder).appendNewStateAndPersist(eq(mockAppContext2), captor.capture());
    Assertions.assertEquals(2, captor.getAllValues().size());
    ApplicationState appState1 = captor.getAllValues().get(0);
    Assertions.assertEquals(
        ApplicationStateSummary.ResourceReleased, appState1.getCurrentStateSummary());
    Assertions.assertEquals(Constants.APP_CANCELLED_MESSAGE, appState1.getMessage());
    ApplicationState appState2 = captor.getAllValues().get(1);
    Assertions.assertEquals(
        ApplicationStateSummary.ResourceReleased, appState2.getCurrentStateSummary());
    Assertions.assertEquals(Constants.APP_CANCELLED_MESSAGE, appState2.getMessage());
    verifyNoMoreInteractions(
        mockAppContext1, mockAppContext2, mockRecorder, mockApp1, mockApp2, mockClient, driverPod);
  }

  private ApplicationStatus prepareApplicationStatus(ApplicationStateSummary currentStateSummary) {
    ApplicationStatus status = new ApplicationStatus();
    ApplicationState state = new ApplicationState(currentStateSummary, "foo");
    return status.appendNewState(state);
  }

  private ApplicationStatus prepareApplicationStatus(
      ApplicationStateSummary currentStateSummary, ApplicationStateSummary previousStateSummary) {
    ApplicationStatus status = prepareApplicationStatus(previousStateSummary);
    ApplicationState state = new ApplicationState(currentStateSummary, "foo");
    return status.appendNewState(state);
  }

  @Test
  void isReleasingResourcesForSchedulingFailureAttempt() {
    AppCleanUpStep appCleanUpStep = new AppCleanUpStep();
    ApplicationStatus status = new ApplicationStatus();
    assertFalse(appCleanUpStep.isReleasingResourcesForSchedulingFailureAttempt(status));
    status =
        status.appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, "foo"));
    assertFalse(appCleanUpStep.isReleasingResourcesForSchedulingFailureAttempt(status));
    status =
        status.appendNewState(new ApplicationState(ApplicationStateSummary.RunningHealthy, "foo"));
    assertFalse(appCleanUpStep.isReleasingResourcesForSchedulingFailureAttempt(status));
    status = status.appendNewState(new ApplicationState(ApplicationStateSummary.Failed, "foo"));
    assertFalse(appCleanUpStep.isReleasingResourcesForSchedulingFailureAttempt(status));
    status =
        status.appendNewState(
            new ApplicationState(ApplicationStateSummary.ScheduledToRestart, "foo"));
    assertFalse(appCleanUpStep.isReleasingResourcesForSchedulingFailureAttempt(status));
    status =
        status.appendNewState(
            new ApplicationState(ApplicationStateSummary.SchedulingFailure, "foo"));
    assertTrue(appCleanUpStep.isReleasingResourcesForSchedulingFailureAttempt(status));
    status =
        status.appendNewState(
            new ApplicationState(ApplicationStateSummary.TerminatedWithoutReleaseResources, "foo"));
    assertTrue(appCleanUpStep.isReleasingResourcesForSchedulingFailureAttempt(status));
  }
}
