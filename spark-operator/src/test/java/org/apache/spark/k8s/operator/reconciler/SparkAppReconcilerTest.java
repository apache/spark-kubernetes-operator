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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import org.apache.spark.k8s.operator.SparkAppSubmissionWorker;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.metrics.healthcheck.SentinelManager;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.utils.ReconcilerUtils;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;

class SparkAppReconcilerTest {
  private final SparkAppStatusRecorder mockRecorder = mock(SparkAppStatusRecorder.class);
  private final SentinelManager<SparkApplication> mockSentinelManager = mock(SentinelManager.class);
  private final KubernetesClient mockClient = mock(KubernetesClient.class);
  private final Context<SparkApplication> mockContext = mock(Context.class);
  private final Pod mockDriver = mock(Pod.class);
  private final SparkAppSubmissionWorker mockWorker = mock(SparkAppSubmissionWorker.class);
  SparkApplication app = new SparkApplication();
  SparkAppReconciler reconciler =
      new SparkAppReconciler(mockWorker, mockRecorder, mockSentinelManager);

  @BeforeEach
  void beforeEach() {
    when(mockContext.getClient()).thenReturn(mockClient);
    doNothing().when(mockRecorder).removeCachedStatus(any(SparkApplication.class));
    doAnswer(
            invocation -> {
              app.setStatus(invocation.getArgument(1));
              return null;
            })
        .when(mockRecorder)
        .persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class));
    doAnswer(
            invocation -> {
              ApplicationStatus updatedStatus =
                  app.getStatus().appendNewState(invocation.getArgument(1));
              app.setStatus(updatedStatus);
              return null;
            })
        .when(mockRecorder)
        .appendNewStateAndPersist(any(SparkAppContext.class), any(ApplicationState.class));
  }

  @SuppressWarnings("PMD.UnusedLocalVariable")
  @Test
  void testCleanupRunningApp() {
    try (MockedConstruction<SparkAppContext> mockAppContext =
            mockConstruction(
                SparkAppContext.class,
                (mock, context) -> {
                  when(mock.getResource()).thenReturn(app);
                  when(mock.getClient()).thenReturn(mockClient);
                  when(mock.getDriverPod()).thenReturn(Optional.of(mockDriver));
                  when(mock.getDriverPodSpec()).thenReturn(mockDriver);
                  when(mock.getDriverPreResourcesSpec()).thenReturn(List.of());
                  when(mock.getDriverResourcesSpec()).thenReturn(List.of());
                });
        MockedStatic<ReconcilerUtils> utils = Mockito.mockStatic(ReconcilerUtils.class)) {
      // delete running app
      app.setStatus(
          app.getStatus()
              .appendNewState(new ApplicationState(ApplicationStateSummary.RunningHealthy, "")));
      DeleteControl deleteControl = reconciler.cleanup(app, mockContext);
      assertFalse(deleteControl.isRemoveFinalizer());
      utils.verify(() -> ReconcilerUtils.deleteResourceIfExists(mockClient, mockDriver, false));
      assertEquals(
          ApplicationStateSummary.ResourceReleased,
          app.getStatus().getCurrentState().getCurrentStateSummary());

      // proceed delete for terminated app
      deleteControl = reconciler.cleanup(app, mockContext);
      assertTrue(deleteControl.isRemoveFinalizer());
    }
  }

  @SuppressWarnings("PMD.UnusedLocalVariable")
  @Test
  void testCleanupAppTerminatedWithoutReleaseResources() {
    try (MockedConstruction<SparkAppContext> mockAppContext =
            mockConstruction(
                SparkAppContext.class,
                (mock, context) -> {
                  when(mock.getResource()).thenReturn(app);
                  when(mock.getClient()).thenReturn(mockClient);
                  when(mock.getDriverPod()).thenReturn(Optional.of(mockDriver));
                  when(mock.getDriverPodSpec()).thenReturn(mockDriver);
                  when(mock.getDriverPreResourcesSpec()).thenReturn(List.of());
                  when(mock.getDriverResourcesSpec()).thenReturn(List.of());
                });
        MockedStatic<ReconcilerUtils> utils = Mockito.mockStatic(ReconcilerUtils.class)) {
      // delete app
      app.setStatus(
          app.getStatus()
              .appendNewState(
                  new ApplicationState(
                      ApplicationStateSummary.TerminatedWithoutReleaseResources, "")));
      DeleteControl deleteControl = reconciler.cleanup(app, mockContext);
      assertFalse(deleteControl.isRemoveFinalizer());
      utils.verify(() -> ReconcilerUtils.deleteResourceIfExists(mockClient, mockDriver, false));
      assertEquals(
          ApplicationStateSummary.ResourceReleased,
          app.getStatus().getCurrentState().getCurrentStateSummary());

      // proceed delete for terminated app
      deleteControl = reconciler.cleanup(app, mockContext);
      assertTrue(deleteControl.isRemoveFinalizer());
    }
  }

  @SuppressWarnings("PMD.UnusedLocalVariable")
  @Test
  void testCleanupAppTerminatedResourceReleased() {
    try (MockedConstruction<SparkAppContext> mockAppContext =
            mockConstruction(
                SparkAppContext.class,
                (mock, context) -> {
                  when(mock.getResource()).thenReturn(app);
                  when(mock.getClient()).thenReturn(mockClient);
                  when(mock.getDriverPreResourcesSpec()).thenReturn(List.of());
                  when(mock.getDriverResourcesSpec()).thenReturn(List.of());
                });
        MockedStatic<ReconcilerUtils> utils = Mockito.mockStatic(ReconcilerUtils.class)) {
      // delete app
      app.setStatus(
          app.getStatus()
              .appendNewState(new ApplicationState(ApplicationStateSummary.ResourceReleased, "")));
      DeleteControl deleteControl = reconciler.cleanup(app, mockContext);
      assertTrue(deleteControl.isRemoveFinalizer());
      utils.verifyNoInteractions();
    }
  }
}
