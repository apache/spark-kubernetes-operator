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

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.API_SECONDARY_RESOURCE_CREATE_BACKOFF_JITTER_MILLIS;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.API_SECONDARY_RESOURCE_CREATE_BACKOFF_MULTIPLIER;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.API_SECONDARY_RESOURCE_CREATE_INITIAL_BACKOFF_MILLIS;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.API_SECONDARY_RESOURCE_CREATE_MAX_ATTEMPTS;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.API_SECONDARY_RESOURCE_CREATE_MAX_BACKOFF_MILLIS;
import static org.apache.spark.k8s.operator.utils.TestUtils.setConfigKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.NamespaceableResource;
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

    @SuppressWarnings("unchecked")
    private NamespaceableResource<Pod> mockSecondaryResource(Pod pod) {
        NamespaceableResource<Pod> mockResource = mock(NamespaceableResource.class);
        when(mockClient.resource(pod)).thenReturn(mockResource);
        return mockResource;
    }

    private void setFastBackoff() {
        setConfigKey(API_SECONDARY_RESOURCE_CREATE_MAX_ATTEMPTS, 3L);
        setConfigKey(API_SECONDARY_RESOURCE_CREATE_INITIAL_BACKOFF_MILLIS, 5000L);
        setConfigKey(API_SECONDARY_RESOURCE_CREATE_MAX_BACKOFF_MILLIS, 10000L);
        setConfigKey(API_SECONDARY_RESOURCE_CREATE_BACKOFF_JITTER_MILLIS, 0L);
        setConfigKey(API_SECONDARY_RESOURCE_CREATE_BACKOFF_MULTIPLIER, 2.0);
    }

    private void restoreBackoffDefaults(
            long maxAttempts,
            long initialBackoff,
            long maxBackoff,
            long jitter,
            double multiplier) {
        setConfigKey(API_SECONDARY_RESOURCE_CREATE_MAX_ATTEMPTS, maxAttempts);
        setConfigKey(API_SECONDARY_RESOURCE_CREATE_INITIAL_BACKOFF_MILLIS, initialBackoff);
        setConfigKey(API_SECONDARY_RESOURCE_CREATE_MAX_BACKOFF_MILLIS, maxBackoff);
        setConfigKey(API_SECONDARY_RESOURCE_CREATE_BACKOFF_JITTER_MILLIS, jitter);
        setConfigKey(API_SECONDARY_RESOURCE_CREATE_BACKOFF_MULTIPLIER, multiplier);
    }

    private Pod buildTestPod() {
        return new PodBuilder()
                .withNewMetadata()
                .withName("test-pod")
                .withNamespace("default")
                .endMetadata()
                .build();
    }

    @Test
    void secondaryResourceCreationRetriesWithBackoffOnConflict() {
        long savedMaxAttempts = API_SECONDARY_RESOURCE_CREATE_MAX_ATTEMPTS.getValue();
        long savedInitialBackoff = API_SECONDARY_RESOURCE_CREATE_INITIAL_BACKOFF_MILLIS.getValue();
        long savedMaxBackoff = API_SECONDARY_RESOURCE_CREATE_MAX_BACKOFF_MILLIS.getValue();
        long savedJitter = API_SECONDARY_RESOURCE_CREATE_BACKOFF_JITTER_MILLIS.getValue();
        double savedMultiplier = API_SECONDARY_RESOURCE_CREATE_BACKOFF_MULTIPLIER.getValue();
        try {
            setFastBackoff();
            Pod pod = buildTestPod();
            NamespaceableResource<Pod> mockResource = mockSecondaryResource(pod);
            KubernetesClientException conflict =
                    new KubernetesClientException("conflict", HTTP_CONFLICT, null);
            when(mockResource.get()).thenReturn(null);
            when(mockResource.create()).thenThrow(conflict).thenReturn(pod);

            long start = System.currentTimeMillis();
            Optional<Pod> result = ReconcilerUtils.getOrCreateSecondaryResource(mockClient, pod);
            long elapsed = System.currentTimeMillis() - start;

            assertTrue(result.isPresent());
            verify(mockResource, times(2)).create();
            assertTrue(elapsed >= 5000L,
                    "Expected backoff delay of at least "
                            + "5000ms, but was: "
                            + elapsed + "ms");
        } finally {
            restoreBackoffDefaults(
                    savedMaxAttempts,
                    savedInitialBackoff,
                    savedMaxBackoff,
                    savedJitter,
                    savedMultiplier);
        }
    }
}
