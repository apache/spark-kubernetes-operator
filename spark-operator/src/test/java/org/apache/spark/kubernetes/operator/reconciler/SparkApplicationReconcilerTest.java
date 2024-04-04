/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.kubernetes.operator.reconciler;

import java.util.Collections;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import org.apache.spark.kubernetes.operator.SparkApplication;
import org.apache.spark.kubernetes.operator.controller.SparkApplicationContext;
import org.apache.spark.kubernetes.operator.health.SentinelManager;
import org.apache.spark.kubernetes.operator.status.ApplicationState;
import org.apache.spark.kubernetes.operator.status.ApplicationStateSummary;
import org.apache.spark.kubernetes.operator.status.ApplicationStatus;
import org.apache.spark.kubernetes.operator.utils.StatusRecorder;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

class SparkApplicationReconcilerTest {
  private StatusRecorder mockRecorder = mock(StatusRecorder.class);
  private SentinelManager<SparkApplication> mockSentinelManager = mock(SentinelManager.class);
  private KubernetesClient mockClient = mock(KubernetesClient.class);
  private Context<SparkApplication> mockContext = mock(Context.class);
  private Pod mockDriver = mock(Pod.class);
  SparkApplication app = new SparkApplication();
  SparkApplicationReconciler reconciler = new SparkApplicationReconciler(mockRecorder,
      mockSentinelManager);

  @BeforeEach
  public void beforeEach() {
    when(mockContext.getClient()).thenReturn(mockClient);
    doNothing().when(mockRecorder).removeCachedStatus(any(SparkApplication.class));
    doAnswer(invocation -> {
      app.setStatus(invocation.getArgument(1));
      return null;
    }).when(mockRecorder).persistStatus(any(SparkApplicationContext.class),
        any(ApplicationStatus.class));
  }

  @Test
  void testCleanupRunningApp() {
    try (MockedConstruction<SparkApplicationContext> mockAppContext = mockConstruction(
        SparkApplicationContext.class, (mock, context) -> {
          when(mock.getSparkApplication()).thenReturn(app);
          when(mock.getClient()).thenReturn(mockClient);
          when(mock.getDriverPod()).thenReturn(Optional.of(mockDriver));
          when(mock.getDriverPodSpec()).thenReturn(mockDriver);
          when(mock.getDriverPreResourcesSpec()).thenReturn(Collections.emptyList());
          when(mock.getDriverResourcesSpec()).thenReturn(Collections.emptyList());
        }); MockedStatic<SparkReconcilerUtils> utils =
             Mockito.mockStatic(SparkReconcilerUtils.class)) {
      // delete running app
      app.setStatus(app.getStatus().appendNewState(new ApplicationState(
          ApplicationStateSummary.RUNNING_HEALTHY, "")));
      DeleteControl deleteControl = reconciler.cleanup(app, mockContext);
      Assertions.assertFalse(deleteControl.isRemoveFinalizer());
      utils.verify(() -> SparkReconcilerUtils.deleteResourceIfExists(mockClient,
          mockDriver, false));
      Assertions.assertEquals(ApplicationStateSummary.RESOURCE_RELEASED,
          app.getStatus().getCurrentState().getCurrentStateSummary());

      // proceed delete for terminated app
      deleteControl = reconciler.cleanup(app, mockContext);
      Assertions.assertTrue(deleteControl.isRemoveFinalizer());
    }
  }
}
