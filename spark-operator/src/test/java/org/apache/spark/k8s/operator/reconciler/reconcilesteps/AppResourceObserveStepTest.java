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

import java.util.List;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.SparkAppSubmissionWorker;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.reconciler.observers.AppDriverReadyObserver;
import org.apache.spark.k8s.operator.reconciler.observers.AppDriverStartObserver;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;
import org.apache.spark.k8s.operator.utils.Utils;

class AppResourceObserveStepTest {
  private SparkApplication app;
  private Context<SparkApplication> josdkContext;
  private SparkAppContext context;
  private SparkAppStatusRecorder recorder;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    app = new SparkApplication();
    app.setMetadata(new ObjectMetaBuilder().withName("app").withNamespace("default").build());
    app.setSpec(new ApplicationSpec());
    app.setStatus(
        new ApplicationStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, "")));
    josdkContext = mock(Context.class);
    context = new SparkAppContext(app, josdkContext, mock(SparkAppSubmissionWorker.class));
    recorder = mock(SparkAppStatusRecorder.class);
    when(recorder.persistStatus(any(SparkAppContext.class), any(ApplicationStatus.class)))
        .thenAnswer(
            invocation -> {
              app.setStatus(invocation.getArgument(1));
              return true;
            });
  }

  @Test
  void driverSucceededBeforeFirstObservationIsRecordedAsSucceededOnly() {
    assertEquals(
        List.of(ApplicationStateSummary.Succeeded), observeTerminatedDriver("Succeeded", 0));
  }

  @Test
  void driverFailedBeforeFirstObservationIsRecordedAsFailedOnce() {
    assertEquals(List.of(ApplicationStateSummary.Failed), observeTerminatedDriver("Failed", 1));
  }

  private List<ApplicationStateSummary> observeTerminatedDriver(String phase, int exitCode) {
    Pod driver =
        new PodBuilder()
            .withNewMetadata()
            .withName("app-driver")
            .withNamespace("default")
            .withLabels(Utils.driverLabels(app))
            .endMetadata()
            .withNewStatus()
            .withPhase(phase)
            .addNewContainerStatus()
            .withName("spark-kubernetes-driver")
            .withReady(false)
            .withRestartCount(0)
            .withNewState()
            .withNewTerminated()
            .withExitCode(exitCode)
            .endTerminated()
            .endState()
            .endContainerStatus()
            .endStatus()
            .build();
    when(josdkContext.getSecondaryResourcesAsStream(Pod.class))
        .thenAnswer(invocation -> List.of(driver).stream());
    int previousSize = app.getStatus().getStateTransitionHistory().size();

    new AppResourceObserveStep(List.of(new AppDriverStartObserver(), new AppDriverReadyObserver()))
        .reconcile(context, recorder);

    return app.getStatus().getStateTransitionHistory().values().stream()
        .skip(previousSize)
        .map(ApplicationState::getCurrentStateSummary)
        .toList();
  }
}
