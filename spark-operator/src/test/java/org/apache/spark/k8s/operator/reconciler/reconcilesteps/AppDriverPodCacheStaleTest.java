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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkAppSubmissionWorker;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.reconciler.observers.AppDriverStartObserver;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;

@EnableKubernetesMockClient(crud = true)
@SuppressFBWarnings(
    value = {"UWF_UNWRITTEN_FIELD", "NP_UNWRITTEN_FIELD", "UUF_UNUSED_FIELD"},
    justification = "Fields are written by the Kubernetes mock client extension.")
class AppDriverPodCacheStaleTest {
  private static final String APP_NAME = "stale-cache-app";
  private static final String NAMESPACE = "default";
  private static final String OPERATOR_NAME = "spark-kubernetes-operator";
  private static final String DRIVER_POD_NAME = "stale-cache-app-driver";

  private KubernetesMockServer mockServer;
  private KubernetesClient kubernetesClient;

  private SparkApplication app;
  private SparkAppContext context;
  private SparkAppStatusRecorder recorder;

  @BeforeEach
  void setUp() {
    app = new SparkApplication();
    app.setMetadata(
        new ObjectMetaBuilder().withName(APP_NAME).withNamespace(NAMESPACE).build());
    app.setSpec(new ApplicationSpec());
    app.setStatus(new ApplicationStatus());

    @SuppressWarnings("unchecked")
    Context<SparkApplication> josdkContext = mock(Context.class);
    when(josdkContext.getClient()).thenReturn(kubernetesClient);
    // Simulate a stale informer: secondary resource cache returns no pods even though
    // the apiserver may have the driver pod.
    when(josdkContext.getSecondaryResourcesAsStream(Pod.class)).thenReturn(Stream.empty());

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
  void staleCacheWithHealthyDriverOnApiServerDoesNotFailTheApplication() {
    createDriverPodOnApiServer();
    setCurrentStateAged(ApplicationStateSummary.DriverRequested, Duration.ofMinutes(2));

    AppResourceObserveStep step =
        new AppResourceObserveStep(List.of(new AppDriverStartObserver()));
    step.reconcile(context, recorder);

    ApplicationStateSummary post = app.getStatus().getCurrentState().getCurrentStateSummary();
    assertNotEquals(
        ApplicationStateSummary.Failed,
        post,
        "Stale informer cache must not cause the SparkApp to fail when the driver "
            + "pod is healthy on the apiserver.");
  }

  @Test
  void youngStateWithCacheMissDefersVerification() {
    setCurrentStateAged(ApplicationStateSummary.DriverRequested, Duration.ofSeconds(5));

    AppResourceObserveStep step =
        new AppResourceObserveStep(List.of(new AppDriverStartObserver()));
    step.reconcile(context, recorder);

    assertEquals(
        ApplicationStateSummary.DriverRequested,
        app.getStatus().getCurrentState().getCurrentStateSummary(),
        "Cache miss within grace period must not advance state.");
  }

  @Test
  void agedStateWithCacheAndApiServerEmptyTransitionsToFailed() {
    setCurrentStateAged(ApplicationStateSummary.DriverRequested, Duration.ofMinutes(2));

    AppResourceObserveStep step =
        new AppResourceObserveStep(List.of(new AppDriverStartObserver()));
    step.reconcile(context, recorder);

    assertEquals(
        ApplicationStateSummary.Failed,
        app.getStatus().getCurrentState().getCurrentStateSummary(),
        "Genuine driver removal (cache and apiserver agree pod is gone) must still "
            + "transition the SparkApp to Failed.");
  }

  private void createDriverPodOnApiServer() {
    Pod driverPod =
        new PodBuilder()
            .withNewMetadata()
            .withName(DRIVER_POD_NAME)
            .withNamespace(NAMESPACE)
            .withLabels(
                Map.of(
                    Constants.LABEL_SPARK_OPERATOR_NAME, OPERATOR_NAME,
                    Constants.LABEL_SPARK_APPLICATION_NAME, APP_NAME,
                    Constants.LABEL_SPARK_ROLE_NAME, Constants.LABEL_SPARK_ROLE_DRIVER_VALUE))
            .endMetadata()
            .withNewStatus()
            .withPhase("Running")
            .endStatus()
            .build();
    kubernetesClient.pods().inNamespace(NAMESPACE).resource(driverPod).create();
  }

  private void setCurrentStateAged(ApplicationStateSummary summary, Duration age) {
    ApplicationState state = new ApplicationState(summary, summary.name());
    state.setLastTransitionTime(Instant.now().minus(age).toString());
    app.setStatus(app.getStatus().appendNewState(state));
  }
}
