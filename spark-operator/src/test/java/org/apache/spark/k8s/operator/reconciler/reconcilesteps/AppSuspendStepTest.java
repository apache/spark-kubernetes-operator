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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.fabric8.mockwebserver.http.RecordedRequest;
import io.javaoperatorsdk.operator.api.event.EventRecord;
import io.javaoperatorsdk.operator.api.event.EventType;
import io.javaoperatorsdk.operator.api.event.ResourceEventRecorder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.config.SparkOperatorConf;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadFactory;
import org.apache.spark.k8s.operator.kueue.v1beta2.Workload;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.spec.ResourceRetainPolicy;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.utils.EventUtils;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;
import org.apache.spark.k8s.operator.utils.TestUtils;
import org.apache.spark.k8s.operator.utils.Utils;

@EnableKubernetesMockClient(crud = true)
class AppSuspendStepTest {
  private static final String DRIVER = "app1-0-driver";
  private static final String EXECUTOR = "app1-0-exec-1";
  // The driver of the attempt after the one of DRIVER
  private static final String NEXT_DRIVER = "app1-1-driver";
  // Keeps a deleted pod in the mock server, like the grace period of a terminating pod
  private static final String FINALIZER = "example.com/finalizer";

  private KubernetesMockServer server;
  private KubernetesClient kubernetesClient;

  // The default of spark.kubernetes.operator.reconciler.suspendHoldRequeueIntervalSeconds
  private static final ReconcileProgress SUSPEND_HOLD_PROGRESS =
      ReconcileProgress.completeAndRequeueAfter(Duration.ofMinutes(30));

  // The deletion of each pod is observed by the pod informer, which reconciles again
  private static final ReconcileProgress WAITING_FOR_PODS_PROGRESS =
      ReconcileProgress.completeAndDefaultRequeue();

  private final SparkAppContext mockContext = mock(SparkAppContext.class);
  private final SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
  private final ResourceEventRecorder eventRecorder = mock(ResourceEventRecorder.class);

  @BeforeEach
  void enableKueue() {
    TestUtils.setConfigKey(SparkOperatorConf.KUEUE_ENABLED, true);
  }

  @AfterEach
  void disableKueue() {
    TestUtils.setConfigKey(SparkOperatorConf.KUEUE_ENABLED, false);
  }

  @ParameterizedTest
  @EnumSource(
      value = ApplicationStateSummary.class,
      from = "DriverRequested",
      to = "RunningWithBelowThresholdExecutors")
  void runningAppWithoutSuspendProceeds(ApplicationStateSummary summary) {
    SparkApplication app = buildApp(summary, false);
    stubContext(app);
    createDriver(app);

    Assertions.assertEquals(
        ReconcileProgress.proceed(), new AppSuspendStep().reconcile(mockContext, recorder));

    verifyNoInteractions(recorder);
    Assertions.assertNotNull(getDriver());
  }

  @ParameterizedTest
  @EnumSource(
      value = ApplicationStateSummary.class,
      from = "DriverRequested",
      to = "RunningWithBelowThresholdExecutors")
  void suspendingRunningAppEntersSuspendedBeforeReleasingResources(
      ApplicationStateSummary summary) {
    SparkApplication app = buildKueueApp(summary, true);
    stubContext(app);
    createDriver(app);
    createWorkload(app);

    Assertions.assertEquals(
        ReconcileProgress.completeAndImmediateRequeue(),
        new AppSuspendStep().reconcile(mockContext, recorder));

    ApplicationState state = captureAppendedState();
    Assertions.assertEquals(ApplicationStateSummary.Suspended, state.getCurrentStateSummary());
    Assertions.assertEquals(Constants.APP_SUSPENDED_MESSAGE, state.getMessage());
    // Nothing is released until Suspended is persisted, so that an application which is resumed
    // in the meantime is never left running without its driver, which would fail it.
    Assertions.assertNotNull(getDriver());
    Assertions.assertNotNull(getWorkload());
  }

  @Test
  void suspendedStateWhichIsNotPersistedIsRetriedLater() {
    SparkApplication app = buildApp(ApplicationStateSummary.RunningHealthy, true);
    stubContext(app);
    createDriver(app);
    when(recorder.appendNewStateAndPersist(any(), any())).thenReturn(false);

    // A status which is rejected, e.g. by a CRD without the Suspended state, is not retried at once
    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        new AppSuspendStep().reconcile(mockContext, recorder));

    Assertions.assertNotNull(getDriver());
  }

  @ParameterizedTest
  @ValueSource(strings = {"Succeeded", "Failed"})
  void attemptWhoseDriverEndedMeanwhileEndsAsUsual(String phase) {
    SparkApplication app = buildKueueApp(ApplicationStateSummary.RunningHealthy, true);
    stubContext(app);
    // The API server knows that the driver ended, while the informer cache which the driver
    // observers read may not show it yet
    kubernetesClient
        .resource(new PodBuilder(driver(app)).withNewStatus().withPhase(phase).endStatus().build())
        .create();
    createWorkload(app);

    Assertions.assertEquals(
        ReconcileProgress.completeAndImmediateRequeue(),
        new AppSuspendStep().reconcile(mockContext, recorder));

    // The attempt ends rather than being suspended and run again from scratch on resume
    Assertions.assertEquals(
        ApplicationStateSummary.valueOf(phase), captureAppendedState().getCurrentStateSummary());
    Assertions.assertNotNull(getDriver());
    Assertions.assertNotNull(getWorkload());
  }

  @Test
  void terminatingDriverOfEarlierAttemptDoesNotEndTheAttempt() {
    SparkApplication app = buildApp(ApplicationStateSummary.RunningHealthy, true);
    stubContext(app);
    // The failed driver of an earlier attempt, which is still terminating
    kubernetesClient
        .resource(
            withFinalizers(
                new PodBuilder(driver(app)).withNewStatus().withPhase("Failed").endStatus().build(),
                FINALIZER))
        .create();
    kubernetesClient.resource(driver(app)).delete();
    kubernetesClient.resource(pod(NEXT_DRIVER, Utils.driverLabels(app))).create();

    Assertions.assertEquals(
        ReconcileProgress.completeAndImmediateRequeue(),
        new AppSuspendStep().reconcile(mockContext, recorder));

    Assertions.assertEquals(
        ApplicationStateSummary.Suspended, captureAppendedState().getCurrentStateSummary());
  }

  @Test
  void suspensionWaitsUntilTheDriverCanBeLookedUp() {
    SparkApplication app = buildApp(ApplicationStateSummary.RunningHealthy, true);
    KubernetesClient client = spy(kubernetesClient);
    doThrow(new KubernetesClientException("Service Unavailable", 503, null)).when(client).pods();
    stubContext(app, client);

    // Whether the driver ended meanwhile is unknown, so the application is not suspended yet
    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        new AppSuspendStep().reconcile(mockContext, recorder));

    verifyNoInteractions(recorder, eventRecorder);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void suspendedAppKeepsKueueWorkloadWhilePodsRemain(boolean suspend) throws Exception {
    // Resuming waits as well, so that the new attempt does not run on the admission of the
    // suspended one
    SparkApplication app = buildKueueApp(ApplicationStateSummary.Suspended, suspend);
    stubContext(app);
    createDriver(app, FINALIZER);
    createWorkload(app);

    Assertions.assertEquals(
        WAITING_FOR_PODS_PROGRESS, new AppSuspendStep().reconcile(mockContext, recorder));

    // The driver is deleted with its executors and the resources it owns, without forcing it
    // within the grace period
    List<String> deletions = podDeleteOptions();
    Assertions.assertEquals(1, deletions.size(), deletions.toString());
    Assertions.assertTrue(deletions.get(0).contains("\"propagationPolicy\":\"Foreground\""));
    Assertions.assertFalse(deletions.get(0).contains("\"gracePeriodSeconds\":0"));
    Assertions.assertNotNull(getDriver().getMetadata().getDeletionTimestamp());
    // The driver is still terminating, so it keeps holding the quota of the Workload
    Assertions.assertNotNull(getWorkload());

    // The terminating driver is not deleted again while it is waited for
    Assertions.assertEquals(
        WAITING_FOR_PODS_PROGRESS, new AppSuspendStep().reconcile(mockContext, recorder));
    Assertions.assertEquals(List.of(), podDeleteOptions());
    verifyNoInteractions(recorder, eventRecorder);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void podsOfAppWithoutKueueAreWaitedForAsWell(boolean suspend) {
    // A resumed application starts its new attempt without a restart backoff, and the observers
    // of that attempt would take a driver of the suspended one for theirs
    TestUtils.setConfigKey(SparkOperatorConf.KUEUE_ENABLED, false);
    SparkApplication app = buildApp(ApplicationStateSummary.Suspended, suspend);
    stubContext(app);
    createDriver(app);
    createExecutor(app);

    Assertions.assertEquals(
        WAITING_FOR_PODS_PROGRESS, new AppSuspendStep().reconcile(mockContext, recorder));

    Assertions.assertNull(getDriver());
    verifyNoInteractions(recorder);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void podsAreWaitedForWhenTheReleaseStartsLate(boolean suspend) throws Exception {
    SparkApplication app = buildKueueApp(ApplicationStateSummary.Suspended, suspend);
    // Suspended for longer than forceTerminationGracePeriodMillis, 5 minutes by default, e.g. as
    // the operator was down meanwhile or the deletion kept failing
    app.getStatus()
        .getCurrentState()
        .setLastTransitionTime(Instant.now().minus(Duration.ofMinutes(6)).toString());
    stubContext(app);
    createDriver(app, FINALIZER);
    createExecutor(app);
    createWorkload(app);

    Assertions.assertEquals(
        WAITING_FOR_PODS_PROGRESS, new AppSuspendStep().reconcile(mockContext, recorder));

    // The wait is measured per pod, so the driver still gets its grace period, and the quota which
    // its executors occupy is not given back yet
    List<String> deletions = podDeleteOptions();
    Assertions.assertEquals(1, deletions.size(), deletions.toString());
    Assertions.assertTrue(deletions.get(0).contains("\"propagationPolicy\":\"Foreground\""));
    Assertions.assertFalse(deletions.get(0).contains("\"gracePeriodSeconds\":0"));
    Assertions.assertNotNull(getWorkload());
    verifyNoInteractions(recorder, eventRecorder);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void liveDriverIsReleasedBesideTerminatingDriverOfEarlierAttempt(boolean suspend)
      throws Exception {
    SparkApplication app = buildKueueApp(ApplicationStateSummary.Suspended, suspend);
    stubContext(app);
    // The driver of an earlier attempt, which is still terminating, e.g. on a lost node
    createDriver(app, FINALIZER);
    kubernetesClient.resource(driver(app)).delete();
    kubernetesClient.resource(pod(NEXT_DRIVER, Utils.driverLabels(app))).create();
    createWorkload(app);
    Assertions.assertEquals(1, podDeleteOptions().size());

    Assertions.assertEquals(
        WAITING_FOR_PODS_PROGRESS, new AppSuspendStep().reconcile(mockContext, recorder));

    // Every driver is looked at, so the one of the suspended attempt is deleted as well, while the
    // terminating one is not deleted again within its grace period
    List<String> deletions = podDeleteOptions();
    Assertions.assertEquals(1, deletions.size(), deletions.toString());
    Assertions.assertTrue(deletions.get(0).contains("\"propagationPolicy\":\"Foreground\""));
    Assertions.assertNull(getPod(NEXT_DRIVER));
    Assertions.assertNotNull(getWorkload());
    verifyNoInteractions(recorder, eventRecorder);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void podsWhichOutliveTheGracePeriodNoLongerHoldTheRelease(boolean suspend) throws Exception {
    SparkApplication app = buildKueueApp(ApplicationStateSummary.Suspended, suspend);
    // The wait for a pod ends this long after its own grace period ended, at its deletionTimestamp
    app.getSpec()
        .getApplicationTolerations()
        .getApplicationTimeoutConfig()
        .setForceTerminationGracePeriodMillis(0L);
    stubContext(app);
    createDriver(app, FINALIZER);
    createExecutor(app, FINALIZER);
    createWorkload(app);
    // Pods which are still terminating, e.g. on a lost node
    kubernetesClient.resource(driver(app)).delete();
    kubernetesClient.resource(executor(app)).delete();
    Assertions.assertEquals(2, podDeleteOptions().size());

    Assertions.assertEquals(
        suspend ? SUSPEND_HOLD_PROGRESS : ReconcileProgress.completeAndImmediateRequeue(),
        new AppSuspendStep().reconcile(mockContext, recorder));

    // The terminating driver is force deleted, so that it no longer keeps its pod object
    List<String> deletions = podDeleteOptions();
    Assertions.assertEquals(1, deletions.size(), deletions.toString());
    Assertions.assertTrue(deletions.get(0).contains("\"gracePeriodSeconds\":0"));
    Assertions.assertNull(getWorkload());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void waitIsRequeuedAtTheNextPodDeadline(boolean suspend) {
    SparkApplication app = buildKueueApp(ApplicationStateSummary.Suspended, suspend);
    app.getSpec()
        .getApplicationTolerations()
        .getApplicationTimeoutConfig()
        .setForceTerminationGracePeriodMillis(Duration.ofMinutes(1).toMillis());
    stubContext(app);
    // A driver which is still terminating, e.g. on a lost node, and its executor which is not
    // deleted yet
    createDriver(app, FINALIZER);
    kubernetesClient.resource(driver(app)).delete();
    createExecutor(app);
    createWorkload(app);

    ReconcileProgress progress = new AppSuspendStep().reconcile(mockContext, recorder);

    // The wait is requeued to end when the driver has been terminating for the timeout, rather
    // than after the default interval, since a driver on a lost node may send no more events
    Assertions.assertTrue(progress.isCompleted());
    Duration requeueAfter = progress.getRequeueAfterDuration();
    Assertions.assertTrue(
        requeueAfter.compareTo(Duration.ofSeconds(30)) > 0
            && requeueAfter.compareTo(Duration.ofMinutes(1)) <= 0,
        requeueAfter::toString);
    Assertions.assertNotNull(getWorkload());
    verifyNoInteractions(recorder, eventRecorder);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void waitIsNotRequeuedAtPassedPodDeadlines(boolean suspend) {
    SparkApplication app = buildApp(ApplicationStateSummary.Suspended, suspend);
    app.getSpec()
        .getApplicationTolerations()
        .getApplicationTimeoutConfig()
        .setForceTerminationGracePeriodMillis(0L);
    stubContext(app);
    createDriver(app);
    // An executor which is still terminating, e.g. on a lost node
    createExecutor(app, FINALIZER);
    kubernetesClient.resource(executor(app)).delete();

    // Only the driver, whose deletion is observed by the pod informer, holds the application, so
    // the executor which no longer holds it does not requeue the wait at once
    Assertions.assertEquals(
        WAITING_FOR_PODS_PROGRESS, new AppSuspendStep().reconcile(mockContext, recorder));
    Assertions.assertNull(getDriver());
  }

  @ParameterizedTest
  @EnumSource(ResourceRetainPolicy.class)
  void suspendedAppReleasesKueueWorkloadAfterPodsAreGone(ResourceRetainPolicy policy) {
    SparkApplication app = buildKueueApp(ApplicationStateSummary.Suspended, true);
    app.getSpec().getApplicationTolerations().setResourceRetainPolicy(policy);
    stubContext(app);
    createDriver(app);
    createWorkload(app);

    // The driver which is deleted is waited for
    Assertions.assertEquals(
        WAITING_FOR_PODS_PROGRESS, new AppSuspendStep().reconcile(mockContext, recorder));
    Assertions.assertNull(getDriver());
    Assertions.assertNotNull(getWorkload());

    // The quota is given back whatever the retain policy is, and a resumed application starts a
    // new attempt, which does not reuse the resources of the suspended one
    Assertions.assertEquals(
        SUSPEND_HOLD_PROGRESS, new AppSuspendStep().reconcile(mockContext, recorder));

    Assertions.assertNull(getWorkload());
    // Unlike an application held before its driver is requested, its status says that it is
    // suspended, so no SuspendHeld event is published
    verifyNoInteractions(recorder, eventRecorder);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void workloadIsReleasedAfterQueueLabelIsRemoved(boolean suspend) {
    SparkApplication app = buildKueueApp(ApplicationStateSummary.Suspended, suspend);
    createWorkload(app);
    app.getMetadata().setLabels(Map.of());
    stubContext(app);

    new AppSuspendStep().reconcile(mockContext, recorder);

    // The Workload admitted before the label was removed neither keeps the quota while suspended,
    // nor lends its admission to a resumed application whose label is added back
    Assertions.assertNull(getWorkload());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void kueueWorkloadIsNotAccessedWhenKueueIsDisabled(boolean suspend) {
    SparkApplication app = buildKueueApp(ApplicationStateSummary.Suspended, suspend);
    createWorkload(app);
    KubernetesClient client = spy(kubernetesClient);
    stubContext(app, client);
    TestUtils.setConfigKey(SparkOperatorConf.KUEUE_ENABLED, false);

    Assertions.assertEquals(
        suspend ? SUSPEND_HOLD_PROGRESS : ReconcileProgress.completeAndImmediateRequeue(),
        new AppSuspendStep().reconcile(mockContext, recorder));

    // The queue label is ignored, and neither the suspended nor the resumed application calls Kueue
    verify(client, never()).resources(Workload.class);
    Assertions.assertNotNull(getWorkload());
  }

  @ParameterizedTest
  @ValueSource(ints = {403, 429, 500})
  void persistentReleaseFailureIsReportedAndRetried(int code) {
    SparkApplication app = buildKueueApp(ApplicationStateSummary.Suspended, false);
    createDriver(app);
    KubernetesClient client = spy(kubernetesClient);
    // Unlike a timeout or a 503, these do not clear on their own, e.g. an admission webhook which
    // is down keeps failing with 500, so they are reported
    doThrow(new KubernetesClientException("failed", code, null))
        .when(client)
        .resource(any(Pod.class));
    stubContext(app, client);

    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        new AppSuspendStep().reconcile(mockContext, recorder));

    EventRecord event = captureEvent();
    Assertions.assertEquals(EventType.WARNING, event.type());
    Assertions.assertEquals(EventUtils.REASON_SUSPEND_RELEASE_FAILED, event.reason());
    // It stays Suspended rather than resuming on what it still holds
    verify(client, never()).resources(Workload.class);
    verifyNoInteractions(recorder);
  }

  @Test
  void transientReleaseFailureIsRetriedWithoutReport() {
    SparkApplication app = buildKueueApp(ApplicationStateSummary.Suspended, false);
    createDriver(app);
    KubernetesClient client = spy(kubernetesClient);
    doThrow(new KubernetesClientException("Service Unavailable", 503, null))
        .when(client)
        .resource(any(Pod.class));
    stubContext(app, client);

    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        new AppSuspendStep().reconcile(mockContext, recorder));

    verify(client, never()).resources(Workload.class);
    verifyNoInteractions(recorder, eventRecorder);
  }

  @Test
  void failedPodListIsRetriedWithoutReleasingKueueWorkload() {
    SparkApplication app = buildKueueApp(ApplicationStateSummary.Suspended, false);
    createWorkload(app);
    KubernetesClient client = spy(kubernetesClient);
    doThrow(new KubernetesClientException("Service Unavailable", 503, null)).when(client).pods();
    stubContext(app, client);

    // Whether the pods are gone is unknown, so the Workload is kept and nothing is resumed
    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        new AppSuspendStep().reconcile(mockContext, recorder));

    Assertions.assertNotNull(getWorkload());
    verifyNoInteractions(recorder, eventRecorder);
  }

  @Test
  void failedKueueWorkloadReleaseIsReportedAndRetried() {
    SparkApplication app = buildKueueApp(ApplicationStateSummary.Suspended, false);
    stubContext(app);
    createWorkload(app);
    server
        .expect()
        .delete()
        .withPath("/apis/kueue.x-k8s.io/v1beta2/namespaces/default/workloads/sparkapplication-app1")
        .andReturn(403, null)
        .once();

    // The quota is not given back yet, so the application is neither held for the hold interval
    // nor resumed on the admission of the suspended attempt
    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        new AppSuspendStep().reconcile(mockContext, recorder));

    Assertions.assertNotNull(getWorkload());
    Assertions.assertEquals(EventUtils.REASON_SUSPEND_RELEASE_FAILED, captureEvent().reason());
    verifyNoInteractions(recorder);
  }

  @Test
  void resumingSuspendedAppStartsNewAttemptOnceEverythingIsReleased() {
    SparkApplication app = buildKueueApp(ApplicationStateSummary.Suspended, false);
    stubContext(app);
    // Resumed before anything was released: the driver and the Workload are still there
    createDriver(app);
    createWorkload(app);
    long lastStateId = app.getStatus().getStateTransitionHistory().lastKey();
    long attemptId = app.getStatus().getCurrentAttemptSummary().getAttemptInfo().getId();

    // The driver which is deleted is waited for, along with the Workload
    Assertions.assertEquals(
        WAITING_FOR_PODS_PROGRESS, new AppSuspendStep().reconcile(mockContext, recorder));
    Assertions.assertNull(getDriver());
    Assertions.assertNotNull(getWorkload());
    verifyNoInteractions(recorder);

    Assertions.assertEquals(
        ReconcileProgress.completeAndImmediateRequeue(),
        new AppSuspendStep().reconcile(mockContext, recorder));

    ArgumentCaptor<ApplicationStatus> status = ArgumentCaptor.forClass(ApplicationStatus.class);
    verify(recorder).persistStatus(eq(mockContext), status.capture());
    ApplicationState state = status.getValue().getCurrentState();
    Assertions.assertEquals(ApplicationStateSummary.Submitted, state.getCurrentStateSummary());
    Assertions.assertEquals(Constants.APP_RESUMED_MESSAGE, state.getMessage());
    // The history of the suspended attempt moves to the previous attempt, like on a restart
    Assertions.assertEquals(
        Map.of(lastStateId + 1, state), status.getValue().getStateTransitionHistory());
    // A new attempt, see ApplicationStatus#resume
    Assertions.assertEquals(
        attemptId + 1, status.getValue().getCurrentAttemptSummary().getAttemptInfo().getId());
    // Whatever the Workload was admitted for, AppInitStep requests a new admission from scratch
    Assertions.assertNull(getWorkload());
  }

  @Test
  void resumeWhichIsNotPersistedIsRetriedLater() {
    SparkApplication app = buildApp(ApplicationStateSummary.Suspended, false);
    stubContext(app);
    when(recorder.persistStatus(any(), any())).thenReturn(false);

    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        new AppSuspendStep().reconcile(mockContext, recorder));
  }

  @ParameterizedTest
  @EnumSource(
      value = ApplicationStateSummary.class,
      mode = EnumSource.Mode.EXCLUDE,
      names = {
        "DriverRequested",
        "DriverStarted",
        "DriverReady",
        "InitializedBelowThresholdExecutors",
        "RunningHealthy",
        "RunningWithPartialCapacity",
        "RunningWithBelowThresholdExecutors",
        "Suspended"
      })
  void otherStatesProceed(ApplicationStateSummary summary) {
    // An initializing application is held by AppInitStep, and a stopping one stops as usual
    SparkApplication app = buildApp(summary, true);
    stubContext(app);
    createDriver(app);

    Assertions.assertEquals(
        ReconcileProgress.proceed(), new AppSuspendStep().reconcile(mockContext, recorder));

    verifyNoInteractions(recorder);
    Assertions.assertNotNull(getDriver());
  }

  private void stubContext(SparkApplication app) {
    stubContext(app, kubernetesClient);
  }

  private void stubContext(SparkApplication app, KubernetesClient client) {
    when(recorder.appendNewStateAndPersist(any(), any())).thenReturn(true);
    when(recorder.persistStatus(any(), any())).thenReturn(true);
    when(mockContext.getResource()).thenReturn(app);
    when(mockContext.getClient()).thenReturn(client);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);
  }

  private ApplicationState captureAppendedState() {
    ArgumentCaptor<ApplicationState> captor = ArgumentCaptor.forClass(ApplicationState.class);
    verify(recorder).appendNewStateAndPersist(eq(mockContext), captor.capture());
    verify(recorder, never()).persistStatus(any(), any());
    return captor.getValue();
  }

  private EventRecord captureEvent() {
    ArgumentCaptor<EventRecord> event = ArgumentCaptor.forClass(EventRecord.class);
    verify(eventRecorder).record(event.capture());
    return event.getValue();
  }

  /** Returns the delete options of the pod deletions which the mock server received. */
  private List<String> podDeleteOptions() throws InterruptedException {
    List<String> deletions = new ArrayList<>();
    RecordedRequest request = server.takeRequest(0, TimeUnit.SECONDS);
    while (request != null) {
      if ("DELETE".equals(request.getMethod()) && request.getPath().contains("/pods/")) {
        deletions.add(request.getUtf8Body());
      }
      request = server.takeRequest(0, TimeUnit.SECONDS);
    }
    return deletions;
  }

  private Pod getDriver() {
    return getPod(DRIVER);
  }

  private Pod getPod(String name) {
    return kubernetesClient.pods().inNamespace("default").withName(name).get();
  }

  private Workload getWorkload() {
    return kubernetesClient
        .resources(Workload.class)
        .inNamespace("default")
        .withName("sparkapplication-app1")
        .get();
  }

  private void createDriver(SparkApplication app, String... finalizers) {
    kubernetesClient.resource(withFinalizers(driver(app), finalizers)).create();
  }

  private void createExecutor(SparkApplication app, String... finalizers) {
    kubernetesClient.resource(withFinalizers(executor(app), finalizers)).create();
  }

  private void createWorkload(SparkApplication app) {
    kubernetesClient.resource(KueueWorkloadFactory.buildWorkload(app)).create();
  }

  private static Pod driver(SparkApplication app) {
    return pod(DRIVER, Utils.driverLabels(app));
  }

  private static Pod executor(SparkApplication app) {
    return pod(EXECUTOR, Utils.executorLabels(app));
  }

  private static Pod pod(String name, Map<String, String> labels) {
    return new PodBuilder()
        .withNewMetadata()
        .withName(name)
        .withNamespace("default")
        .withLabels(labels)
        .endMetadata()
        .build();
  }

  private static Pod withFinalizers(Pod pod, String... finalizers) {
    return new PodBuilder(pod).editMetadata().withFinalizers(finalizers).endMetadata().build();
  }

  private static SparkApplication buildApp(ApplicationStateSummary summary, boolean suspend) {
    SparkApplication app = new SparkApplication();
    app.setMetadata(
        new ObjectMetaBuilder()
            .withName("app1")
            .withNamespace("default")
            .withUid("app-uid")
            .build());
    app.getSpec().setSuspend(suspend);
    app.setStatus(app.getStatus().appendNewState(new ApplicationState(summary, "")));
    return app;
  }

  private static SparkApplication buildKueueApp(ApplicationStateSummary summary, boolean suspend) {
    SparkApplication app = buildApp(summary, suspend);
    app.getMetadata().setLabels(Map.of(Constants.LABEL_QUEUE_NAME, "test-queue"));
    return app;
  }
}
