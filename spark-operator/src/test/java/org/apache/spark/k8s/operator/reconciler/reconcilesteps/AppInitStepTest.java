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

import static org.apache.spark.k8s.operator.utils.Utils.driverLabels;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.net.SocketException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Stream;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ConditionBuilder;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.FieldsV1;
import io.fabric8.kubernetes.api.model.ManagedFieldsEntry;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.scheduling.v1.PriorityClassList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable;
import io.fabric8.kubernetes.client.dsl.NamespaceableResource;
import io.fabric8.kubernetes.client.dsl.ServerSideApplicable;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.javaoperatorsdk.operator.api.event.EventRecord;
import io.javaoperatorsdk.operator.api.event.EventType;
import io.javaoperatorsdk.operator.api.event.ResourceEventRecorder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkAppSubmissionWorker;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.config.SparkOperatorConf;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.kueue.KueuePodSetFlavor;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadFactory;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadUtils;
import org.apache.spark.k8s.operator.kueue.v1beta2.Admission;
import org.apache.spark.k8s.operator.kueue.v1beta2.PodSetAssignment;
import org.apache.spark.k8s.operator.kueue.v1beta2.ResourceFlavor;
import org.apache.spark.k8s.operator.kueue.v1beta2.ResourceFlavorSpec;
import org.apache.spark.k8s.operator.kueue.v1beta2.Workload;
import org.apache.spark.k8s.operator.kueue.v1beta2.WorkloadStatus;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.spec.ApplicationTolerations;
import org.apache.spark.k8s.operator.spec.DeploymentMode;
import org.apache.spark.k8s.operator.spec.RestartConfig;
import org.apache.spark.k8s.operator.status.ApplicationAttemptSummary;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.utils.EventUtils;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;
import org.apache.spark.k8s.operator.utils.TestUtils;

@EnableKubernetesMockClient(crud = true)
@SuppressFBWarnings(
    value = {"UWF_UNWRITTEN_FIELD", "NP_UNWRITTEN_FIELD", "UUF_UNUSED_FIELD"},
    justification = "Unwritten fields are covered by Kubernetes mock client")
class AppInitStepTest {
  private KubernetesMockServer mockServer;
  private KubernetesClient kubernetesClient;

  private final ResourceEventRecorder eventRecorder = mock(ResourceEventRecorder.class);

  // The default of spark.kubernetes.operator.reconciler.suspendHoldRequeueIntervalSeconds, which
  // docs/configuration.md and docs/spark_custom_resources.md both state as 30 minutes.
  private static final ReconcileProgress SUSPEND_HOLD_PROGRESS =
      ReconcileProgress.completeAndRequeueAfter(Duration.ofMinutes(30));

  private final ConfigMap preResourceConfigMapSpec =
      new ConfigMapBuilder()
          .withNewMetadata()
          .withName("pre-configmap")
          .withNamespace("default")
          .endMetadata()
          .withData(Map.of("foo1", "bar1"))
          .build();

  private final ConfigMap resourceConfigMapSpec =
      new ConfigMapBuilder()
          .withNewMetadata()
          .withName("resource-configmap")
          .withNamespace("default")
          .endMetadata()
          .withData(Map.of("foo", "bar"))
          .build();
  private final Pod driverPodSpec =
      new PodBuilder()
          .withNewMetadata()
          .withName("driver-pod")
          .withNamespace("default")
          .endMetadata()
          .editOrNewSpec()
          .addNewContainer()
          .withName("driver-container")
          .withImage("spark")
          .endContainer()
          .endSpec()
          .build();

  private final ObjectMeta applicationMetadata =
      new ObjectMetaBuilder().withName("sparkapp1").withNamespace("default").build();

  private final ObjectMeta kueueApplicationMetadata =
      new ObjectMetaBuilder()
          .withName("sparkapp1")
          .withNamespace("default")
          .withUid("app-uid")
          .withLabels(Map.of(Constants.LABEL_QUEUE_NAME, "test-queue"))
          .build();

  @BeforeEach
  void enableKueue() {
    TestUtils.setConfigKey(SparkOperatorConf.KUEUE_ENABLED, true);
  }

  @AfterEach
  void disableKueue() {
    TestUtils.setConfigKey(SparkOperatorConf.KUEUE_ENABLED, false);
  }

  @Test
  void driverResourcesHaveOwnerReferencesToDriver() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mocksparkAppContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(applicationMetadata);
    when(mocksparkAppContext.getResource()).thenReturn(application);
    when(mocksparkAppContext.getDriverPreResourcesSpec()).thenReturn(List.of());
    when(mocksparkAppContext.getDriverPodSpec()).thenReturn(driverPodSpec);
    when(mocksparkAppContext.getDriverResourcesSpec()).thenReturn(List.of(resourceConfigMapSpec));
    when(mocksparkAppContext.getClient()).thenReturn(kubernetesClient);
    when(recorder.appendNewStateAndPersist(any(), any())).thenReturn(true);
    when(recorder.persistStatus(any(), any())).thenReturn(true);
    ReconcileProgress reconcileProgress = appInitStep.reconcile(mocksparkAppContext, recorder);
    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), reconcileProgress);
    Pod createdPod = kubernetesClient.pods().inNamespace("default").withName("driver-pod").get();
    ConfigMap createCM =
        kubernetesClient.configMaps().inNamespace("default").withName("resource-configmap").get();
    Assertions.assertNotNull(createCM);
    Assertions.assertNotNull(createdPod);
    Assertions.assertEquals(1, createCM.getMetadata().getOwnerReferences().size());
    Assertions.assertEquals(
        createdPod.getMetadata().getName(),
        createCM.getMetadata().getOwnerReferences().get(0).getName());
    Assertions.assertEquals(
        createdPod.getMetadata().getUid(),
        createCM.getMetadata().getOwnerReferences().get(0).getUid());
    Assertions.assertEquals(
        createdPod.getKind(), createCM.getMetadata().getOwnerReferences().get(0).getKind());
  }

  @Test
  void createdPreResourcesPatchedWithOwnerReferencesToDriver() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mocksparkAppContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(applicationMetadata);
    when(mocksparkAppContext.getResource()).thenReturn(application);
    when(mocksparkAppContext.getDriverPreResourcesSpec())
        .thenReturn(List.of(preResourceConfigMapSpec));
    when(mocksparkAppContext.getDriverPodSpec()).thenReturn(driverPodSpec);
    when(mocksparkAppContext.getDriverResourcesSpec()).thenReturn(List.of());
    when(recorder.appendNewStateAndPersist(any(), any())).thenReturn(true);
    when(recorder.persistStatus(any(), any())).thenReturn(true);

    KubernetesClient mockClient = mock(KubernetesClient.class);
    when(mocksparkAppContext.getClient()).thenReturn(mockClient);

    ConfigMap createdConfigMap =
        new ConfigMapBuilder(preResourceConfigMapSpec)
            .editOrNewMetadata()
            .withManagedFields(
                new ManagedFieldsEntry(
                    "v1", "FieldsV1", new FieldsV1(), "foo", "foo", "foo", "foo"))
            .endMetadata()
            .build();
    Pod createdPod =
        new PodBuilder(driverPodSpec).editOrNewMetadata().withUid("foobar").endMetadata().build();

    NamespaceableResource<ConfigMap> mockCreatedNamespaceableResource =
        mock(NamespaceableResource.class);
    when(mockCreatedNamespaceableResource.get()).thenReturn(createdConfigMap);
    NamespaceableResource<Pod> mockCreatedPod = mock(NamespaceableResource.class);
    when(mockCreatedPod.get()).thenReturn(createdPod);

    when(mockClient.resource(preResourceConfigMapSpec))
        .thenReturn(mockCreatedNamespaceableResource);
    when(mockClient.resource(driverPodSpec)).thenReturn(mockCreatedPod);

    ServerSideApplicable mockServerSideApplicable = mock(ServerSideApplicable.class);
    NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable mockList =
        mock(NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable.class);
    when(mockClient.resourceList(anyList())).thenReturn(mockList);
    when(mockList.forceConflicts()).thenReturn(mockServerSideApplicable);

    ReconcileProgress reconcileProgress = appInitStep.reconcile(mocksparkAppContext, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), reconcileProgress);
    ArgumentCaptor<List<ConfigMap>> argument = ArgumentCaptor.forClass(List.class);
    verify(mockClient).resourceList(argument.capture());
    Assertions.assertEquals(1, argument.getValue().size());
    ConfigMap decoratedConfigMap = argument.getValue().get(0);
    Assertions.assertEquals(1, decoratedConfigMap.getMetadata().getOwnerReferences().size());
    Assertions.assertEquals(
        createdPod.getMetadata().getName(),
        decoratedConfigMap.getMetadata().getOwnerReferences().get(0).getName());
    Assertions.assertEquals(
        createdPod.getMetadata().getUid(),
        decoratedConfigMap.getMetadata().getOwnerReferences().get(0).getUid());
    Assertions.assertEquals(
        createdPod.getKind(),
        decoratedConfigMap.getMetadata().getOwnerReferences().get(0).getKind());
    Assertions.assertTrue(decoratedConfigMap.getMetadata().getManagedFields().isEmpty());
  }

  @Test
  void appInitStepShouldBeIdempotentWhenStatusUpdateFails() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mocksparkAppContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(applicationMetadata);
    when(mocksparkAppContext.getResource()).thenReturn(application);
    when(mocksparkAppContext.getDriverPreResourcesSpec()).thenReturn(List.of());
    when(mocksparkAppContext.getDriverPodSpec()).thenReturn(driverPodSpec);
    when(mocksparkAppContext.getDriverResourcesSpec()).thenReturn(List.of(resourceConfigMapSpec));
    when(mocksparkAppContext.getClient()).thenReturn(kubernetesClient);
    when(recorder.appendNewStateAndPersist(any(), any())).thenReturn(false, true);
    when(recorder.persistStatus(any(), any())).thenReturn(false, true);

    // If the first reconcile manages to create everything but fails to update status
    ReconcileProgress reconcileProgress1 = appInitStep.reconcile(mocksparkAppContext, recorder);
    Assertions.assertEquals(ReconcileProgress.completeAndImmediateRequeue(), reconcileProgress1);
    Pod createdPod = kubernetesClient.pods().inNamespace("default").withName("driver-pod").get();
    ConfigMap createCM =
        kubernetesClient.configMaps().inNamespace("default").withName("resource-configmap").get();
    Assertions.assertNotNull(createCM);
    Assertions.assertNotNull(createdPod);

    // The second reconcile shall update the status without re-creating everything
    ReconcileProgress reconcileProgress2 = appInitStep.reconcile(mocksparkAppContext, recorder);
    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), reconcileProgress2);
    createdPod = kubernetesClient.pods().inNamespace("default").withName("driver-pod").get();
    createCM =
        kubernetesClient.configMaps().inNamespace("default").withName("resource-configmap").get();
    Assertions.assertNotNull(createCM);
    Assertions.assertNotNull(createdPod);
  }

  @Test
  void banClientMode() {
    AppValidateStep appValidateStep = new AppValidateStep();
    SparkAppContext mocksparkAppContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(applicationMetadata);
    application.getSpec().setDeploymentMode(DeploymentMode.ClientMode);
    when(mocksparkAppContext.getResource()).thenReturn(application);

    appValidateStep.reconcile(mocksparkAppContext, recorder);
    ReconcileProgress progress = appValidateStep.reconcile(mocksparkAppContext, recorder);
    Assertions.assertEquals(ReconcileProgress.completeAndImmediateRequeue(), progress);
  }

  @Test
  void nonTrimModeRestartBackoffElapsedProceedsToDriverCreation() {
    // Non-trim mode: previousAttemptSummary has null stateTransitionHistory.
    // The fix falls back to the main history to resolve the stopping state before
    // ScheduledToRestart. With backoff elapsed the app should reach DriverRequested without NPE.
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);

    SparkApplication application = new SparkApplication();
    application.setMetadata(applicationMetadata);
    application.getSpec().setApplicationTolerations(
        ApplicationTolerations.builder()
            .restartConfig(RestartConfig.builder().restartBackoffMillis(5000L).build())
            .build());

    // Main history: DriverStartTimedOut → ScheduledToRestart entered 60s ago (backoff 5s elapsed)
    ApplicationState timedOutState =
        new ApplicationState(ApplicationStateSummary.DriverStartTimedOut, "timed out");
    ApplicationState scheduledState =
        new ApplicationState(ApplicationStateSummary.ScheduledToRestart, "restarting");
    scheduledState.setLastTransitionTime(Instant.now().minusMillis(60000L).toString());
    Map<Long, ApplicationState> history = new TreeMap<>();
    history.put(0L, timedOutState);
    history.put(1L, scheduledState);

    // Non-trim mode: previousAttemptSummary is present but has null stateTransitionHistory
    ApplicationStatus status = new ApplicationStatus(
        scheduledState, history,
        new ApplicationAttemptSummary(), new ApplicationAttemptSummary());
    application.setStatus(status);

    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getDriverPreResourcesSpec()).thenReturn(List.of());
    when(mockContext.getDriverPodSpec()).thenReturn(driverPodSpec);
    when(mockContext.getDriverResourcesSpec()).thenReturn(List.of());
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(recorder.persistStatus(any(), any())).thenAnswer(invocation -> {
      ApplicationStatus newStatus = invocation.getArgument(1);
      application.setStatus(newStatus);
      return true;
    });

    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    Assertions.assertEquals(
        ApplicationStateSummary.DriverRequested,
        application.getStatus().getCurrentState().getCurrentStateSummary());
  }

  @Test
  void nonTrimModeRestartBackoffActiveRequeuesWithDelay() {
    // Non-trim mode: backoff has NOT elapsed — should requeue with remaining delay,
    // not throw NullPointerException.
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);

    SparkApplication application = new SparkApplication();
    application.setMetadata(applicationMetadata);
    application.getSpec().setApplicationTolerations(
        ApplicationTolerations.builder()
            .restartConfig(RestartConfig.builder().restartBackoffMillis(60000L).build())
            .build());

    // ScheduledToRestart entered just now — 60s backoff has not elapsed
    ApplicationState timedOutState =
        new ApplicationState(ApplicationStateSummary.DriverStartTimedOut, "timed out");
    ApplicationState scheduledState =
        new ApplicationState(ApplicationStateSummary.ScheduledToRestart, "restarting");
    Map<Long, ApplicationState> history = new TreeMap<>();
    history.put(0L, timedOutState);
    history.put(1L, scheduledState);

    ApplicationStatus status = new ApplicationStatus(
        scheduledState, history,
        new ApplicationAttemptSummary(), new ApplicationAttemptSummary());
    application.setStatus(status);

    when(mockContext.getResource()).thenReturn(application);

    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    // Should requeue after the remaining backoff, not throw NPE
    Assertions.assertTrue(progress.isCompleted());
    Assertions.assertTrue(progress.isRequeue());
    Assertions.assertTrue(progress.getRequeueAfterDuration().toMillis() > 0);
    // State must remain ScheduledToRestart — no driver creation attempted
    Assertions.assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        application.getStatus().getCurrentState().getCurrentStateSummary());
  }

  @Test
  void suspendedAppDoesNotRequestDriver() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(applicationMetadata);
    application.getSpec().setSuspend(true);
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);
    when(mockContext.getClient()).thenReturn(kubernetesClient);

    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(SUSPEND_HOLD_PROGRESS, progress);
    verify(mockContext, never()).getDriverPreResourcesSpec();
    verify(mockContext, never()).getDriverPodSpec();
    // Only the Kueue Workload is looked up to be released, while no driver is created
    Assertions.assertTrue(
        kubernetesClient.pods().inNamespace("default").list().getItems().isEmpty());
    verifyNoInteractions(recorder);
    Assertions.assertEquals(
        ApplicationStateSummary.Submitted,
        application.getStatus().getCurrentState().getCurrentStateSummary());
    // The Submitted status of a suspended first attempt is never persisted, so the event is the
    // only signal users have
    EventRecord event = captureEvents(1).get(0);
    Assertions.assertEquals(EventType.NORMAL, event.type());
    Assertions.assertEquals(EventUtils.REASON_SUSPEND_HELD, event.reason());
    Assertions.assertEquals(
        "The SparkApplication is suspended by spec.suspend, driver would not be requested. "
            + "Set spec.suspend to false to resume it.",
        event.message());
  }

  @Test
  void suspendedAppScheduledToRestartDoesNotRequestDriver() {
    // ScheduledToRestart with an elapsed backoff: suspend takes precedence over restart.
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(applicationMetadata);
    application.getSpec().setSuspend(true);
    application.getSpec().setApplicationTolerations(
        ApplicationTolerations.builder()
            .restartConfig(RestartConfig.builder().restartBackoffMillis(5000L).build())
            .build());
    ApplicationState timedOutState =
        new ApplicationState(ApplicationStateSummary.DriverStartTimedOut, "timed out");
    ApplicationState scheduledState =
        new ApplicationState(ApplicationStateSummary.ScheduledToRestart, "restarting");
    scheduledState.setLastTransitionTime(Instant.now().minusMillis(60000L).toString());
    Map<Long, ApplicationState> history = new TreeMap<>();
    history.put(0L, timedOutState);
    history.put(1L, scheduledState);
    application.setStatus(new ApplicationStatus(
        scheduledState, history,
        new ApplicationAttemptSummary(), new ApplicationAttemptSummary()));
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);
    when(mockContext.getClient()).thenReturn(kubernetesClient);

    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(SUSPEND_HOLD_PROGRESS, progress);
    verify(mockContext, never()).getDriverPodSpec();
    // Only the Kueue Workload is looked up to be released, while no driver is created
    Assertions.assertTrue(
        kubernetesClient.pods().inNamespace("default").list().getItems().isEmpty());
    verifyNoInteractions(recorder);
    Assertions.assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        application.getStatus().getCurrentState().getCurrentStateSummary());
    // The persisted ScheduledToRestart status of the previous attempt says that a restart is due,
    // not that the next attempt is withheld by spec.suspend, so the same event is published
    EventRecord event = captureEvents(1).get(0);
    Assertions.assertEquals(EventType.NORMAL, event.type());
    Assertions.assertEquals(EventUtils.REASON_SUSPEND_HELD, event.reason());
  }

  @Test
  void unsuspendedAppRequestsDriverOnNextReconcile() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(applicationMetadata);
    application.getSpec().setSuspend(true);
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getDriverPreResourcesSpec()).thenReturn(List.of());
    when(mockContext.getDriverPodSpec()).thenReturn(driverPodSpec);
    when(mockContext.getDriverResourcesSpec()).thenReturn(List.of());
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);
    when(recorder.persistStatus(any(), any())).thenAnswer(invocation -> {
      ApplicationStatus newStatus = invocation.getArgument(1);
      application.setStatus(newStatus);
      return true;
    });

    // Suspended: nothing is created and the app stays Submitted
    ReconcileProgress progress1 = appInitStep.reconcile(mockContext, recorder);
    Assertions.assertEquals(SUSPEND_HOLD_PROGRESS, progress1);
    Assertions.assertNull(
        kubernetesClient.pods().inNamespace("default").withName("driver-pod").get());
    Assertions.assertEquals(
        ApplicationStateSummary.Submitted,
        application.getStatus().getCurrentState().getCurrentStateSummary());

    // Unsuspended: the regular init path requests the driver
    application.getSpec().setSuspend(false);
    ReconcileProgress progress2 = appInitStep.reconcile(mockContext, recorder);
    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress2);
    Assertions.assertNotNull(
        kubernetesClient.pods().inNamespace("default").withName("driver-pod").get());
    Assertions.assertEquals(
        ApplicationStateSummary.DriverRequested,
        application.getStatus().getCurrentState().getCurrentStateSummary());
    // The only event across both reconciles is the Suspended one of the first: resuming adds none
    // of its own. The DriverRequested transition event comes from the status recorder, which is
    // mocked here, so the e2e covers that part.
    EventRecord event = captureEvents(1).get(0);
    Assertions.assertEquals(EventUtils.REASON_SUSPEND_HELD, event.reason());
  }

  @Test
  void suspendedAppWithUnverifiableDriverIsNotHeld() {
    // A failed verification is not an answer: the driver of this attempt may be live, so the app
    // must not be held with an event claiming that none was requested, nor for the whole suspend
    // hold interval.
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(applicationMetadata);
    application.getSpec().setSuspend(true);
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getCurrentAttemptDriverPod())
        .thenThrow(new KubernetesClientException("unavailable", 503, null));
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    verify(mockContext, never()).getDriverPodSpec();
    verifyNoInteractions(recorder);
    verifyNoInteractions(eventRecorder);
    Assertions.assertEquals(
        ApplicationStateSummary.Submitted,
        application.getStatus().getCurrentState().getCurrentStateSummary());
  }

  @Test
  void suspendedAppPublishesEventOnEveryReconcile() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(applicationMetadata);
    application.getSpec().setSuspend(true);
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);
    when(mockContext.getClient()).thenReturn(kubernetesClient);

    // The event sink aggregates the repeats into one Event, and each repeat refreshes it, which
    // keeps the hold visible past the event retention: the suspended first attempt has no status
    // to fall back on.
    for (int i = 0; i < 3; i++) {
      Assertions.assertEquals(
          SUSPEND_HOLD_PROGRESS,
          appInitStep.reconcile(mockContext, recorder));
    }

    for (EventRecord event : captureEvents(3)) {
      Assertions.assertEquals(EventUtils.REASON_SUSPEND_HELD, event.reason());
    }
  }

  @Test
  void suspendedNonInitializingAppProceeds() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(applicationMetadata);
    application.getSpec().setSuspend(true);
    application.setStatus(
        application
            .getStatus()
            .appendNewState(
                new ApplicationState(ApplicationStateSummary.RunningHealthy, "running")));
    when(mockContext.getResource()).thenReturn(application);

    Assertions.assertEquals(
        ReconcileProgress.proceed(), appInitStep.reconcile(mockContext, recorder));
    verifyNoInteractions(recorder);
  }

  @Test
  void suspendAfterDriverRequestedCompletesInitialization() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(applicationMetadata);
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getDriverPreResourcesSpec()).thenReturn(List.of());
    when(mockContext.getDriverPodSpec()).thenReturn(driverPodSpec);
    when(mockContext.getDriverResourcesSpec()).thenReturn(List.of());
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getCurrentAttemptDriverPod())
        .thenAnswer(
            invocation ->
                Optional.ofNullable(
                    kubernetesClient.pods().inNamespace("default").withName("driver-pod").get()));
    when(recorder.persistStatus(any(), any())).thenReturn(false);

    // The driver is created but the status update to DriverRequested fails
    Assertions.assertEquals(
        ReconcileProgress.completeAndImmediateRequeue(),
        appInitStep.reconcile(mockContext, recorder));
    Assertions.assertNotNull(
        kubernetesClient.pods().inNamespace("default").withName("driver-pod").get());

    // The next reconcile restores Submitted from the cache while the app got suspended meanwhile
    application.setStatus(new ApplicationStatus());
    application.getSpec().setSuspend(true);
    when(recorder.persistStatus(any(), any())).thenAnswer(invocation -> {
      ApplicationStatus newStatus = invocation.getArgument(1);
      application.setStatus(newStatus);
      return true;
    });

    // The live driver of the current attempt takes precedence over the hold
    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        appInitStep.reconcile(mockContext, recorder));
    Assertions.assertEquals(
        ApplicationStateSummary.DriverRequested,
        application.getStatus().getCurrentState().getCurrentStateSummary());
  }

  @Test
  void previousAttemptDriverPodDoesNotBypassSuspend() {
    // A terminating pod of the previous attempt with the very same name (e.g. user-specified
    // spark.app.id) is still visible via getDriverPod() but is not the current attempt's driver.
    // SparkAppContextTest covers the selection itself.
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(applicationMetadata);
    application.getSpec().setSuspend(true);
    Pod previousAttemptDriver =
        new PodBuilder(driverPodSpec)
            .editOrNewMetadata()
            .withDeletionTimestamp(Instant.now().toString())
            .endMetadata()
            .build();
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getDriverPod()).thenReturn(Optional.of(previousAttemptDriver));
    when(mockContext.getCurrentAttemptDriverPod()).thenReturn(Optional.empty());
    when(mockContext.getDriverPodSpec()).thenReturn(driverPodSpec);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);
    when(mockContext.getClient()).thenReturn(kubernetesClient);

    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(SUSPEND_HOLD_PROGRESS, progress);
    // Only the Kueue Workload is looked up to be released, while no driver is created
    Assertions.assertTrue(
        kubernetesClient.pods().inNamespace("default").list().getItems().isEmpty());
    verifyNoInteractions(recorder);
    Assertions.assertEquals(
        ApplicationStateSummary.Submitted,
        application.getStatus().getCurrentState().getCurrentStateSummary());
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  void staleInformerSnapshotDoesNotBypassSuspend() {
    // Consecutive attempts reuse the driver pod name. Clean-up deleted the previous attempt's
    // driver and the app is scheduled to restart with the backoff elapsed, but the informer still
    // holds the pre-deletion snapshot (same name, no deletionTimestamp) while the API server has
    // already removed the pod. A suspended app must not create a new driver.
    AppInitStep appInitStep = new AppInitStep();
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(applicationMetadata);
    application.getSpec().setSuspend(true);
    application.getSpec().setApplicationTolerations(
        ApplicationTolerations.builder()
            .restartConfig(RestartConfig.builder().restartBackoffMillis(0L).build())
            .build());
    ApplicationState failedState = new ApplicationState(ApplicationStateSummary.Failed, "failed");
    ApplicationState scheduledState =
        new ApplicationState(ApplicationStateSummary.ScheduledToRestart, "restarting");
    Map<Long, ApplicationState> history = new TreeMap<>();
    history.put(0L, failedState);
    history.put(1L, scheduledState);
    application.setStatus(new ApplicationStatus(
        scheduledState, history,
        new ApplicationAttemptSummary(), new ApplicationAttemptSummary()));

    Pod stalePreviousAttemptDriver =
        new PodBuilder(driverPodSpec)
            .editOrNewMetadata()
            .withLabels(driverLabels(application))
            .endMetadata()
            .build();
    Context josdkContext = mock(Context.class);
    when(josdkContext.getSecondaryResourcesAsStream(Pod.class))
        .thenAnswer(invocation -> Stream.of(stalePreviousAttemptDriver));
    when(josdkContext.getClient()).thenReturn(kubernetesClient);
    SparkAppContext context =
        spy(new SparkAppContext(application, josdkContext, mock(SparkAppSubmissionWorker.class)));
    doReturn(driverPodSpec).when(context).getDriverPodSpec();
    doReturn(List.of()).when(context).getDriverPreResourcesSpec();
    doReturn(List.of()).when(context).getDriverResourcesSpec();
    doReturn(eventRecorder).when(context).getEventRecorder();

    ReconcileProgress progress = appInitStep.reconcile(context, recorder);

    Assertions.assertEquals(
        SUSPEND_HOLD_PROGRESS,
        progress);
    Assertions.assertNull(
        kubernetesClient.pods().inNamespace("default").withName("driver-pod").get());
    verifyNoInteractions(recorder);
    Assertions.assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        application.getStatus().getCurrentState().getCurrentStateSummary());
  }

  @Test
  void kueueWorkloadIsCreatedAndDriverIsHeldUntilAdmitted() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(kueueApplicationMetadata);
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    Workload workload = getWorkload();
    Assertions.assertNotNull(workload);
    Assertions.assertEquals("test-queue", workload.getSpec().getQueueName());
    Assertions.assertTrue(workload.getSpec().getActive());
    verify(mockContext, never()).getDriverPodSpec();
    verifyNoInteractions(recorder);
    EventRecord event = captureEvents(1).get(0);
    Assertions.assertEquals(EventType.NORMAL, event.type());
    Assertions.assertEquals(EventUtils.REASON_KUEUE_ADMISSION_PENDING, event.reason());
    Assertions.assertTrue(
        event.message().contains("sparkapplication-sparkapp1")
            && event.message().contains("test-queue"),
        event.message());
    Assertions.assertEquals(
        ApplicationStateSummary.Submitted,
        application.getStatus().getCurrentState().getCurrentStateSummary());
  }

  @Test
  void admittedKueueWorkloadRequestsDriver() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(kueueApplicationMetadata);
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getDriverPreResourcesSpec()).thenReturn(List.of());
    when(mockContext.getDriverPodSpec()).thenReturn(driverPodSpec);
    when(mockContext.getDriverResourcesSpec()).thenReturn(List.of());
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);
    when(recorder.persistStatus(any(), any()))
        .thenAnswer(
            invocation -> {
              application.setStatus(invocation.getArgument(1));
              return true;
            });

    // Not admitted yet: the driver is not requested
    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        appInitStep.reconcile(mockContext, recorder));
    Assertions.assertNull(
        kubernetesClient.pods().inNamespace("default").withName("driver-pod").get());

    admitWorkload();

    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        appInitStep.reconcile(mockContext, recorder));
    Assertions.assertNotNull(
        kubernetesClient.pods().inNamespace("default").withName("driver-pod").get());
    Assertions.assertEquals(
        ApplicationStateSummary.DriverRequested,
        application.getStatus().getCurrentState().getCurrentStateSummary());
    List<EventRecord> events = captureEvents(2);
    Assertions.assertEquals(EventUtils.REASON_KUEUE_ADMISSION_PENDING, events.get(0).reason());
    Assertions.assertEquals(EventType.NORMAL, events.get(1).type());
    Assertions.assertEquals(EventUtils.REASON_KUEUE_ADMITTED, events.get(1).reason());
    Assertions.assertTrue(
        events.get(1).message().contains("sparkapplication-sparkapp1"), events.get(1).message());
  }

  @Test
  void pendingKueueWorkloadPublishesEventOnEveryReconcile() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(kueueApplicationMetadata);
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    // The event sink aggregates the repeats into one Event, while republishing restores an Event
    // the API server has already dropped, which the queued first attempt has no status to replace.
    for (int i = 0; i < 3; i++) {
      Assertions.assertEquals(
          ReconcileProgress.completeAndDefaultRequeue(),
          appInitStep.reconcile(mockContext, recorder));
    }

    for (EventRecord event : captureEvents(3)) {
      Assertions.assertEquals(EventUtils.REASON_KUEUE_ADMISSION_PENDING, event.reason());
    }
  }

  @Test
  void suspendedAppWithQueueNameDoesNotCreateKueueWorkload() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(kueueApplicationMetadata);
    application.getSpec().setSuspend(true);
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(SUSPEND_HOLD_PROGRESS, progress);
    Assertions.assertNull(getWorkload());
    verifyNoInteractions(recorder);
    // Never queued, so nothing is said about a KueueAdmissionPending event that was never
    // published
    EventRecord event = captureEvents(1).get(0);
    Assertions.assertEquals(EventUtils.REASON_SUSPEND_HELD, event.reason());
    Assertions.assertEquals(
        "The SparkApplication is suspended by spec.suspend, driver would not be requested. "
            + "Set spec.suspend to false to resume it.",
        event.message());
  }

  @Test
  void suspendingQueuedAppReleasesKueueWorkload() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(kueueApplicationMetadata);
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    // Queued: the Workload waits for the admission
    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        appInitStep.reconcile(mockContext, recorder));
    Assertions.assertNotNull(getWorkload());

    // Suspended while queued: the Workload is deleted so that it does not hold the quota
    application.getSpec().setSuspend(true);
    Assertions.assertEquals(
        SUSPEND_HOLD_PROGRESS,
        appInitStep.reconcile(mockContext, recorder));
    Assertions.assertNull(getWorkload());
    verify(mockContext, never()).getDriverPodSpec();
    verifyNoInteractions(recorder);
    List<EventRecord> events = captureEvents(2);
    Assertions.assertEquals(EventUtils.REASON_KUEUE_ADMISSION_PENDING, events.get(0).reason());
    Assertions.assertEquals(EventUtils.REASON_SUSPEND_HELD, events.get(1).reason());
    // The pending event above outlives the Workload, so the suspend event supersedes it
    Assertions.assertTrue(
        events
            .get(1)
            .message()
            .endsWith(
                "It holds no Kueue Workload while suspended, so an earlier "
                    + EventUtils.REASON_KUEUE_ADMISSION_PENDING
                    + " event no longer applies."),
        events.get(1).message());
  }

  @Test
  void suspendingQueuedAppReleasesKueueWorkloadEvenIfQueueLabelIsRemoved() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(kueueApplicationMetadata);
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    // Queued: the Workload waits for the admission
    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        appInitStep.reconcile(mockContext, recorder));
    Assertions.assertNotNull(getWorkload());

    // Suspended and removed from the queue in one update: the Workload is deleted all the same,
    // since Kueue would admit it later and hold the quota for an app which never uses it
    application.getSpec().setSuspend(true);
    application.getMetadata().setLabels(Map.of());
    Assertions.assertEquals(
        SUSPEND_HOLD_PROGRESS,
        appInitStep.reconcile(mockContext, recorder));
    Assertions.assertNull(getWorkload());
    List<EventRecord> events = captureEvents(2);
    Assertions.assertEquals(EventUtils.REASON_SUSPEND_HELD, events.get(1).reason());
    Assertions.assertTrue(
        events
            .get(1)
            .message()
            .endsWith(
                "It holds no Kueue Workload while suspended, so an earlier "
                    + EventUtils.REASON_KUEUE_ADMISSION_PENDING
                    + " event no longer applies."),
        events.get(1).message());
  }

  @Test
  void queueLabelRemovedWhileQueuedReleasesPendingKueueWorkload() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(kueueApplicationMetadata);
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getDriverPreResourcesSpec()).thenReturn(List.of());
    when(mockContext.getDriverPodSpec()).thenReturn(driverPodSpec);
    when(mockContext.getDriverResourcesSpec()).thenReturn(List.of());
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);
    when(recorder.persistStatus(any(), any()))
        .thenAnswer(
            invocation -> {
              application.setStatus(invocation.getArgument(1));
              return true;
            });

    // Queued: the Workload waits for the admission
    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        appInitStep.reconcile(mockContext, recorder));
    Assertions.assertNotNull(getWorkload());

    // Removed from the queue while waiting: the driver is requested without Kueue, and the pending
    // Workload is released, which Kueue would otherwise admit later into quota which nothing uses
    application.getMetadata().setLabels(Map.of());
    when(mockContext.getCachedKueueWorkload()).thenReturn(Optional.of(getWorkload()));
    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        appInitStep.reconcile(mockContext, recorder));
    Assertions.assertNull(getWorkload());
    Assertions.assertNotNull(
        kubernetesClient.pods().inNamespace("default").withName("driver-pod").get());
  }

  @Test
  void queueLabelIsIgnoredWhenKueueIsDisabled() {
    TestUtils.setConfigKey(SparkOperatorConf.KUEUE_ENABLED, false);
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(kueueApplicationMetadata);
    application.getSpec().setSuspend(true);
    // A Workload left behind by the operator before the Kueue integration was disabled
    kubernetesClient.resource(KueueWorkloadFactory.buildWorkload(application)).create();
    KubernetesClient client = spy(kubernetesClient);
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getDriverPreResourcesSpec()).thenReturn(List.of());
    when(mockContext.getDriverPodSpec()).thenReturn(driverPodSpec);
    when(mockContext.getDriverResourcesSpec()).thenReturn(List.of());
    when(mockContext.getClient()).thenReturn(client);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);
    when(recorder.persistStatus(any(), any()))
        .thenAnswer(
            invocation -> {
              application.setStatus(invocation.getArgument(1));
              return true;
            });

    // Suspended: the Workload is not released, and the event does not mention Kueue
    Assertions.assertEquals(SUSPEND_HOLD_PROGRESS, appInitStep.reconcile(mockContext, recorder));
    Assertions.assertEquals(
        "The SparkApplication is suspended by spec.suspend, driver would not be requested. "
            + "Set spec.suspend to false to resume it.",
        captureEvents(1).get(0).message());

    // Resumed: the driver is requested right away without the Kueue admission, and the author of
    // the label is told that it is ignored
    application.getSpec().setSuspend(false);
    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        appInitStep.reconcile(mockContext, recorder));
    Assertions.assertNotNull(
        kubernetesClient.pods().inNamespace("default").withName("driver-pod").get());
    Assertions.assertEquals(
        ApplicationStateSummary.DriverRequested,
        application.getStatus().getCurrentState().getCurrentStateSummary());
    verify(client, never()).resources(Workload.class);
    verify(client, never()).resource(any(Workload.class));
    Assertions.assertNotNull(getWorkload());
    EventRecord ignored = captureEvents(2).get(1);
    Assertions.assertEquals(EventType.WARNING, ignored.type());
    Assertions.assertEquals(EventUtils.REASON_KUEUE_DISABLED, ignored.reason());
    Assertions.assertEquals(
        "The kueue.x-k8s.io/queue-name label is ignored because the Kueue integration is "
            + "disabled, so the SparkApplication is not queued. Set "
            + "spark.kubernetes.operator.kueue.enabled to true to enable it.",
        ignored.message());
  }

  @Test
  void staleKueueWorkloadIsDeletedBeforeRequestingAdmission() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(kueueApplicationMetadata);
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);
    // A Workload of a deleted application that had the same name is not garbage collected yet
    Workload stale = KueueWorkloadFactory.buildWorkload(application);
    stale.getMetadata().getOwnerReferences().get(0).setUid("stale-uid");
    kubernetesClient.resource(stale).create();

    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(
        ReconcileProgress.completeAndRequeueAfter(
            KueueWorkloadUtils.STALE_WORKLOAD_REQUEUE_INTERVAL),
        progress);
    Assertions.assertNull(getWorkload());
    verify(mockContext, never()).getDriverPodSpec();
    verifyNoInteractions(recorder);
    // The operator replaces the stale Workload by itself, which needs no attention of users
    verifyNoInteractions(eventRecorder);
  }

  @Test
  void unsupportedKueueSpecFailsScheduling() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(kueueApplicationMetadata);
    application.getSpec().getSparkConf().put("spark.dynamicAllocation.enabled", "true");
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getClient()).thenReturn(kubernetesClient);

    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndImmediateRequeue(), progress);
    Assertions.assertNull(getWorkload());
    ArgumentCaptor<ApplicationStatus> captor = ArgumentCaptor.forClass(ApplicationStatus.class);
    verify(recorder).persistStatus(any(), captor.capture());
    Assertions.assertEquals(
        ApplicationStateSummary.SchedulingFailure,
        captor.getValue().getCurrentState().getCurrentStateSummary());
    Assertions.assertTrue(
        captor.getValue().getCurrentState().getMessage().contains("dynamic allocation"),
        captor.getValue().getCurrentState().getMessage());
  }

  @Test
  void kueueApiFailureIsRetried() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(kueueApplicationMetadata);
    // e.g. the operator lacks the Kueue RBAC rules or Kueue is briefly unreachable
    KubernetesClient failingClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    // Mockito cannot deep-stub the generic list, so it is stubbed without any priority class
    when(failingClient.scheduling().v1().priorityClasses().list())
        .thenReturn(new PriorityClassList());
    when(failingClient.resource(any(Workload.class)).create())
        .thenThrow(new KubernetesClientException("forbidden", 403, null));
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getClient()).thenReturn(failingClient);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    // A persistent failure is retried with the default interval, so that the event of an
    // application waiting for a user to fix the cause is not rewritten every few seconds.
    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    verify(mockContext, never()).getDriverPodSpec();
    verifyNoInteractions(recorder);
    Assertions.assertEquals(
        ApplicationStateSummary.Submitted,
        application.getStatus().getCurrentState().getCurrentStateSummary());
    EventRecord event = captureEvents(1).get(0);
    Assertions.assertEquals(EventType.WARNING, event.type());
    Assertions.assertEquals(EventUtils.REASON_KUEUE_ADMISSION_REQUEST_FAILED, event.reason());
    Assertions.assertTrue(event.message().contains("forbidden"), event.message());
  }

  @Test
  void kueueTransientApiFailurePublishesNoEvent() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(kueueApplicationMetadata);
    // An unavailable API server must not be loaded with event writes on top of the retries
    KubernetesClient failingClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    // Mockito cannot deep-stub the generic list, so it is stubbed without any priority class
    when(failingClient.scheduling().v1().priorityClasses().list())
        .thenReturn(new PriorityClassList());
    when(failingClient.resource(any(Workload.class)).create())
        .thenThrow(new KubernetesClientException("unavailable", 503, null));
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getClient()).thenReturn(failingClient);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(
        ReconcileProgress.completeAndRequeueAfter(
            KueueWorkloadUtils.STALE_WORKLOAD_REQUEUE_INTERVAL),
        progress);
    verifyNoInteractions(eventRecorder);
  }

  @Test
  void kueueUnansweredRequestPublishesNoEvent() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(kueueApplicationMetadata);
    // A request that never reached the API server carries no response code, so only its cause
    // marks it as one to wait out rather than report.
    KubernetesClient failingClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    when(failingClient.scheduling().v1().priorityClasses().list())
        .thenReturn(new PriorityClassList());
    when(failingClient.resource(any(Workload.class)).create())
        .thenThrow(new KubernetesClientException("closed", new SocketException("reset")));
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getClient()).thenReturn(failingClient);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(
        ReconcileProgress.completeAndRequeueAfter(
            KueueWorkloadUtils.STALE_WORKLOAD_REQUEUE_INTERVAL),
        progress);
    verifyNoInteractions(eventRecorder);
  }

  @Test
  void kueueClientSideRejectionPublishesAnEvent() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(kueueApplicationMetadata);
    // A rejection raised before the request was sent shares the absent response code, but nothing
    // clears it on its own, so it keeps its event and the default interval.
    KubernetesClient failingClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    when(failingClient.scheduling().v1().priorityClasses().list())
        .thenReturn(new PriorityClassList());
    when(failingClient.resource(any(Workload.class)).create())
        .thenThrow(new KubernetesClientException("resourceVersion cannot be null"));
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getClient()).thenReturn(failingClient);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    verify(eventRecorder).record(any(EventRecord.class));
  }

  @Test
  void failedDriverLookupBeforeKueueAdmissionIsRetried() {
    // A failed verification is not an answer either before the admission: requesting quota for a
    // driver which is already running would hold a live application
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(kueueApplicationMetadata);
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getCurrentAttemptDriverPod())
        .thenThrow(new KubernetesClientException("unavailable", 503, null));

    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    // The application is retried rather than failed with the terminal SchedulingFailure
    Assertions.assertEquals(
        ReconcileProgress.completeAndRequeueAfter(
            KueueWorkloadUtils.STALE_WORKLOAD_REQUEUE_INTERVAL),
        progress);
    Assertions.assertNull(getWorkload());
    verify(mockContext, never()).getDriverPreResourcesSpec();
    verifyNoInteractions(recorder);
    // An unavailable API server must not be loaded with event writes on top of the retries
    verifyNoInteractions(eventRecorder);
    Assertions.assertEquals(
        ApplicationStateSummary.Submitted,
        application.getStatus().getCurrentState().getCurrentStateSummary());
  }

  @Test
  void refusedDriverLookupBeforeKueueAdmissionPublishesEvent() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(kueueApplicationMetadata);
    // e.g. the operator lacks the RBAC rules for reading pods, which a user has to fix
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);
    when(mockContext.getCurrentAttemptDriverPod())
        .thenThrow(new KubernetesClientException("forbidden", 403, null));

    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    // A persistent failure keeps the default interval, so that its event is not rewritten every
    // few seconds until a user fixes the cause
    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    Assertions.assertNull(getWorkload());
    verifyNoInteractions(recorder);
    EventRecord event = captureEvents(1).get(0);
    Assertions.assertEquals(EventType.WARNING, event.type());
    Assertions.assertEquals(EventUtils.REASON_KUEUE_ADMISSION_REQUEST_FAILED, event.reason());
    Assertions.assertTrue(event.message().contains("forbidden"), event.message());
  }

  @Test
  void driverRequestedBeforeBypassesKueueAdmission() {
    // The driver was created, but the status update to DriverRequested did not land. The Workload
    // is gone meanwhile (e.g. evicted and deleted), which must not hold the live driver.
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(kueueApplicationMetadata);
    kubernetesClient.resource(driverPodSpec).create();
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getCurrentAttemptDriverPod()).thenReturn(Optional.of(driverPodSpec));
    when(mockContext.getDriverPreResourcesSpec()).thenReturn(List.of());
    when(mockContext.getDriverPodSpec()).thenReturn(driverPodSpec);
    when(mockContext.getDriverResourcesSpec()).thenReturn(List.of());
    when(recorder.persistStatus(any(), any()))
        .thenAnswer(
            invocation -> {
              application.setStatus(invocation.getArgument(1));
              return true;
            });

    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    Assertions.assertNull(getWorkload());
    verify(mockContext, never()).setKueuePodSetFlavors(any());
    Assertions.assertEquals(
        ApplicationStateSummary.DriverRequested,
        application.getStatus().getCurrentState().getCurrentStateSummary());
  }

  private Workload getWorkload() {
    return kubernetesClient
        .resources(Workload.class)
        .inNamespace("default")
        .withName("sparkapplication-sparkapp1")
        .get();
  }

  @Test
  void admittedKueueFlavorsAreSetBeforeDriverSpecIsBuilt() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(kueueApplicationMetadata);
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);
    when(mockContext.getDriverPreResourcesSpec()).thenReturn(List.of());
    when(mockContext.getDriverPodSpec()).thenReturn(driverPodSpec);
    when(mockContext.getDriverResourcesSpec()).thenReturn(List.of());
    when(recorder.persistStatus(any(), any())).thenReturn(true);
    Toleration spot = new Toleration("NoSchedule", "spot", "Exists", null, null);
    createFlavor("spot-flavor", Map.of("pool", "spot"), List.of(spot));

    appInitStep.reconcile(mockContext, recorder);
    admitWorkload(
        Map.of(
            "driver", Map.of("cpu", "spot-flavor"),
            "executor", Map.of("cpu", "spot-flavor", "memory", "spot-flavor")));
    appInitStep.reconcile(mockContext, recorder);

    KueuePodSetFlavor flavor = new KueuePodSetFlavor(Map.of("pool", "spot"), List.of(spot));
    InOrder inOrder = inOrder(mockContext);
    inOrder.verify(mockContext).setKueuePodSetFlavors(Map.of("driver", flavor, "executor", flavor));
    inOrder.verify(mockContext).getDriverPreResourcesSpec();
    Assertions.assertNotNull(
        kubernetesClient.pods().inNamespace("default").withName("driver-pod").get());
  }

  @Test
  void kueueFlavorConflictFailsSchedulingAndReleasesWorkload() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(kueueApplicationMetadata);
    application.getSpec().getSparkConf().put("spark.kubernetes.node.selector.pool", "on-demand");
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);
    createFlavor("spot-flavor", Map.of("pool", "spot"), List.of());
    when(recorder.persistStatus(any(), any())).thenReturn(true);

    appInitStep.reconcile(mockContext, recorder);
    admitWorkload(Map.of("driver", Map.of("cpu", "spot-flavor")));
    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    // Like Kueue, the conflict is permanent, so the quota is released
    Assertions.assertEquals(ReconcileProgress.completeAndImmediateRequeue(), progress);
    Assertions.assertNull(getWorkload());
    verify(mockContext, never()).setKueuePodSetFlavors(any());
    ArgumentCaptor<ApplicationStatus> captor = ArgumentCaptor.forClass(ApplicationStatus.class);
    verify(recorder).persistStatus(any(), captor.capture());
    Assertions.assertEquals(
        ApplicationStateSummary.SchedulingFailure,
        captor.getValue().getCurrentState().getCurrentStateSummary());
    Assertions.assertTrue(
        captor.getValue().getCurrentState().getMessage().contains("pool"),
        captor.getValue().getCurrentState().getMessage());
  }

  @Test
  void kueueFlavorReadFailureIsRetried() {
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(kueueApplicationMetadata);
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    appInitStep.reconcile(mockContext, recorder);
    // The flavor of the admitted Workload was deleted or renamed meanwhile
    admitWorkload(Map.of("driver", Map.of("cpu", "missing-flavor")));
    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    // Like a persistent admission failure, it is retried with the default interval
    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    Assertions.assertNotNull(getWorkload());
    verify(mockContext, never()).setKueuePodSetFlavors(any());
    verifyNoInteractions(recorder);
    EventRecord event = captureEvents(2).get(1);
    Assertions.assertEquals(EventType.WARNING, event.type());
    Assertions.assertEquals(EventUtils.REASON_KUEUE_RESOURCE_FLAVOR_READ_FAILED, event.reason());
    // The missing flavor is reported as such, rather than as the missing ClusterRole
    Assertions.assertTrue(event.message().contains("missing-flavor"), event.message());
    Assertions.assertFalse(event.message().contains("ClusterRole"), event.message());
  }

  @Test
  void kueueFlavorsAreAppliedAgainWhenTheDriverExists() {
    // The driver resources are applied again while the status update to DriverRequested is
    // retried, so they must not be rebuilt without the flavors the driver was created with.
    AppInitStep appInitStep = new AppInitStep();
    SparkAppContext mockContext = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    SparkApplication application = new SparkApplication();
    application.setMetadata(kueueApplicationMetadata);
    kubernetesClient.resource(driverPodSpec).create();
    Toleration spot = new Toleration("NoSchedule", "spot", "Exists", null, null);
    createFlavor("spot-flavor", Map.of("pool", "spot"), List.of(spot));
    kubernetesClient.resource(KueueWorkloadFactory.buildWorkload(application)).create();
    admitWorkload(Map.of("driver", Map.of("cpu", "spot-flavor")));
    when(mockContext.getResource()).thenReturn(application);
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);
    when(mockContext.getCurrentAttemptDriverPod()).thenReturn(Optional.of(driverPodSpec));
    when(mockContext.getDriverPreResourcesSpec()).thenReturn(List.of());
    when(mockContext.getDriverPodSpec()).thenReturn(driverPodSpec);
    when(mockContext.getDriverResourcesSpec()).thenReturn(List.of());
    when(recorder.persistStatus(any(), any())).thenReturn(true);

    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    verify(mockContext)
        .setKueuePodSetFlavors(
            Map.of("driver", new KueuePodSetFlavor(Map.of("pool", "spot"), List.of(spot))));
    Assertions.assertNotNull(getWorkload());
  }

  private void createFlavor(
      String name, Map<String, String> nodeLabels, List<Toleration> tolerations) {
    ResourceFlavor flavor = new ResourceFlavor();
    flavor.setMetadata(new ObjectMetaBuilder().withName(name).build());
    flavor.setSpec(
        ResourceFlavorSpec.builder().nodeLabels(nodeLabels).tolerations(tolerations).build());
    kubernetesClient.resource(flavor).create();
  }

  private void admitWorkload(Map<String, Map<String, String>> podSetFlavors) {
    Workload workload = getWorkload();
    workload.setStatus(
        WorkloadStatus.builder()
            .conditions(
                List.of(new ConditionBuilder().withType("Admitted").withStatus("True").build()))
            .admission(
                Admission.builder()
                    .clusterQueue("cluster-queue")
                    .podSetAssignments(
                        podSetFlavors.entrySet().stream()
                            .map(
                                e ->
                                    PodSetAssignment.builder()
                                        .name(e.getKey())
                                        .flavors(e.getValue())
                                        .build())
                            .toList())
                    .build())
            .build());
    kubernetesClient.resource(workload).update();
  }

  private List<EventRecord> captureEvents(int count) {
    ArgumentCaptor<EventRecord> captor = ArgumentCaptor.forClass(EventRecord.class);
    verify(eventRecorder, times(count)).record(captor.capture());
    return captor.getAllValues();
  }

  private void admitWorkload() {
    Workload workload = getWorkload();
    workload.setStatus(
        WorkloadStatus.builder()
            .conditions(
                List.of(
                    new ConditionBuilder().withType("Admitted").withStatus("True").build()))
            .build());
    kubernetesClient.resource(workload).update();
  }
}
