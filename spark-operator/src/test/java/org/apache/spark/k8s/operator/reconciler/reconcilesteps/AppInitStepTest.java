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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkAppSubmissionWorker;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadFactory;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadUtils;
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

@EnableKubernetesMockClient(crud = true)
@SuppressFBWarnings(
    value = {"UWF_UNWRITTEN_FIELD", "NP_UNWRITTEN_FIELD", "UUF_UNUSED_FIELD"},
    justification = "Unwritten fields are covered by Kubernetes mock client")
class AppInitStepTest {
  private KubernetesMockServer mockServer;
  private KubernetesClient kubernetesClient;

  private final ResourceEventRecorder eventRecorder = mock(ResourceEventRecorder.class);

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

    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    verify(mockContext, never()).getDriverPreResourcesSpec();
    verify(mockContext, never()).getDriverPodSpec();
    verify(mockContext, never()).getClient();
    verifyNoInteractions(recorder);
    Assertions.assertEquals(
        ApplicationStateSummary.Submitted,
        application.getStatus().getCurrentState().getCurrentStateSummary());
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

    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    verify(mockContext, never()).getDriverPodSpec();
    verify(mockContext, never()).getClient();
    verifyNoInteractions(recorder);
    Assertions.assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        application.getStatus().getCurrentState().getCurrentStateSummary());
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
    when(recorder.persistStatus(any(), any())).thenAnswer(invocation -> {
      ApplicationStatus newStatus = invocation.getArgument(1);
      application.setStatus(newStatus);
      return true;
    });

    // Suspended: nothing is created and the app stays Submitted
    ReconcileProgress progress1 = appInitStep.reconcile(mockContext, recorder);
    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress1);
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

    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    verify(mockContext, never()).getClient();
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

    ReconcileProgress progress = appInitStep.reconcile(context, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
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

    ReconcileProgress progress = appInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    Assertions.assertNull(getWorkload());
    verifyNoInteractions(recorder);
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
        ReconcileProgress.completeAndDefaultRequeue(),
        appInitStep.reconcile(mockContext, recorder));
    Assertions.assertNull(getWorkload());
    verify(mockContext, never()).getDriverPodSpec();
    verifyNoInteractions(recorder);
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
