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
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ConditionBuilder;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetSpecBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.scheduling.v1.PriorityClassList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.fabric8.kubernetes.client.dsl.ServerSideApplicable;
import io.fabric8.kubernetes.client.dsl.ServiceResource;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.event.EventRecord;
import io.javaoperatorsdk.operator.api.event.EventType;
import io.javaoperatorsdk.operator.api.event.ResourceEventRecorder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.SparkClusterSubmissionWorker;
import org.apache.spark.k8s.operator.config.SparkOperatorConf;
import org.apache.spark.k8s.operator.context.SparkClusterContext;
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
import org.apache.spark.k8s.operator.spec.ClusterSpec;
import org.apache.spark.k8s.operator.spec.ClusterTolerations;
import org.apache.spark.k8s.operator.spec.MasterSpec;
import org.apache.spark.k8s.operator.spec.RuntimeVersions;
import org.apache.spark.k8s.operator.spec.WorkerInstanceConfig;
import org.apache.spark.k8s.operator.status.ClusterState;
import org.apache.spark.k8s.operator.status.ClusterStateSummary;
import org.apache.spark.k8s.operator.status.ClusterStatus;
import org.apache.spark.k8s.operator.utils.EventUtils;
import org.apache.spark.k8s.operator.utils.SparkClusterStatusRecorder;
import org.apache.spark.k8s.operator.utils.TestUtils;

@EnableKubernetesMockClient(crud = true)
@SuppressFBWarnings(
    value = {"UWF_UNWRITTEN_FIELD", "NP_UNWRITTEN_FIELD"},
    justification = "Unwritten fields are covered by Kubernetes mock client")
class ClusterInitStepTest {
  private KubernetesClient kubernetesClient;

  private final ResourceEventRecorder eventRecorder = mock(ResourceEventRecorder.class);

  // The default of spark.kubernetes.operator.reconciler.suspendHoldRequeueIntervalSeconds, which
  // docs/configuration.md and docs/spark_custom_resources.md both state as 30 minutes.
  private static final ReconcileProgress SUSPEND_HOLD_PROGRESS =
      ReconcileProgress.completeAndRequeueAfter(Duration.ofMinutes(30));

  private final StatefulSet masterStatefulSetSpec = statefulSet("cluster1-master");
  private final StatefulSet workerStatefulSetSpec = statefulSet("cluster1-worker");

  @BeforeEach
  void enableKueue() {
    TestUtils.setConfigKey(SparkOperatorConf.KUEUE_ENABLED, true);
  }

  @AfterEach
  void disableKueue() {
    TestUtils.setConfigKey(SparkOperatorConf.KUEUE_ENABLED, false);
  }

  @Test
  void suspendedClusterDoesNotRequestResources() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildCluster();
    cluster.getSpec().setSuspend(true);
    KubernetesClient mockClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    // The Workload of a suspended cluster is released even without its queue label
    stubWorkloadRead(mockClient, null);
    when(mockClient.resource(masterStatefulSetSpec).get()).thenReturn(null);
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(mockClient);
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    ReconcileProgress progress = clusterInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(SUSPEND_HOLD_PROGRESS, progress);
    verify(mockContext, never()).getMasterServiceSpec();
    verify(mockContext, never()).getWorkerStatefulSetSpec();
    verify(mockClient, never()).apps();
    verify(mockClient, never()).services();
    verifyNoInteractions(recorder);
    Assertions.assertEquals(
        ClusterStateSummary.Submitted,
        cluster.getStatus().getCurrentState().getCurrentStateSummary());
    // The Submitted status of a suspended cluster is never persisted, so the event is the only
    // signal users have
    EventRecord event = captureEvents(1).get(0);
    Assertions.assertEquals(EventType.NORMAL, event.type());
    Assertions.assertEquals(EventUtils.REASON_SUSPEND_HELD, event.reason());
    Assertions.assertEquals(
        "The SparkCluster is suspended by spec.suspend, master and workers would not be "
            + "requested. Set spec.suspend to false to resume it.",
        event.message());
  }

  @Test
  void suspendedClusterPublishesEventOnEveryReconcile() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildCluster();
    cluster.getSpec().setSuspend(true);
    KubernetesClient mockClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    // The Workload of a suspended cluster is released even without its queue label
    stubWorkloadRead(mockClient, null);
    when(mockClient.resource(masterStatefulSetSpec).get()).thenReturn(null);
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(mockClient);
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    // The event sink aggregates the repeats into one Event, and each repeat refreshes it, which
    // keeps the hold visible past the event retention: the suspended cluster has no status to
    // fall back on.
    for (int i = 0; i < 3; i++) {
      Assertions.assertEquals(
          SUSPEND_HOLD_PROGRESS,
          clusterInitStep.reconcile(mockContext, recorder));
    }

    for (EventRecord event : captureEvents(3)) {
      Assertions.assertEquals(EventUtils.REASON_SUSPEND_HELD, event.reason());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void unsuspendedClusterRequestsResourcesOnNextReconcile() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildCluster();
    cluster.getSpec().setSuspend(true);
    KubernetesClient mockClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    // The Workload of a suspended cluster is released even without its queue label
    stubWorkloadRead(mockClient, null);
    when(mockClient.resource(masterStatefulSetSpec).get()).thenReturn(null);
    ServerSideApplicable<Service> serviceApplicable = mock(ServerSideApplicable.class);
    ServiceResource<Service> serviceResource = mock(ServiceResource.class);
    when(serviceResource.forceConflicts()).thenReturn(serviceApplicable);
    when(mockClient.services().resource(any(Service.class))).thenReturn(serviceResource);
    ServerSideApplicable<StatefulSet> statefulSetApplicable = mock(ServerSideApplicable.class);
    RollableScalableResource<StatefulSet> statefulSetResource =
        mock(RollableScalableResource.class);
    when(statefulSetResource.forceConflicts()).thenReturn(statefulSetApplicable);
    when(mockClient.apps().statefulSets().resource(any(StatefulSet.class)))
        .thenReturn(statefulSetResource);
    ServerSideApplicable<NetworkPolicy> networkPolicyApplicable = mock(ServerSideApplicable.class);
    Resource<NetworkPolicy> networkPolicyResource = mock(Resource.class);
    when(networkPolicyResource.forceConflicts()).thenReturn(networkPolicyApplicable);
    when(mockClient.network().networkPolicies().resource(any(NetworkPolicy.class)))
        .thenReturn(networkPolicyResource);
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(mockClient);
    when(mockContext.getMasterServiceSpec()).thenReturn(service("cluster1-master-svc"));
    when(mockContext.getWorkerServiceSpec()).thenReturn(service("cluster1-worker-svc"));
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);
    when(mockContext.getWorkerStatefulSetSpec()).thenReturn(workerStatefulSetSpec);
    when(mockContext.getWorkerNetworkPolicySpec()).thenReturn(networkPolicy("cluster1-worker"));
    when(mockContext.getHorizontalPodAutoscalerSpec()).thenReturn(Optional.empty());
    when(mockContext.getPodDisruptionBudgetSpec()).thenReturn(Optional.empty());
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    // Suspended: nothing is requested and the status stays unpersisted
    Assertions.assertEquals(
        SUSPEND_HOLD_PROGRESS,
        clusterInitStep.reconcile(mockContext, recorder));
    verify(mockContext, never()).getMasterServiceSpec();
    verifyNoInteractions(recorder);

    // Unsuspended: the regular init path requests the master and workers
    cluster.getSpec().setSuspend(false);
    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        clusterInitStep.reconcile(mockContext, recorder));
    verify(mockClient.apps().statefulSets()).resource(masterStatefulSetSpec);
    verify(mockClient.apps().statefulSets()).resource(workerStatefulSetSpec);
    ArgumentCaptor<ClusterStatus> statusCaptor = ArgumentCaptor.forClass(ClusterStatus.class);
    verify(recorder).persistStatus(any(), statusCaptor.capture());
    Assertions.assertEquals(
        ClusterStateSummary.RunningHealthy,
        statusCaptor.getValue().getCurrentState().getCurrentStateSummary());
    // Resuming adds no event of its own, the RunningHealthy state transition reports it
    Assertions.assertEquals(EventUtils.REASON_SUSPEND_HELD, captureEvents(1).get(0).reason());
  }

  @ParameterizedTest
  @CsvSource({"429, 1", "500, 1", "503, 0"})
  void retryableFailureOfRequestingResourcesIsRetried(int code, int events) {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildCluster();
    stubFailingServiceApply(
        mockContext, cluster, new KubernetesClientException("failed", code, null));

    // A cluster resumed from Suspended goes through this path again, so a request which may yet
    // succeed, e.g. while an admission webhook is down, must not fail it with SchedulingFailure
    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        clusterInitStep.reconcile(mockContext, recorder));
    verifyNoInteractions(recorder);
    Assertions.assertEquals(
        ClusterStateSummary.Submitted,
        cluster.getStatus().getCurrentState().getCurrentStateSummary());
    // The retry leaves nothing in the status, so it is reported unless it may clear on its own
    for (EventRecord event : captureEvents(events)) {
      Assertions.assertEquals(EventType.WARNING, event.type());
      Assertions.assertEquals(EventUtils.REASON_CLUSTER_REQUEST_FAILED, event.reason());
    }
  }

  @Test
  void rejectedRequestOfResourcesFailsScheduling() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildCluster();
    stubFailingServiceApply(
        mockContext, cluster, new KubernetesClientException("Invalid", 422, null));

    // A rejected request would be rejected again, so it is reported as SchedulingFailure rather
    // than retried with the status staying empty
    Assertions.assertEquals(
        ReconcileProgress.completeAndImmediateRequeue(),
        clusterInitStep.reconcile(mockContext, recorder));
    ArgumentCaptor<ClusterStatus> statusCaptor = ArgumentCaptor.forClass(ClusterStatus.class);
    verify(recorder).persistStatus(any(), statusCaptor.capture());
    Assertions.assertEquals(
        ClusterStateSummary.SchedulingFailure,
        statusCaptor.getValue().getCurrentState().getCurrentStateSummary());
    // The status already says why, so no event is needed
    verifyNoInteractions(eventRecorder);
  }

  @Test
  void resumedClusterSuspendedAgainGoesBackToSuspended() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    when(recorder.appendNewStateAndPersist(any(), any())).thenReturn(true);
    SparkCluster cluster = buildCluster();
    // Resumed from Suspended: the persisted Submitted state follows the states of the earlier run
    cluster.setStatus(
        cluster
            .getStatus()
            .appendNewState(new ClusterState(ClusterStateSummary.Suspended, ""))
            .appendNewState(
                new ClusterState(ClusterStateSummary.Submitted, Constants.CLUSTER_RESUMED_MESSAGE),
                true));
    cluster.getSpec().setSuspend(true);
    KubernetesClient mockClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    when(mockClient.resource(masterStatefulSetSpec).get()).thenReturn(null);
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(mockClient);
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);

    // Its status would otherwise keep saying that it is resumed, while ClusterSuspendStep releases
    // its Workload only once its pods are gone
    Assertions.assertEquals(
        ReconcileProgress.completeAndImmediateRequeue(),
        clusterInitStep.reconcile(mockContext, recorder));
    ArgumentCaptor<ClusterState> stateCaptor = ArgumentCaptor.forClass(ClusterState.class);
    verify(recorder).appendNewStateAndPersist(any(), stateCaptor.capture());
    Assertions.assertEquals(
        ClusterStateSummary.Suspended, stateCaptor.getValue().getCurrentStateSummary());
    Assertions.assertEquals(
        Constants.CLUSTER_SUSPENDED_MESSAGE, stateCaptor.getValue().getMessage());
    verify(mockContext, never()).getEventRecorder();
  }

  @SuppressWarnings("unchecked")
  private void stubFailingServiceApply(
      SparkClusterContext mockContext, SparkCluster cluster, KubernetesClientException failure) {
    KubernetesClient mockClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    ServerSideApplicable<Service> serviceApplicable = mock(ServerSideApplicable.class);
    ServiceResource<Service> serviceResource = mock(ServiceResource.class);
    when(serviceResource.forceConflicts()).thenReturn(serviceApplicable);
    when(serviceApplicable.serverSideApply()).thenThrow(failure);
    when(mockClient.services().resource(any(Service.class))).thenReturn(serviceResource);
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(mockClient);
    when(mockContext.getMasterServiceSpec()).thenReturn(service("cluster1-master-svc"));
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);
  }

  @Test
  @SuppressWarnings("unchecked")
  void suspendAfterMasterRequestedCompletesInitialization() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildCluster();
    cluster.getSpec().setSuspend(true);
    // The master was requested by a previous reconcile whose status update did not land
    KubernetesClient mockClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    when(mockClient.resource(masterStatefulSetSpec).get()).thenReturn(masterStatefulSetSpec);
    ServerSideApplicable<Service> serviceApplicable = mock(ServerSideApplicable.class);
    ServiceResource<Service> serviceResource = mock(ServiceResource.class);
    when(serviceResource.forceConflicts()).thenReturn(serviceApplicable);
    when(mockClient.services().resource(any(Service.class))).thenReturn(serviceResource);
    ServerSideApplicable<StatefulSet> statefulSetApplicable = mock(ServerSideApplicable.class);
    RollableScalableResource<StatefulSet> statefulSetResource =
        mock(RollableScalableResource.class);
    when(statefulSetResource.forceConflicts()).thenReturn(statefulSetApplicable);
    when(mockClient.apps().statefulSets().resource(any(StatefulSet.class)))
        .thenReturn(statefulSetResource);
    ServerSideApplicable<NetworkPolicy> networkPolicyApplicable = mock(ServerSideApplicable.class);
    Resource<NetworkPolicy> networkPolicyResource = mock(Resource.class);
    when(networkPolicyResource.forceConflicts()).thenReturn(networkPolicyApplicable);
    when(mockClient.network().networkPolicies().resource(any(NetworkPolicy.class)))
        .thenReturn(networkPolicyResource);
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(mockClient);
    when(mockContext.getMasterServiceSpec()).thenReturn(service("cluster1-master-svc"));
    when(mockContext.getWorkerServiceSpec()).thenReturn(service("cluster1-worker-svc"));
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);
    when(mockContext.getWorkerStatefulSetSpec()).thenReturn(workerStatefulSetSpec);
    when(mockContext.getWorkerNetworkPolicySpec()).thenReturn(networkPolicy("cluster1-worker"));
    when(mockContext.getHorizontalPodAutoscalerSpec()).thenReturn(Optional.empty());
    when(mockContext.getPodDisruptionBudgetSpec()).thenReturn(Optional.empty());

    ReconcileProgress progress = clusterInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    ArgumentCaptor<ClusterStatus> statusCaptor = ArgumentCaptor.forClass(ClusterStatus.class);
    verify(recorder).persistStatus(any(), statusCaptor.capture());
    Assertions.assertEquals(
        ClusterStateSummary.RunningHealthy,
        statusCaptor.getValue().getCurrentState().getCurrentStateSummary());
    verify(mockClient.apps().statefulSets()).resource(masterStatefulSetSpec);
    verify(mockClient.apps().statefulSets()).resource(workerStatefulSetSpec);
    verify(statefulSetApplicable, times(2)).serverSideApply();
  }

  @Test
  void nonInitializingClusterProceeds() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildCluster();
    cluster.getSpec().setSuspend(true);
    cluster.setStatus(
        cluster
            .getStatus()
            .appendNewState(new ClusterState(ClusterStateSummary.RunningHealthy, "running")));
    when(mockContext.getResource()).thenReturn(cluster);

    ReconcileProgress progress = clusterInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.proceed(), progress);
    verifyNoInteractions(recorder);
  }

  @Test
  void kueueWorkloadIsCreatedAndMasterIsHeldUntilAdmitted() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildKueueCluster();
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    ReconcileProgress progress = clusterInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    Workload workload = getWorkload();
    Assertions.assertNotNull(workload);
    Assertions.assertEquals("cluster-queue", workload.getSpec().getQueueName());
    verify(mockContext, never()).getMasterServiceSpec();
    verify(mockContext, never()).getWorkerStatefulSetSpec();
    verifyNoInteractions(recorder);
    Assertions.assertEquals(
        ClusterStateSummary.Submitted,
        cluster.getStatus().getCurrentState().getCurrentStateSummary());
    EventRecord event = captureEvents(1).get(0);
    Assertions.assertEquals(EventType.NORMAL, event.type());
    Assertions.assertEquals(EventUtils.REASON_KUEUE_ADMISSION_PENDING, event.reason());
    Assertions.assertTrue(
        event.message().contains("sparkcluster-cluster1")
            && event.message().contains("cluster-queue"),
        event.message());
  }

  @Test
  void pendingKueueWorkloadPublishesEventOnEveryReconcile() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildKueueCluster();
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    // The event sink aggregates the repeats into one Event, while republishing restores an Event
    // the API server has already dropped, which the queued first attempt has no status to replace.
    for (int i = 0; i < 3; i++) {
      Assertions.assertEquals(
          ReconcileProgress.completeAndDefaultRequeue(),
          clusterInitStep.reconcile(mockContext, recorder));
    }

    for (EventRecord event : captureEvents(3)) {
      Assertions.assertEquals(EventUtils.REASON_KUEUE_ADMISSION_PENDING, event.reason());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void admittedKueueWorkloadRequestsMasterAndWorker() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildKueueCluster();
    KubernetesClient mockClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    // Mockito cannot deep-stub the generic list, so it is stubbed without any priority class
    when(mockClient.scheduling().v1().priorityClasses().list())
        .thenReturn(new PriorityClassList());
    when(mockClient.resource(masterStatefulSetSpec).get()).thenReturn(null);
    when(mockClient.resource(any(Workload.class)).get()).thenReturn(admittedWorkload(cluster));
    ServerSideApplicable<Service> serviceApplicable = mock(ServerSideApplicable.class);
    ServiceResource<Service> serviceResource = mock(ServiceResource.class);
    when(serviceResource.forceConflicts()).thenReturn(serviceApplicable);
    when(mockClient.services().resource(any(Service.class))).thenReturn(serviceResource);
    ServerSideApplicable<StatefulSet> statefulSetApplicable = mock(ServerSideApplicable.class);
    RollableScalableResource<StatefulSet> statefulSetResource =
        mock(RollableScalableResource.class);
    when(statefulSetResource.forceConflicts()).thenReturn(statefulSetApplicable);
    when(mockClient.apps().statefulSets().resource(any(StatefulSet.class)))
        .thenReturn(statefulSetResource);
    ServerSideApplicable<NetworkPolicy> networkPolicyApplicable = mock(ServerSideApplicable.class);
    Resource<NetworkPolicy> networkPolicyResource = mock(Resource.class);
    when(networkPolicyResource.forceConflicts()).thenReturn(networkPolicyApplicable);
    when(mockClient.network().networkPolicies().resource(any(NetworkPolicy.class)))
        .thenReturn(networkPolicyResource);
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(mockClient);
    when(mockContext.getMasterServiceSpec()).thenReturn(service("cluster1-master-svc"));
    when(mockContext.getWorkerServiceSpec()).thenReturn(service("cluster1-worker-svc"));
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);
    when(mockContext.getWorkerStatefulSetSpec()).thenReturn(workerStatefulSetSpec);
    when(mockContext.getWorkerNetworkPolicySpec()).thenReturn(networkPolicy("cluster1-worker"));
    when(mockContext.getHorizontalPodAutoscalerSpec()).thenReturn(Optional.empty());
    when(mockContext.getPodDisruptionBudgetSpec()).thenReturn(Optional.empty());
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    ReconcileProgress progress = clusterInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    verify(mockClient).resource(any(Workload.class));
    verify(statefulSetApplicable, times(2)).serverSideApply();
    ArgumentCaptor<ClusterStatus> captor = ArgumentCaptor.forClass(ClusterStatus.class);
    verify(recorder).persistStatus(any(), captor.capture());
    Assertions.assertEquals(
        ClusterStateSummary.RunningHealthy,
        captor.getValue().getCurrentState().getCurrentStateSummary());
    EventRecord event = captureEvents(1).get(0);
    Assertions.assertEquals(EventType.NORMAL, event.type());
    Assertions.assertEquals(EventUtils.REASON_KUEUE_ADMITTED, event.reason());
    Assertions.assertTrue(event.message().contains("sparkcluster-cluster1"), event.message());
  }

  @Test
  void staleKueueWorkloadIsDeletedBeforeRequestingAdmission() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildKueueCluster();
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);
    Workload stale = KueueWorkloadFactory.buildWorkload(cluster);
    stale.getMetadata().getOwnerReferences().get(0).setUid("stale-uid");
    kubernetesClient.resource(stale).create();

    ReconcileProgress progress = clusterInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(
        ReconcileProgress.completeAndRequeueAfter(
            KueueWorkloadUtils.STALE_WORKLOAD_REQUEUE_INTERVAL),
        progress);
    Assertions.assertNull(getWorkload());
    verify(mockContext, never()).getMasterServiceSpec();
    verifyNoInteractions(recorder);
    // The operator replaces the stale Workload by itself, which needs no attention of users
    verifyNoInteractions(eventRecorder);
  }

  @Test
  @SuppressWarnings("unchecked")
  void masterRequestedBeforeBypassesKueueAdmission() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildKueueCluster();
    // The master was requested by a previous reconcile whose status update did not land
    KubernetesClient mockClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    when(mockClient.resource(masterStatefulSetSpec).get()).thenReturn(masterStatefulSetSpec);
    // The Workload is gone meanwhile, e.g. evicted and deleted, so no flavor is applied
    stubWorkloadRead(mockClient, null);
    ServerSideApplicable<Service> serviceApplicable = mock(ServerSideApplicable.class);
    ServiceResource<Service> serviceResource = mock(ServiceResource.class);
    when(serviceResource.forceConflicts()).thenReturn(serviceApplicable);
    when(mockClient.services().resource(any(Service.class))).thenReturn(serviceResource);
    ServerSideApplicable<StatefulSet> statefulSetApplicable = mock(ServerSideApplicable.class);
    RollableScalableResource<StatefulSet> statefulSetResource =
        mock(RollableScalableResource.class);
    when(statefulSetResource.forceConflicts()).thenReturn(statefulSetApplicable);
    when(mockClient.apps().statefulSets().resource(any(StatefulSet.class)))
        .thenReturn(statefulSetResource);
    ServerSideApplicable<NetworkPolicy> networkPolicyApplicable = mock(ServerSideApplicable.class);
    Resource<NetworkPolicy> networkPolicyResource = mock(Resource.class);
    when(networkPolicyResource.forceConflicts()).thenReturn(networkPolicyApplicable);
    when(mockClient.network().networkPolicies().resource(any(NetworkPolicy.class)))
        .thenReturn(networkPolicyResource);
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(mockClient);
    when(mockContext.getMasterServiceSpec()).thenReturn(service("cluster1-master-svc"));
    when(mockContext.getWorkerServiceSpec()).thenReturn(service("cluster1-worker-svc"));
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);
    when(mockContext.getWorkerStatefulSetSpec()).thenReturn(workerStatefulSetSpec);
    when(mockContext.getWorkerNetworkPolicySpec()).thenReturn(networkPolicy("cluster1-worker"));
    when(mockContext.getHorizontalPodAutoscalerSpec()).thenReturn(Optional.empty());
    when(mockContext.getPodDisruptionBudgetSpec()).thenReturn(Optional.empty());

    ReconcileProgress progress = clusterInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    verify(mockClient, never()).resource(any(Workload.class));
    verify(mockContext, never()).setKueuePodSetFlavors(any());
    ArgumentCaptor<ClusterStatus> captor = ArgumentCaptor.forClass(ClusterStatus.class);
    verify(recorder).persistStatus(any(), captor.capture());
    Assertions.assertEquals(
        ClusterStateSummary.RunningHealthy,
        captor.getValue().getCurrentState().getCurrentStateSummary());
  }

  @Test
  void failedReleaseOfPendingKueueWorkloadHoldsDequeuedCluster() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    // Queued before, and removed from the queue while its Workload waited for the admission
    SparkCluster cluster = buildCluster();
    KubernetesClient mockClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    Resource<Workload> workload = stubWorkloadRead(mockClient, null);
    when(workload.delete()).thenThrow(new KubernetesClientException("forbidden", 403, null));
    when(mockClient.resource(masterStatefulSetSpec).get()).thenReturn(null);
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(mockClient);
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);
    when(mockContext.getCachedKueueWorkload())
        .thenReturn(Optional.of(KueueWorkloadFactory.buildWorkload(buildKueueCluster())));

    // The master is not requested while the pending Workload may still be admitted for nothing
    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        clusterInitStep.reconcile(mockContext, recorder));
    verify(workload).delete();
    verify(mockContext, never()).getMasterServiceSpec();
    verifyNoInteractions(recorder);
    EventRecord event = captureEvents(1).get(0);
    Assertions.assertEquals(EventUtils.REASON_KUEUE_ADMISSION_REQUEST_FAILED, event.reason());
  }

  @Test
  @SuppressWarnings("unchecked")
  void queueLabelIsIgnoredWhenKueueIsDisabled() {
    TestUtils.setConfigKey(SparkOperatorConf.KUEUE_ENABLED, false);
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildKueueCluster();
    cluster.getSpec().setSuspend(true);
    KubernetesClient mockClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    when(mockClient.resource(masterStatefulSetSpec).get()).thenReturn(null);
    ServerSideApplicable<Service> serviceApplicable = mock(ServerSideApplicable.class);
    ServiceResource<Service> serviceResource = mock(ServiceResource.class);
    when(serviceResource.forceConflicts()).thenReturn(serviceApplicable);
    when(mockClient.services().resource(any(Service.class))).thenReturn(serviceResource);
    ServerSideApplicable<StatefulSet> statefulSetApplicable = mock(ServerSideApplicable.class);
    RollableScalableResource<StatefulSet> statefulSetResource =
        mock(RollableScalableResource.class);
    when(statefulSetResource.forceConflicts()).thenReturn(statefulSetApplicable);
    when(mockClient.apps().statefulSets().resource(any(StatefulSet.class)))
        .thenReturn(statefulSetResource);
    ServerSideApplicable<NetworkPolicy> networkPolicyApplicable = mock(ServerSideApplicable.class);
    Resource<NetworkPolicy> networkPolicyResource = mock(Resource.class);
    when(networkPolicyResource.forceConflicts()).thenReturn(networkPolicyApplicable);
    when(mockClient.network().networkPolicies().resource(any(NetworkPolicy.class)))
        .thenReturn(networkPolicyResource);
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(mockClient);
    when(mockContext.getMasterServiceSpec()).thenReturn(service("cluster1-master-svc"));
    when(mockContext.getWorkerServiceSpec()).thenReturn(service("cluster1-worker-svc"));
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);
    when(mockContext.getWorkerStatefulSetSpec()).thenReturn(workerStatefulSetSpec);
    when(mockContext.getWorkerNetworkPolicySpec()).thenReturn(networkPolicy("cluster1-worker"));
    when(mockContext.getHorizontalPodAutoscalerSpec()).thenReturn(Optional.empty());
    when(mockContext.getPodDisruptionBudgetSpec()).thenReturn(Optional.empty());
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    // Suspended: no Workload is released, and the event does not mention Kueue
    Assertions.assertEquals(
        SUSPEND_HOLD_PROGRESS, clusterInitStep.reconcile(mockContext, recorder));
    Assertions.assertEquals(
        "The SparkCluster is suspended by spec.suspend, master and workers would not be "
            + "requested. Set spec.suspend to false to resume it.",
        captureEvents(1).get(0).message());

    // Resumed: the master and workers are requested right away without the Kueue admission, and
    // the author of the label is told that it is ignored
    cluster.getSpec().setSuspend(false);
    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        clusterInitStep.reconcile(mockContext, recorder));
    verify(statefulSetApplicable, times(2)).serverSideApply();
    verify(mockClient, never()).resources(Workload.class);
    verify(mockClient, never()).resource(any(Workload.class));
    EventRecord ignored = captureEvents(2).get(1);
    Assertions.assertEquals(EventType.WARNING, ignored.type());
    Assertions.assertEquals(EventUtils.REASON_KUEUE_DISABLED, ignored.reason());
    Assertions.assertEquals(
        "The kueue.x-k8s.io/queue-name label is ignored because the Kueue integration is "
            + "disabled, so the SparkCluster is not queued. Set "
            + "spark.kubernetes.operator.kueue.enabled to true to enable it.",
        ignored.message());
  }

  @Test
  void suspendingQueuedClusterReleasesKueueWorkload() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildKueueCluster();
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    // Queued: the Workload waits for the admission
    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        clusterInitStep.reconcile(mockContext, recorder));
    Assertions.assertNotNull(getWorkload());

    // Suspended while queued: the Workload is deleted so that it does not hold the quota
    cluster.getSpec().setSuspend(true);
    Assertions.assertEquals(
        SUSPEND_HOLD_PROGRESS,
        clusterInitStep.reconcile(mockContext, recorder));
    Assertions.assertNull(getWorkload());
    verify(mockContext, never()).getMasterServiceSpec();
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
  void suspendingQueuedClusterReleasesKueueWorkloadAfterQueueLabelIsRemoved() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildKueueCluster();
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);
    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        clusterInitStep.reconcile(mockContext, recorder));
    Assertions.assertNotNull(getWorkload());

    // Suspended with the queue label removed in the same update: the queued Workload would be
    // admitted into quota which nothing uses, so it is deleted as well
    cluster.getSpec().setSuspend(true);
    cluster.getMetadata().setLabels(Map.of());
    Assertions.assertEquals(
        SUSPEND_HOLD_PROGRESS,
        clusterInitStep.reconcile(mockContext, recorder));
    Assertions.assertNull(getWorkload());
    List<EventRecord> events = captureEvents(2);
    Assertions.assertEquals(EventUtils.REASON_SUSPEND_HELD, events.get(1).reason());
    Assertions.assertTrue(
        events.get(1).message().endsWith(" event no longer applies."), events.get(1).message());
  }

  @Test
  void kueueApiFailureIsRetried() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildKueueCluster();
    // e.g. the operator lacks the Kueue RBAC rules or Kueue is briefly unreachable
    KubernetesClient failingClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    // Mockito cannot deep-stub the generic list, so it is stubbed without any priority class
    when(failingClient.scheduling().v1().priorityClasses().list())
        .thenReturn(new PriorityClassList());
    when(failingClient.resource(masterStatefulSetSpec).get()).thenReturn(null);
    when(failingClient.resource(any(Workload.class)).create())
        .thenThrow(new KubernetesClientException("forbidden", 403, null));
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(failingClient);
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    ReconcileProgress progress = clusterInitStep.reconcile(mockContext, recorder);

    // The cluster is not failed permanently, the admission is requested again. A persistent
    // failure is retried with the default interval, so that the event of a cluster waiting for a
    // user to fix the cause is not rewritten every few seconds.
    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    verify(mockContext, never()).getMasterServiceSpec();
    verifyNoInteractions(recorder);
    Assertions.assertEquals(
        ClusterStateSummary.Submitted,
        cluster.getStatus().getCurrentState().getCurrentStateSummary());
    EventRecord event = captureEvents(1).get(0);
    Assertions.assertEquals(EventType.WARNING, event.type());
    Assertions.assertEquals(EventUtils.REASON_KUEUE_ADMISSION_REQUEST_FAILED, event.reason());
    Assertions.assertTrue(event.message().contains("forbidden"), event.message());
  }

  @Test
  void kueueTransientApiFailurePublishesNoEvent() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildKueueCluster();
    // An unavailable API server must not be loaded with event writes on top of the retries
    KubernetesClient failingClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    when(failingClient.resource(masterStatefulSetSpec).get()).thenReturn(null);
    // Mockito cannot deep-stub the generic list, so it is stubbed without any priority class
    when(failingClient.scheduling().v1().priorityClasses().list())
        .thenReturn(new PriorityClassList());
    when(failingClient.resource(any(Workload.class)).create())
        .thenThrow(new KubernetesClientException("unavailable", 503, null));
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(failingClient);
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    ReconcileProgress progress = clusterInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(
        ReconcileProgress.completeAndRequeueAfter(
            KueueWorkloadUtils.STALE_WORKLOAD_REQUEUE_INTERVAL),
        progress);
    verifyNoInteractions(eventRecorder);
  }

  @Test
  void suspendedClusterWithUnverifiableMasterIsNotHeld() {
    // A failed lookup is not an answer: the master may be running, so the cluster must not be
    // held with an event claiming that none was requested, and its Kueue quota must not be
    // released either
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildKueueCluster();
    cluster.getSpec().setSuspend(true);
    KubernetesClient mockClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    when(mockClient.resource(masterStatefulSetSpec).get())
        .thenThrow(new KubernetesClientException("unavailable", 503, null));
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(mockClient);
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    ReconcileProgress progress = clusterInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    verify(mockClient, never()).resources(Workload.class);
    verify(mockContext, never()).getMasterServiceSpec();
    verifyNoInteractions(recorder);
    verifyNoInteractions(eventRecorder);
    Assertions.assertEquals(
        ClusterStateSummary.Submitted,
        cluster.getStatus().getCurrentState().getCurrentStateSummary());
  }

  @Test
  void failedMasterLookupBeforeKueueAdmissionIsRetried() {
    // A failed lookup is not an answer either before the admission: requesting quota for a master
    // which is already running would hold a live cluster
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildKueueCluster();
    KubernetesClient mockClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    when(mockClient.resource(masterStatefulSetSpec).get())
        .thenThrow(new KubernetesClientException("unavailable", 503, null));
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(mockClient);
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);

    ReconcileProgress progress = clusterInitStep.reconcile(mockContext, recorder);

    // The cluster is retried rather than failed with the terminal SchedulingFailure
    Assertions.assertEquals(
        ReconcileProgress.completeAndRequeueAfter(
            KueueWorkloadUtils.STALE_WORKLOAD_REQUEUE_INTERVAL),
        progress);
    verify(mockClient, never()).resource(any(Workload.class));
    verify(mockContext, never()).getMasterServiceSpec();
    verifyNoInteractions(recorder);
    // An unavailable API server must not be loaded with event writes on top of the retries
    verifyNoInteractions(eventRecorder);
    Assertions.assertEquals(
        ClusterStateSummary.Submitted,
        cluster.getStatus().getCurrentState().getCurrentStateSummary());
  }

  @Test
  void refusedMasterLookupBeforeKueueAdmissionPublishesEvent() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildKueueCluster();
    // e.g. the operator lacks the RBAC rules for reading StatefulSets, which a user has to fix
    KubernetesClient mockClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    when(mockClient.resource(masterStatefulSetSpec).get())
        .thenThrow(new KubernetesClientException("forbidden", 403, null));
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(mockClient);
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    ReconcileProgress progress = clusterInitStep.reconcile(mockContext, recorder);

    // A persistent failure keeps the default interval, so that its event is not rewritten every
    // few seconds until a user fixes the cause
    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    verify(mockClient, never()).resource(any(Workload.class));
    verify(mockContext, never()).getMasterServiceSpec();
    verifyNoInteractions(recorder);
    EventRecord event = captureEvents(1).get(0);
    Assertions.assertEquals(EventType.WARNING, event.type());
    Assertions.assertEquals(EventUtils.REASON_KUEUE_ADMISSION_REQUEST_FAILED, event.reason());
    Assertions.assertTrue(event.message().contains("forbidden"), event.message());
  }

  @Test
  @SuppressWarnings("unchecked")
  void admittedKueueFlavorsReachTheAppliedStatefulSets() {
    // A real context is used, so that the assertion covers the rebuild which the flavors trigger
    // rather than the call which sets them.
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildKueueCluster();
    Toleration spot = new Toleration("NoSchedule", "spot", "Exists", null, null);
    KubernetesClient mockClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    // Mockito cannot deep-stub the generic list, so it is stubbed without any priority class
    when(mockClient.scheduling().v1().priorityClasses().list())
        .thenReturn(new PriorityClassList());
    // The master does not exist yet, while Kueue admitted the Workload with a flavor
    when(mockClient.resource(isA(StatefulSet.class)).get()).thenReturn(null);
    when(mockClient.resource(isA(Workload.class)).get())
        .thenReturn(
            admittedWorkload(
                cluster,
                Map.of(
                    "master", Map.of("cpu", "spot-flavor"),
                    "worker", Map.of("cpu", "spot-flavor"))));
    stubFlavorRead(mockClient, flavor("spot-flavor", Map.of("pool", "spot"), List.of(spot)));
    ServerSideApplicable<Service> serviceApplicable = mock(ServerSideApplicable.class);
    ServiceResource<Service> serviceResource = mock(ServiceResource.class);
    when(serviceResource.forceConflicts()).thenReturn(serviceApplicable);
    when(mockClient.services().resource(any(Service.class))).thenReturn(serviceResource);
    ServerSideApplicable<StatefulSet> statefulSetApplicable = mock(ServerSideApplicable.class);
    RollableScalableResource<StatefulSet> statefulSetResource =
        mock(RollableScalableResource.class);
    when(statefulSetResource.forceConflicts()).thenReturn(statefulSetApplicable);
    when(mockClient.apps().statefulSets().resource(any(StatefulSet.class)))
        .thenReturn(statefulSetResource);
    ServerSideApplicable<NetworkPolicy> networkPolicyApplicable = mock(ServerSideApplicable.class);
    Resource<NetworkPolicy> networkPolicyResource = mock(Resource.class);
    when(networkPolicyResource.forceConflicts()).thenReturn(networkPolicyApplicable);
    when(mockClient.network().networkPolicies().resource(any(NetworkPolicy.class)))
        .thenReturn(networkPolicyResource);
    ServerSideApplicable<PodDisruptionBudget> budgetApplicable = mock(ServerSideApplicable.class);
    Resource<PodDisruptionBudget> budgetResource = mock(Resource.class);
    when(budgetResource.forceConflicts()).thenReturn(budgetApplicable);
    when(mockClient.policy().v1().podDisruptionBudget().resource(any(PodDisruptionBudget.class)))
        .thenReturn(budgetResource);
    Context<?> josdkContext = mock(Context.class);
    when(josdkContext.getClient()).thenReturn(mockClient);
    when(josdkContext.eventRecorder()).thenReturn(eventRecorder);
    SparkClusterContext context =
        new SparkClusterContext(cluster, josdkContext, new SparkClusterSubmissionWorker());

    ReconcileProgress progress = clusterInitStep.reconcile(context, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    ArgumentCaptor<StatefulSet> captor = ArgumentCaptor.forClass(StatefulSet.class);
    verify(mockClient.apps().statefulSets(), times(2)).resource(captor.capture());
    for (StatefulSet applied : captor.getAllValues()) {
      PodSpec podSpec = applied.getSpec().getTemplate().getSpec();
      String name = applied.getMetadata().getName();
      Assertions.assertEquals(Map.of("pool", "spot"), podSpec.getNodeSelector(), name);
      Assertions.assertEquals(List.of(spot), podSpec.getTolerations(), name);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("unchecked")
  void kueueFlavorsAreAppliedAgainWhenTheMasterExists(boolean queueLabelRemoved) {
    // The StatefulSets are applied again while the status update to RunningHealthy is retried, so
    // they must not be rebuilt without the flavors their pods were created with, even if the queue
    // label was removed meanwhile.
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildKueueCluster();
    Workload admitted = admittedWorkload(cluster, Map.of("master", Map.of("cpu", "spot-flavor")));
    if (queueLabelRemoved) {
      cluster.getMetadata().setLabels(Map.of());
      when(mockContext.getCachedKueueWorkload()).thenReturn(Optional.of(admitted));
    }
    Toleration spot = new Toleration("NoSchedule", "spot", "Exists", null, null);
    KubernetesClient mockClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    when(mockClient.resource(masterStatefulSetSpec).get()).thenReturn(masterStatefulSetSpec);
    Resource<Workload> workload = stubWorkloadRead(mockClient, admitted);
    stubFlavorRead(mockClient, flavor("spot-flavor", Map.of("pool", "spot"), List.of(spot)));
    ServerSideApplicable<Service> serviceApplicable = mock(ServerSideApplicable.class);
    ServiceResource<Service> serviceResource = mock(ServiceResource.class);
    when(serviceResource.forceConflicts()).thenReturn(serviceApplicable);
    when(mockClient.services().resource(any(Service.class))).thenReturn(serviceResource);
    ServerSideApplicable<StatefulSet> statefulSetApplicable = mock(ServerSideApplicable.class);
    RollableScalableResource<StatefulSet> statefulSetResource =
        mock(RollableScalableResource.class);
    when(statefulSetResource.forceConflicts()).thenReturn(statefulSetApplicable);
    when(mockClient.apps().statefulSets().resource(any(StatefulSet.class)))
        .thenReturn(statefulSetResource);
    ServerSideApplicable<NetworkPolicy> networkPolicyApplicable = mock(ServerSideApplicable.class);
    Resource<NetworkPolicy> networkPolicyResource = mock(Resource.class);
    when(networkPolicyResource.forceConflicts()).thenReturn(networkPolicyApplicable);
    when(mockClient.network().networkPolicies().resource(any(NetworkPolicy.class)))
        .thenReturn(networkPolicyResource);
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(mockClient);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);
    when(mockContext.getMasterServiceSpec()).thenReturn(service("cluster1-master-svc"));
    when(mockContext.getWorkerServiceSpec()).thenReturn(service("cluster1-worker-svc"));
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);
    when(mockContext.getWorkerStatefulSetSpec()).thenReturn(workerStatefulSetSpec);
    when(mockContext.getWorkerNetworkPolicySpec()).thenReturn(networkPolicy("cluster1-worker"));
    when(mockContext.getHorizontalPodAutoscalerSpec()).thenReturn(Optional.empty());
    when(mockContext.getPodDisruptionBudgetSpec()).thenReturn(Optional.empty());

    ReconcileProgress progress = clusterInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    InOrder inOrder = inOrder(mockContext);
    inOrder
        .verify(mockContext)
        .setKueuePodSetFlavors(
            Map.of("master", new KueuePodSetFlavor(Map.of("pool", "spot"), List.of(spot))));
    inOrder.verify(mockContext).getMasterStatefulSetSpec();
    // The admission is not requested again for a master which exists, nor is the Workload released
    verify(mockClient, never()).resource(any(Workload.class));
    verify(workload, never()).delete();
  }

  @Test
  void kueueFlavorConflictFailsSchedulingAndReleasesWorkload() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildKueueCluster();
    cluster
        .getSpec()
        .setMasterSpec(
            MasterSpec.builder()
                .statefulSetSpec(
                    new StatefulSetSpecBuilder()
                        .withNewTemplate()
                        .withNewSpec()
                        .withNodeSelector(Map.of("pool", "on-demand"))
                        .endSpec()
                        .endTemplate()
                        .build())
                .build());
    kubernetesClient.resource(flavor("spot-flavor", Map.of("pool", "spot"), List.of())).create();
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);

    clusterInitStep.reconcile(mockContext, recorder);
    admitWorkload(Map.of("master", Map.of("cpu", "spot-flavor")));
    ReconcileProgress progress = clusterInitStep.reconcile(mockContext, recorder);

    // Like Kueue, the conflict is permanent, so the quota is released
    Assertions.assertEquals(ReconcileProgress.completeAndImmediateRequeue(), progress);
    Assertions.assertNull(getWorkload());
    verify(mockContext, never()).setKueuePodSetFlavors(any());
    verify(mockContext, never()).getMasterServiceSpec();
    ArgumentCaptor<ClusterStatus> captor = ArgumentCaptor.forClass(ClusterStatus.class);
    verify(recorder).persistStatus(any(), captor.capture());
    Assertions.assertEquals(
        ClusterStateSummary.SchedulingFailure,
        captor.getValue().getCurrentState().getCurrentStateSummary());
    Assertions.assertTrue(
        captor.getValue().getCurrentState().getMessage().contains("pool"),
        captor.getValue().getCurrentState().getMessage());
  }

  @Test
  void kueueFlavorReadFailureIsRetried() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildKueueCluster();
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(kubernetesClient);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);

    clusterInitStep.reconcile(mockContext, recorder);
    // The flavor of the admitted Workload was deleted or renamed meanwhile
    admitWorkload(Map.of("master", Map.of("cpu", "missing-flavor")));
    ReconcileProgress progress = clusterInitStep.reconcile(mockContext, recorder);

    // The cluster is not failed permanently, the flavors are read again
    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    Assertions.assertNotNull(getWorkload());
    verify(mockContext, never()).setKueuePodSetFlavors(any());
    verify(mockContext, never()).getMasterServiceSpec();
    verifyNoInteractions(recorder);
    EventRecord event = captureEvents(2).get(1);
    Assertions.assertEquals(EventType.WARNING, event.type());
    Assertions.assertEquals(EventUtils.REASON_KUEUE_RESOURCE_FLAVOR_READ_FAILED, event.reason());
    // The missing flavor is reported as such, rather than as the missing ClusterRole
    Assertions.assertTrue(event.message().contains("missing-flavor"), event.message());
    Assertions.assertFalse(event.message().contains("ClusterRole"), event.message());
  }

  private void admitWorkload(Map<String, Map<String, String>> podSetFlavors) {
    Workload workload = getWorkload();
    workload.setStatus(admittedStatus(podSetFlavors));
    kubernetesClient.resource(workload).update();
  }

  @SuppressWarnings("unchecked")
  private static void stubFlavorRead(KubernetesClient client, ResourceFlavor flavor) {
    MixedOperation<ResourceFlavor, KubernetesResourceList<ResourceFlavor>, Resource<ResourceFlavor>>
        flavors = mock(MixedOperation.class);
    Resource<ResourceFlavor> resource = mock(Resource.class);
    when(client.resources(ResourceFlavor.class)).thenReturn(flavors);
    when(flavors.withName(flavor.getMetadata().getName())).thenReturn(resource);
    when(resource.get()).thenReturn(flavor);
  }

  @SuppressWarnings("unchecked")
  private static Resource<Workload> stubWorkloadRead(KubernetesClient client, Workload workload) {
    MixedOperation<Workload, KubernetesResourceList<Workload>, Resource<Workload>> workloads =
        mock(MixedOperation.class);
    NonNamespaceOperation<Workload, KubernetesResourceList<Workload>, Resource<Workload>>
        namespaced = mock(NonNamespaceOperation.class);
    Resource<Workload> resource = mock(Resource.class);
    when(client.resources(Workload.class)).thenReturn(workloads);
    when(workloads.inNamespace("default")).thenReturn(namespaced);
    when(namespaced.withName("sparkcluster-cluster1")).thenReturn(resource);
    when(resource.get()).thenReturn(workload);
    return resource;
  }

  private static WorkloadStatus admittedStatus(Map<String, Map<String, String>> podSetFlavors) {
    return WorkloadStatus.builder()
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
        .build();
  }

  private static ResourceFlavor flavor(
      String name, Map<String, String> nodeLabels, List<Toleration> tolerations) {
    ResourceFlavor flavor = new ResourceFlavor();
    flavor.setMetadata(new ObjectMetaBuilder().withName(name).build());
    flavor.setSpec(
        ResourceFlavorSpec.builder().nodeLabels(nodeLabels).tolerations(tolerations).build());
    return flavor;
  }

  private SparkCluster buildKueueCluster() {
    SparkCluster cluster = buildCluster();
    cluster.getMetadata().setUid("cluster-uid");
    cluster.getMetadata().setLabels(Map.of(Constants.LABEL_QUEUE_NAME, "cluster-queue"));
    cluster.setSpec(
        ClusterSpec.builder()
            .runtimeVersions(RuntimeVersions.builder().sparkVersion("4.2.0").build())
            .clusterTolerations(
                ClusterTolerations.builder()
                    .instanceConfig(
                        WorkerInstanceConfig.builder()
                            .initWorkers(1)
                            .minWorkers(1)
                            .maxWorkers(1)
                            .build())
                    .build())
            .build());
    return cluster;
  }

  private Workload getWorkload() {
    return kubernetesClient
        .resources(Workload.class)
        .inNamespace("default")
        .withName("sparkcluster-cluster1")
        .get();
  }

  private List<EventRecord> captureEvents(int count) {
    ArgumentCaptor<EventRecord> captor = ArgumentCaptor.forClass(EventRecord.class);
    verify(eventRecorder, times(count)).record(captor.capture());
    return captor.getAllValues();
  }

  private static Workload admittedWorkload(
      SparkCluster cluster, Map<String, Map<String, String>> podSetFlavors) {
    Workload workload = KueueWorkloadFactory.buildWorkload(cluster);
    workload.setStatus(admittedStatus(podSetFlavors));
    return workload;
  }

  private static Workload admittedWorkload(SparkCluster cluster) {
    Workload workload = KueueWorkloadFactory.buildWorkload(cluster);
    workload.setStatus(
        WorkloadStatus.builder()
            .conditions(
                List.of(new ConditionBuilder().withType("Admitted").withStatus("True").build()))
            .build());
    return workload;
  }

  private SparkCluster buildCluster() {
    SparkCluster cluster = new SparkCluster();
    cluster.setMetadata(
        new ObjectMetaBuilder().withName("cluster1").withNamespace("default").build());
    return cluster;
  }

  private static StatefulSet statefulSet(String name) {
    return new StatefulSetBuilder()
        .withNewMetadata()
        .withName(name)
        .withNamespace("default")
        .endMetadata()
        .withNewSpec()
        .withReplicas(1)
        .endSpec()
        .build();
  }

  private static Service service(String name) {
    return new ServiceBuilder()
        .withNewMetadata()
        .withName(name)
        .withNamespace("default")
        .endMetadata()
        .build();
  }

  private static NetworkPolicy networkPolicy(String name) {
    return new NetworkPolicyBuilder()
        .withNewMetadata()
        .withName(name)
        .withNamespace("default")
        .endMetadata()
        .build();
  }
}
