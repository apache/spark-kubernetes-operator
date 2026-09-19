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
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ConditionBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.fabric8.kubernetes.client.dsl.ServerSideApplicable;
import io.fabric8.kubernetes.client.dsl.ServiceResource;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.context.SparkClusterContext;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadFactory;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadUtils;
import org.apache.spark.k8s.operator.kueue.v1beta2.Workload;
import org.apache.spark.k8s.operator.kueue.v1beta2.WorkloadStatus;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.spec.ClusterSpec;
import org.apache.spark.k8s.operator.spec.ClusterTolerations;
import org.apache.spark.k8s.operator.spec.RuntimeVersions;
import org.apache.spark.k8s.operator.spec.WorkerInstanceConfig;
import org.apache.spark.k8s.operator.status.ClusterState;
import org.apache.spark.k8s.operator.status.ClusterStateSummary;
import org.apache.spark.k8s.operator.status.ClusterStatus;
import org.apache.spark.k8s.operator.utils.SparkClusterStatusRecorder;

@EnableKubernetesMockClient(crud = true)
@SuppressFBWarnings(
    value = {"UWF_UNWRITTEN_FIELD", "NP_UNWRITTEN_FIELD"},
    justification = "Unwritten fields are covered by Kubernetes mock client")
class ClusterInitStepTest {
  private KubernetesClient kubernetesClient;

  private final StatefulSet masterStatefulSetSpec = statefulSet("cluster1-master");
  private final StatefulSet workerStatefulSetSpec = statefulSet("cluster1-worker");

  @Test
  void suspendedClusterDoesNotRequestResources() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildCluster();
    cluster.getSpec().setSuspend(true);
    KubernetesClient mockClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    when(mockClient.resource(masterStatefulSetSpec).get()).thenReturn(null);
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(mockClient);
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);

    ReconcileProgress progress = clusterInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    verify(mockContext, never()).getMasterServiceSpec();
    verify(mockContext, never()).getWorkerStatefulSetSpec();
    verify(mockClient, never()).apps();
    verify(mockClient, never()).services();
    verifyNoInteractions(recorder);
    Assertions.assertEquals(
        ClusterStateSummary.Submitted,
        cluster.getStatus().getCurrentState().getCurrentStateSummary());
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
  void invalidWorkerNetworkPolicyDoesNotCreateStatefulSets() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildCluster();
    KubernetesClient mockClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(mockClient);
    when(mockContext.getMasterServiceSpec()).thenReturn(service("cluster1-master-svc"));
    when(mockContext.getWorkerServiceSpec()).thenReturn(service("cluster1-worker-svc"));
    when(mockContext.getWorkerNetworkPolicySpec()).thenReturn(networkPolicy("cluster1-worker"));
    @SuppressWarnings("unchecked")
    ServerSideApplicable<Service> serviceApplicable = mock(ServerSideApplicable.class);
    @SuppressWarnings("unchecked")
    ServiceResource<Service> serviceResource = mock(ServiceResource.class);
    when(serviceResource.forceConflicts()).thenReturn(serviceApplicable);
    when(mockClient.services().resource(any(Service.class))).thenReturn(serviceResource);
    @SuppressWarnings("unchecked")
    ServerSideApplicable<NetworkPolicy> policyApplicable = mock(ServerSideApplicable.class);
    @SuppressWarnings("unchecked")
    Resource<NetworkPolicy> policyResource = mock(Resource.class);
    when(policyResource.forceConflicts()).thenReturn(policyApplicable);
    when(mockClient.network().networkPolicies().resource(any(NetworkPolicy.class)))
        .thenReturn(policyResource);
    when(policyApplicable.serverSideApply())
        .thenThrow(new IllegalArgumentException("Invalid NetworkPolicyPeer"));

    ReconcileProgress progress = clusterInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndImmediateRequeue(), progress);
    verify(mockClient, never()).apps();
    verify(mockContext, never()).getMasterStatefulSetSpec();
    verify(mockContext, never()).getWorkerStatefulSetSpec();
    ArgumentCaptor<ClusterStatus> statusCaptor = ArgumentCaptor.forClass(ClusterStatus.class);
    verify(recorder).persistStatus(any(), statusCaptor.capture());
    Assertions.assertEquals(
        ClusterStateSummary.SchedulingFailure,
        statusCaptor.getValue().getCurrentState().getCurrentStateSummary());
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
  }

  @Test
  @SuppressWarnings("unchecked")
  void admittedKueueWorkloadRequestsMasterAndWorker() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildKueueCluster();
    KubernetesClient mockClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
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

    ReconcileProgress progress = clusterInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    verify(mockClient).resource(any(Workload.class));
    verify(statefulSetApplicable, times(2)).serverSideApply();
    ArgumentCaptor<ClusterStatus> captor = ArgumentCaptor.forClass(ClusterStatus.class);
    verify(recorder).persistStatus(any(), captor.capture());
    Assertions.assertEquals(
        ClusterStateSummary.RunningHealthy,
        captor.getValue().getCurrentState().getCurrentStateSummary());
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
    ArgumentCaptor<ClusterStatus> captor = ArgumentCaptor.forClass(ClusterStatus.class);
    verify(recorder).persistStatus(any(), captor.capture());
    Assertions.assertEquals(
        ClusterStateSummary.RunningHealthy,
        captor.getValue().getCurrentState().getCurrentStateSummary());
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

    // Queued: the Workload waits for the admission
    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        clusterInitStep.reconcile(mockContext, recorder));
    Assertions.assertNotNull(getWorkload());

    // Suspended while queued: the Workload is deleted so that it does not hold the quota
    cluster.getSpec().setSuspend(true);
    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        clusterInitStep.reconcile(mockContext, recorder));
    Assertions.assertNull(getWorkload());
    verify(mockContext, never()).getMasterServiceSpec();
    verifyNoInteractions(recorder);
  }

  @Test
  void kueueApiFailureIsRetried() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = buildKueueCluster();
    // e.g. the operator lacks the Kueue RBAC rules or Kueue is briefly unreachable
    KubernetesClient failingClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    when(failingClient.resource(masterStatefulSetSpec).get()).thenReturn(null);
    when(failingClient.resource(any(Workload.class)).create())
        .thenThrow(new KubernetesClientException("forbidden", 403, null));
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(failingClient);
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);

    ReconcileProgress progress = clusterInitStep.reconcile(mockContext, recorder);

    // The cluster is not failed permanently, the admission is requested again
    Assertions.assertEquals(
        ReconcileProgress.completeAndRequeueAfter(
            KueueWorkloadUtils.STALE_WORKLOAD_REQUEUE_INTERVAL),
        progress);
    verify(mockContext, never()).getMasterServiceSpec();
    verifyNoInteractions(recorder);
    Assertions.assertEquals(
        ClusterStateSummary.Submitted,
        cluster.getStatus().getCurrentState().getCurrentStateSummary());
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
