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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Map;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ListOptions;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodListBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.api.model.autoscaling.v2.HorizontalPodAutoscaler;
import io.fabric8.kubernetes.api.model.autoscaling.v2.HorizontalPodAutoscalerBuilder;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudgetBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.event.EventRecord;
import io.javaoperatorsdk.operator.api.event.EventType;
import io.javaoperatorsdk.operator.api.event.ResourceEventRecorder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.config.SparkOperatorConf;
import org.apache.spark.k8s.operator.context.SparkClusterContext;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadFactory;
import org.apache.spark.k8s.operator.kueue.v1beta2.Workload;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.spec.ClusterSpec;
import org.apache.spark.k8s.operator.spec.ClusterTolerations;
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
class ClusterSuspendStepTest {
  private KubernetesClient kubernetesClient;

  // The default of spark.kubernetes.operator.reconciler.suspendHoldRequeueIntervalSeconds
  private static final ReconcileProgress SUSPEND_HOLD_PROGRESS =
      ReconcileProgress.completeAndRequeueAfter(Duration.ofMinutes(30));

  private final StatefulSet masterStatefulSetSpec = statefulSet("cluster1-master");
  private final StatefulSet workerStatefulSetSpec = statefulSet("cluster1-worker");
  private final SparkClusterContext mockContext = mock(SparkClusterContext.class);
  private final SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
  private final ResourceEventRecorder eventRecorder = mock(ResourceEventRecorder.class);

  @Test
  void runningClusterWithoutSuspendProceeds() {
    SparkCluster cluster = buildCluster(ClusterStateSummary.RunningHealthy, false);
    stubContext(cluster);
    createRunningCluster();

    Assertions.assertEquals(
        ReconcileProgress.proceed(), new ClusterSuspendStep().reconcile(mockContext, recorder));

    verifyNoInteractions(recorder);
    Assertions.assertNotNull(get(masterStatefulSetSpec));
    Assertions.assertNotNull(get(workerStatefulSetSpec));
  }

  @Test
  void suspendingRunningClusterEntersSuspendedBeforeReleasingResources() {
    SparkCluster cluster = buildCluster(ClusterStateSummary.RunningHealthy, true);
    stubContext(cluster);
    createRunningCluster();

    Assertions.assertEquals(
        ReconcileProgress.completeAndImmediateRequeue(),
        new ClusterSuspendStep().reconcile(mockContext, recorder));

    ClusterState state = captureAppendedState();
    Assertions.assertEquals(ClusterStateSummary.Suspended, state.getCurrentStateSummary());
    Assertions.assertEquals(Constants.CLUSTER_SUSPENDED_MESSAGE, state.getMessage());
    // Nothing is released until Suspended is persisted, so that a cluster which is resumed in the
    // meantime is never left in RunningHealthy without its master and workers.
    Assertions.assertNotNull(get(masterStatefulSetSpec));
    Assertions.assertNotNull(get(workerStatefulSetSpec));
  }

  @Test
  void suspendedStateWhichIsNotPersistedIsRetriedLater() {
    SparkCluster cluster = buildCluster(ClusterStateSummary.RunningHealthy, true);
    stubContext(cluster);
    createRunningCluster();
    when(recorder.appendNewStateAndPersist(any(), any())).thenReturn(false);

    // A status which is rejected, e.g. by a CRD without the Suspended state, is not retried at once
    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        new ClusterSuspendStep().reconcile(mockContext, recorder));

    Assertions.assertNotNull(get(masterStatefulSetSpec));
    Assertions.assertNotNull(get(workerStatefulSetSpec));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void suspendedClusterKeepsKueueWorkloadWhilePodsRemain(boolean suspend) {
    // Resuming waits as well, so that the cluster starts over only once everything is released
    SparkCluster cluster = buildKueueCluster(ClusterStateSummary.Suspended, suspend);
    stubContext(cluster);
    createRunningCluster();
    kubernetesClient.resource(KueueWorkloadFactory.buildWorkload(cluster)).create();
    Service masterService = service("cluster1-master-svc");
    kubernetesClient.resource(masterService).create();

    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        new ClusterSuspendStep().reconcile(mockContext, recorder));

    Assertions.assertNull(get(masterStatefulSetSpec));
    Assertions.assertNull(get(workerStatefulSetSpec));
    // The pods are still terminating, so they keep holding the quota of the Workload
    Assertions.assertNotNull(getWorkload());
    // The Services hold no quota and are applied again on resume
    Assertions.assertNotNull(kubernetesClient.resource(masterService).get());
    verifyNoInteractions(recorder);
  }

  @Test
  void suspendedClusterReleasesKueueWorkloadAfterPodsAreGone() {
    SparkCluster cluster = buildKueueCluster(ClusterStateSummary.Suspended, true);
    stubContext(cluster);
    kubernetesClient.resource(KueueWorkloadFactory.buildWorkload(cluster)).create();

    Assertions.assertEquals(
        SUSPEND_HOLD_PROGRESS, new ClusterSuspendStep().reconcile(mockContext, recorder));

    Assertions.assertNull(getWorkload());
    verifyNoInteractions(recorder);
  }

  @Test
  void suspendedClusterReleasesAutoscalerAndDisruptionBudgetByName() {
    SparkCluster cluster = buildCluster(ClusterStateSummary.Suspended, true);
    stubContext(cluster);
    HorizontalPodAutoscaler hpa =
        new HorizontalPodAutoscalerBuilder()
            .withNewMetadata()
            .withName("cluster1-worker-hpa")
            .withNamespace("default")
            .endMetadata()
            .build();
    PodDisruptionBudget pdb =
        new PodDisruptionBudgetBuilder()
            .withNewMetadata()
            .withName("cluster1-worker-pdb")
            .withNamespace("default")
            .endMetadata()
            .build();
    kubernetesClient.resource(hpa).create();
    kubernetesClient.resource(pdb).create();

    // The current spec (minWorkers == maxWorkers) no longer asks for them, but they are released
    Assertions.assertEquals(
        SUSPEND_HOLD_PROGRESS, new ClusterSuspendStep().reconcile(mockContext, recorder));

    Assertions.assertNull(kubernetesClient.resource(hpa).get());
    Assertions.assertNull(kubernetesClient.resource(pdb).get());
    verify(mockContext, never()).getHorizontalPodAutoscalerSpec();
  }

  @Test
  @SuppressWarnings("unchecked")
  void onlyMasterAndWorkerPodsHoldRelease() {
    SparkCluster cluster = buildKueueCluster(ClusterStateSummary.Suspended, true);
    KubernetesClient client = spy(kubernetesClient);
    FilterWatchListDeletable<Pod, PodList, PodResource> rolePods = stubPodList(client);
    // Other pods carry the cluster label too, e.g. a client pod which reaches the workers through
    // their NetworkPolicy, but only the master and workers are listed, and none of them is left
    when(rolePods.list(any(ListOptions.class)))
        .thenReturn(new PodListBuilder().withNewMetadata().endMetadata().build());
    stubContext(cluster, client);
    kubernetesClient.resource(KueueWorkloadFactory.buildWorkload(cluster)).create();

    Assertions.assertEquals(
        SUSPEND_HOLD_PROGRESS, new ClusterSuspendStep().reconcile(mockContext, recorder));

    Assertions.assertNull(getWorkload());
  }

  @Test
  void workloadIsReleasedAfterQueueLabelIsRemoved() {
    SparkCluster cluster = buildKueueCluster(ClusterStateSummary.Suspended, true);
    stubContext(cluster);
    kubernetesClient.resource(KueueWorkloadFactory.buildWorkload(cluster)).create();
    cluster.getMetadata().setLabels(Map.of());
    TestUtils.setConfigKey(SparkOperatorConf.KUEUE_WORKLOAD_INFORMER_ENABLED, true);
    try {
      Assertions.assertEquals(
          SUSPEND_HOLD_PROGRESS, new ClusterSuspendStep().reconcile(mockContext, recorder));
    } finally {
      TestUtils.setConfigKey(SparkOperatorConf.KUEUE_WORKLOAD_INFORMER_ENABLED, false);
    }

    // The Workload admitted before the label was removed does not keep the quota
    Assertions.assertNull(getWorkload());
  }

  @Test
  @SuppressWarnings("unchecked")
  void continueTokenMeansThatPodsRemain() {
    SparkCluster cluster = buildKueueCluster(ClusterStateSummary.Suspended, true);
    KubernetesClient client = spy(kubernetesClient);
    FilterWatchListDeletable<Pod, PodList, PodResource> rolePods = stubPodList(client);
    ArgumentCaptor<ListOptions> options = ArgumentCaptor.forClass(ListOptions.class);
    // An empty page with a continue token, as a limited LIST with a label selector may return
    when(rolePods.list(options.capture()))
        .thenReturn(
            new PodListBuilder().withNewMetadata().withContinue("next").endMetadata().build());
    stubContext(cluster, client);
    kubernetesClient.resource(KueueWorkloadFactory.buildWorkload(cluster)).create();

    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        new ClusterSuspendStep().reconcile(mockContext, recorder));

    Assertions.assertEquals(1L, options.getValue().getLimit());
    Assertions.assertNotNull(getWorkload());
  }

  @Test
  void failedReleaseIsRetriedWithoutReleasingKueueWorkload() {
    SparkCluster cluster = buildKueueCluster(ClusterStateSummary.Suspended, true);
    KubernetesClient mockClient = mock(KubernetesClient.class);
    when(mockClient.resource(workerStatefulSetSpec))
        .thenThrow(new KubernetesClientException("Service Unavailable", 503, null));
    stubContext(cluster, mockClient);

    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        new ClusterSuspendStep().reconcile(mockContext, recorder));

    verify(mockClient, never()).resources(Workload.class);
    verifyNoInteractions(recorder);
    // A failure which may clear on its own is not reported
    verify(mockContext, never()).getEventRecorder();
  }

  @Test
  void rejectedReleaseIsReportedAndRetried() {
    SparkCluster cluster = buildKueueCluster(ClusterStateSummary.Suspended, false);
    KubernetesClient mockClient = mock(KubernetesClient.class);
    when(mockClient.resource(workerStatefulSetSpec))
        .thenThrow(new KubernetesClientException("Forbidden", 403, null));
    stubContext(cluster, mockClient);
    when(mockContext.getEventRecorder()).thenReturn(eventRecorder);

    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        new ClusterSuspendStep().reconcile(mockContext, recorder));

    ArgumentCaptor<EventRecord> event = ArgumentCaptor.forClass(EventRecord.class);
    verify(eventRecorder).record(event.capture());
    Assertions.assertEquals(EventType.WARNING, event.getValue().type());
    Assertions.assertEquals(EventUtils.REASON_SUSPEND_RELEASE_FAILED, event.getValue().reason());
    // It stays Suspended rather than resuming on what it still holds
    verifyNoInteractions(recorder);
  }

  @Test
  @SuppressWarnings("unchecked")
  void failedKueueWorkloadReleaseIsRetried() {
    SparkCluster cluster = buildKueueCluster(ClusterStateSummary.Suspended, false);
    KubernetesClient client = spy(kubernetesClient);
    MixedOperation<Workload, KubernetesResourceList<Workload>, Resource<Workload>> workloads =
        mock(MixedOperation.class);
    NonNamespaceOperation<Workload, KubernetesResourceList<Workload>, Resource<Workload>>
        namespaced = mock(NonNamespaceOperation.class);
    Resource<Workload> workload = mock(Resource.class);
    doReturn(workloads).when(client).resources(Workload.class);
    when(workloads.inNamespace("default")).thenReturn(namespaced);
    when(namespaced.withName("sparkcluster-cluster1")).thenReturn(workload);
    when(workload.delete())
        .thenThrow(new KubernetesClientException("Service Unavailable", 503, null));
    stubContext(cluster, client);

    // The quota is not given back yet, so the cluster is neither held for the hold interval nor
    // resumed on the Workload of the previous spec
    Assertions.assertEquals(
        ReconcileProgress.completeAndDefaultRequeue(),
        new ClusterSuspendStep().reconcile(mockContext, recorder));

    verifyNoInteractions(recorder);
  }

  @Test
  void resumingSuspendedClusterReleasesEverythingBeforeSubmitted() {
    SparkCluster cluster = buildKueueCluster(ClusterStateSummary.Suspended, false);
    stubContext(cluster);
    // Resumed before anything was released: the StatefulSets and the Workload are still there
    kubernetesClient.resource(masterStatefulSetSpec).create();
    kubernetesClient.resource(workerStatefulSetSpec).create();
    kubernetesClient.resource(KueueWorkloadFactory.buildWorkload(cluster)).create();
    long lastStateId = cluster.getStatus().getStateTransitionHistory().lastKey();

    Assertions.assertEquals(
        ReconcileProgress.completeAndImmediateRequeue(),
        new ClusterSuspendStep().reconcile(mockContext, recorder));

    ArgumentCaptor<ClusterStatus> status = ArgumentCaptor.forClass(ClusterStatus.class);
    verify(recorder).persistStatus(eq(mockContext), status.capture());
    ClusterState state = status.getValue().getCurrentState();
    Assertions.assertEquals(ClusterStateSummary.Submitted, state.getCurrentStateSummary());
    Assertions.assertEquals(Constants.CLUSTER_RESUMED_MESSAGE, state.getMessage());
    // The history of the run which is over is dropped, so that it stays bounded
    Assertions.assertEquals(
        Map.of(lastStateId + 1, state), status.getValue().getStateTransitionHistory());
    // Whatever the Workload was admitted for, ClusterInitStep requests a new admission from scratch
    Assertions.assertNull(get(masterStatefulSetSpec));
    Assertions.assertNull(get(workerStatefulSetSpec));
    Assertions.assertNull(getWorkload());
  }

  @Test
  void otherStatesProceed() {
    for (ClusterStateSummary summary :
        new ClusterStateSummary[] {
          ClusterStateSummary.Submitted,
          ClusterStateSummary.SchedulingFailure,
          ClusterStateSummary.Failed,
          ClusterStateSummary.ResourceReleased
        }) {
      SparkCluster cluster = buildCluster(summary, true);
      stubContext(cluster);

      Assertions.assertEquals(
          ReconcileProgress.proceed(), new ClusterSuspendStep().reconcile(mockContext, recorder));
    }
    verifyNoInteractions(recorder);
  }

  private void stubContext(SparkCluster cluster) {
    stubContext(cluster, kubernetesClient);
  }

  private void stubContext(SparkCluster cluster, KubernetesClient client) {
    when(recorder.appendNewStateAndPersist(any(), any())).thenReturn(true);
    when(recorder.persistStatus(any(), any())).thenReturn(true);
    when(mockContext.getResource()).thenReturn(cluster);
    when(mockContext.getClient()).thenReturn(client);
    when(mockContext.getMasterStatefulSetSpec()).thenReturn(masterStatefulSetSpec);
    when(mockContext.getWorkerStatefulSetSpec()).thenReturn(workerStatefulSetSpec);
  }

  /**
   * Stubs the LIST of the master and worker pods of cluster1 on the given spy, since the mock
   * server does not evaluate set-based label selectors, and returns the filter to list them with.
   */
  @SuppressWarnings("unchecked")
  private static FilterWatchListDeletable<Pod, PodList, PodResource> stubPodList(
      KubernetesClient client) {
    MixedOperation<Pod, PodList, PodResource> pods = mock(MixedOperation.class);
    NonNamespaceOperation<Pod, PodList, PodResource> namespacedPods =
        mock(NonNamespaceOperation.class);
    FilterWatchListDeletable<Pod, PodList, PodResource> labeledPods =
        mock(FilterWatchListDeletable.class);
    FilterWatchListDeletable<Pod, PodList, PodResource> rolePods =
        mock(FilterWatchListDeletable.class);
    doReturn(pods).when(client).pods();
    when(pods.inNamespace("default")).thenReturn(namespacedPods);
    when(namespacedPods.withLabel(Constants.LABEL_SPARK_CLUSTER_NAME, "cluster1"))
        .thenReturn(labeledPods);
    when(labeledPods.withLabelIn(
            Constants.LABEL_SPARK_ROLE_NAME,
            Constants.LABEL_SPARK_ROLE_MASTER_VALUE,
            Constants.LABEL_SPARK_ROLE_WORKER_VALUE))
        .thenReturn(rolePods);
    return rolePods;
  }

  private void createRunningCluster() {
    kubernetesClient.resource(masterStatefulSetSpec).create();
    kubernetesClient.resource(workerStatefulSetSpec).create();
    kubernetesClient
        .resource(pod("cluster1-master-0", Constants.LABEL_SPARK_ROLE_MASTER_VALUE))
        .create();
    kubernetesClient
        .resource(pod("cluster1-worker-0", Constants.LABEL_SPARK_ROLE_WORKER_VALUE))
        .create();
  }

  private ClusterState captureAppendedState() {
    ArgumentCaptor<ClusterState> captor = ArgumentCaptor.forClass(ClusterState.class);
    verify(recorder).appendNewStateAndPersist(eq(mockContext), captor.capture());
    verify(recorder, never()).persistStatus(any(), any());
    return captor.getValue();
  }

  private StatefulSet get(StatefulSet statefulSet) {
    return kubernetesClient.resource(statefulSet).get();
  }

  private Workload getWorkload() {
    return kubernetesClient
        .resources(Workload.class)
        .inNamespace("default")
        .withName("sparkcluster-cluster1")
        .get();
  }

  private static SparkCluster buildCluster(ClusterStateSummary summary, boolean suspend) {
    SparkCluster cluster = new SparkCluster();
    cluster.setMetadata(
        new ObjectMetaBuilder()
            .withName("cluster1")
            .withNamespace("default")
            .withUid("cluster-uid")
            .build());
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
    cluster.getSpec().setSuspend(suspend);
    cluster.setStatus(cluster.getStatus().appendNewState(new ClusterState(summary, "")));
    return cluster;
  }

  private static SparkCluster buildKueueCluster(ClusterStateSummary summary, boolean suspend) {
    SparkCluster cluster = buildCluster(summary, suspend);
    cluster.getMetadata().setLabels(Map.of(Constants.LABEL_QUEUE_NAME, "cluster-queue"));
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

  private static Pod pod(String name, String role) {
    return new PodBuilder()
        .withNewMetadata()
        .withName(name)
        .withNamespace("default")
        .addToLabels(Constants.LABEL_SPARK_CLUSTER_NAME, "cluster1")
        .addToLabels(Constants.LABEL_SPARK_ROLE_NAME, role)
        .endMetadata()
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
}
