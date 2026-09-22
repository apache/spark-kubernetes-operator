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

package org.apache.spark.k8s.operator.kueue;

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ConditionBuilder;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.scheduling.v1.PriorityClass;
import io.fabric8.kubernetes.api.model.scheduling.v1.PriorityClassBuilder;
import io.fabric8.kubernetes.api.model.scheduling.v1.PriorityClassList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.api.event.EventRecord;
import io.javaoperatorsdk.operator.api.event.ResourceEventRecorder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadUtils.AdmissionResponse;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadUtils.AdmissionResult;
import org.apache.spark.k8s.operator.kueue.v1beta2.Admission;
import org.apache.spark.k8s.operator.kueue.v1beta2.PodSet;
import org.apache.spark.k8s.operator.kueue.v1beta2.PodSetAssignment;
import org.apache.spark.k8s.operator.kueue.v1beta2.PriorityClassRef;
import org.apache.spark.k8s.operator.kueue.v1beta2.ResourceFlavor;
import org.apache.spark.k8s.operator.kueue.v1beta2.ResourceFlavorSpec;
import org.apache.spark.k8s.operator.kueue.v1beta2.Workload;
import org.apache.spark.k8s.operator.kueue.v1beta2.WorkloadPriorityClass;
import org.apache.spark.k8s.operator.kueue.v1beta2.WorkloadSpec;
import org.apache.spark.k8s.operator.kueue.v1beta2.WorkloadStatus;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.utils.EventUtils;

@EnableKubernetesMockClient(crud = true)
@SuppressFBWarnings(
    value = {"UWF_UNWRITTEN_FIELD", "NP_UNWRITTEN_FIELD"},
    justification = "Unwritten fields are covered by Kubernetes mock client")
class KueueWorkloadUtilsTest {
  private static final String NAME = "sparkapplication-app-1";

  private KubernetesClient kubernetesClient;

  @Test
  void newWorkloadIsCreatedAndPendingUntilAdmitted() {
    Workload desired = workload("owner-uid-1", 1);

    Assertions.assertEquals(
        AdmissionResult.PENDING, KueueWorkloadUtils.requestAdmission(kubernetesClient, desired)
            .result());
    Workload created = getWorkload();
    Assertions.assertNotNull(created);
    Assertions.assertEquals("test-queue", created.getSpec().getQueueName());
    Assertions.assertEquals(
        KueueWorkloadUtils.hashPodSets(desired),
        created.getMetadata().getAnnotations().get(KueueWorkloadUtils.ANNOTATION_POD_SETS_HASH));

    // A second reconciliation reuses the same Workload and still waits for the admission
    Assertions.assertEquals(
        AdmissionResult.PENDING,
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1)).result());
    Assertions.assertEquals(
        created.getMetadata().getUid(), getWorkload().getMetadata().getUid());
  }

  @Test
  void admittedWorkloadIsReported() {
    KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1));
    admitWorkload();

    AdmissionResponse response =
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1));
    Assertions.assertEquals(AdmissionResult.ADMITTED, response.result());
    // The admitted Workload is returned so that its flavors can be applied to the pods
    Assertions.assertTrue(KueueWorkloadUtils.isAdmitted(response.workload()));
    Assertions.assertEquals(
        getWorkload().getMetadata().getUid(), response.workload().getMetadata().getUid());
  }

  @Test
  void admittedWorkloadIsKeptEvenIfPodSetsChanged() {
    KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1));
    admitWorkload();

    Assertions.assertEquals(
        AdmissionResult.ADMITTED,
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 5)).result());
    Assertions.assertNotNull(getWorkload());
  }

  @Test
  void pendingWorkloadWithOutdatedPodSetsIsRecreated() {
    KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1));

    // The spec changed while waiting for the admission
    Assertions.assertEquals(
        AdmissionResult.STALE,
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 5)).result());
    Assertions.assertNull(getWorkload());

    Assertions.assertEquals(
        AdmissionResult.PENDING,
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 5)).result());
    Assertions.assertEquals(5, getWorkload().getSpec().getPodSets().get(0).getCount());
  }

  @Test
  void workloadOwnedByAnotherResourceIsDeleted() {
    KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("stale-owner-uid", 1));
    admitWorkload();

    Assertions.assertEquals(
        AdmissionResult.STALE,
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1)).result());
    Assertions.assertNull(getWorkload());
  }

  @Test
  void terminatingWorkloadIsNotUsed() {
    Workload terminating = workload("owner-uid-1", 1);
    terminating.getMetadata().setFinalizers(List.of("kueue.x-k8s.io/resource-in-use"));
    KueueWorkloadUtils.requestAdmission(kubernetesClient, terminating);
    admitWorkload();
    kubernetesClient.resources(Workload.class).inNamespace("default").withName(NAME).delete();
    Assertions.assertNotNull(getWorkload().getMetadata().getDeletionTimestamp());

    Assertions.assertEquals(
        AdmissionResult.STALE,
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1)).result());
  }

  @Test
  void admissionChangeIsDetected() {
    Workload pending = workload("owner-uid-1", 1);
    Workload quotaReserved = workload("owner-uid-1", 1);
    quotaReserved.setStatus(status("QuotaReserved", "True"));
    Workload admitted = workload("owner-uid-1", 1);
    admitted.setStatus(status("Admitted", "True"));
    Workload withoutStatus = workload("owner-uid-1", 1);
    withoutStatus.setStatus(null);

    Assertions.assertFalse(KueueWorkloadUtils.isAdmitted(pending));
    Assertions.assertFalse(KueueWorkloadUtils.isAdmitted(quotaReserved));
    Assertions.assertFalse(KueueWorkloadUtils.isAdmitted(withoutStatus));
    Assertions.assertTrue(KueueWorkloadUtils.isAdmitted(admitted));

    Assertions.assertTrue(KueueWorkloadUtils.isAdmissionChanged(admitted, quotaReserved));
    Assertions.assertTrue(KueueWorkloadUtils.isAdmissionChanged(withoutStatus, admitted));
    // Status updates of a pending Workload do not change its admission
    Assertions.assertFalse(KueueWorkloadUtils.isAdmissionChanged(quotaReserved, pending));
    Assertions.assertFalse(KueueWorkloadUtils.isAdmissionChanged(pending, withoutStatus));
    Assertions.assertFalse(KueueWorkloadUtils.isAdmissionChanged(admitted, admitted));
  }

  @Test
  void hashPodSetsIsStableForTheSameSpec() {
    Map<String, String> sparkConf = new HashMap<>();
    sparkConf.put("spark.executor.instances", "2");
    for (int i = 0; i < 12; i++) {
      sparkConf.put("spark.kubernetes.node.selector.key" + i, "value" + i);
    }
    String hash = hashPodSets(app(sparkConf));
    Assertions.assertEquals(hash, hashPodSets(app(sparkConf)));

    sparkConf.put("spark.kubernetes.node.selector.key0", "changed");
    Assertions.assertNotEquals(hash, hashPodSets(app(sparkConf)));
  }

  @Test
  void hashPodSetsIgnoresMapEntryOrder() {
    Map<String, String> ordered = new LinkedHashMap<>();
    Map<String, String> reversed = new LinkedHashMap<>();
    for (int i = 0; i < 12; i++) {
      ordered.put("key" + i, "value" + i);
      reversed.put("key" + (11 - i), "value" + (11 - i));
    }
    Assertions.assertEquals(
        KueueWorkloadUtils.hashPodSets(workloadWithNodeSelector(ordered)),
        KueueWorkloadUtils.hashPodSets(workloadWithNodeSelector(reversed)));
  }

  @Test
  void releaseWorkloadDeletesWorkload() {
    KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1));

    KueueWorkloadUtils.releaseWorkload(kubernetesClient, owner());

    Assertions.assertNull(getWorkload());
    // Releasing again is a no-op
    KueueWorkloadUtils.releaseWorkload(kubernetesClient, owner());
  }

  @Test
  @SuppressWarnings("unchecked")
  void releaseWorkloadIgnoresFailures() {
    KubernetesClient client = mock(KubernetesClient.class);
    MixedOperation<Workload, KubernetesResourceList<Workload>, Resource<Workload>> operation =
        mock(MixedOperation.class);
    NonNamespaceOperation<Workload, KubernetesResourceList<Workload>, Resource<Workload>>
        namespaced = mock(NonNamespaceOperation.class);
    Resource<Workload> resource = mock(Resource.class);
    when(client.resources(Workload.class)).thenReturn(operation);
    when(operation.inNamespace("default")).thenReturn(namespaced);
    when(namespaced.withName(NAME)).thenReturn(resource);
    when(resource.delete()).thenThrow(new KubernetesClientException("forbidden", 403, null));

    Assertions.assertDoesNotThrow(() -> KueueWorkloadUtils.releaseWorkload(client, owner()));
  }

  @Test
  void resolvePodSetFlavorsMergesFlavorsOfEachPodSet() {
    Toleration spot = toleration("spot");
    Toleration gpu = toleration("gpu");
    createFlavor("cpu-flavor", Map.of("pool", "cpu", "zone", "a"), List.of(spot));
    createFlavor("gpu-flavor", Map.of("pool", "gpu", "accelerator", "a100"), List.of(spot, gpu));
    // Like Kueue, a flavor assigned to several resources is applied once
    Map<String, String> driverFlavors = Map.of("cpu", "cpu-flavor", "memory", "cpu-flavor");
    // Like Kueue, a later flavor overwrites a node label, which is in the resource name order.
    // The reverse insertion order pins the assertion to the sorting of `resolvePodSetFlavors`.
    Map<String, String> executorFlavors = new LinkedHashMap<>();
    executorFlavors.put("nvidia.com/gpu", "gpu-flavor");
    executorFlavors.put("cpu", "cpu-flavor");

    Map<String, KueuePodSetFlavor> flavors =
        KueueWorkloadUtils.resolvePodSetFlavors(
            kubernetesClient,
            admittedWorkload(Map.of("driver", driverFlavors, "executor", executorFlavors)));

    Assertions.assertEquals(
        Map.of(
            "driver",
            new KueuePodSetFlavor(Map.of("pool", "cpu", "zone", "a"), List.of(spot)),
            "executor",
            new KueuePodSetFlavor(
                Map.of("pool", "gpu", "zone", "a", "accelerator", "a100"), List.of(spot, gpu))),
        flavors);
  }

  @Test
  void resolvePodSetFlavorsWithoutAdmissionIsEmpty() {
    Workload admitted = workload("owner-uid-1", 1);
    admitted.setStatus(status("Admitted", "True"));

    Assertions.assertEquals(
        Map.of(), KueueWorkloadUtils.resolvePodSetFlavors(kubernetesClient, admitted));
    Assertions.assertEquals(
        Map.of(),
        KueueWorkloadUtils.resolvePodSetFlavors(kubernetesClient, admittedWorkload(Map.of())));
  }

  @Test
  void resolvePodSetFlavorsSkipsFlavorsWithoutNodeLabelsAndTolerations() {
    // Kueue's stock `default-flavor` has neither, so its pod sets are built as they are without
    // Kueue, instead of being rebuilt around an empty pod template.
    createFlavor("default-flavor", Map.of(), List.of());
    createFlavor("spot-flavor", Map.of("pool", "spot"), List.of());

    Map<String, KueuePodSetFlavor> flavors =
        KueueWorkloadUtils.resolvePodSetFlavors(
            kubernetesClient,
            admittedWorkload(
                Map.of(
                    "driver", Map.of("cpu", "default-flavor"),
                    "executor", Map.of("cpu", "spot-flavor"))));

    Assertions.assertEquals(
        Map.of("executor", new KueuePodSetFlavor(Map.of("pool", "spot"), List.of())), flavors);
  }

  @Test
  void checkNoNodeSelectorConflictAllowsTheSameNodeSelector() {
    Map<String, KueuePodSetFlavor> flavors =
        Map.of("executor", new KueuePodSetFlavor(Map.of("pool", "cpu"), List.of()));

    Assertions.assertDoesNotThrow(
        () ->
            KueueWorkloadUtils.checkNoNodeSelectorConflict(
                flavors, workloadWithNodeSelector(Map.of("pool", "cpu", "zone", "a"))));
  }

  @Test
  void checkNoNodeSelectorConflictFailsOnDifferentValue() {
    Map<String, KueuePodSetFlavor> flavors =
        Map.of("executor", new KueuePodSetFlavor(Map.of("pool", "cpu"), List.of()));

    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                KueueWorkloadUtils.checkNoNodeSelectorConflict(
                    flavors, workloadWithNodeSelector(Map.of("pool", "gpu"))));
    Assertions.assertTrue(e.getMessage().contains("executor"), e.getMessage());
    Assertions.assertTrue(e.getMessage().contains("pool"), e.getMessage());
  }

  @Test
  void resolvePodSetFlavorsFailsOnMissingFlavor() {
    KubernetesClientException e =
        Assertions.assertThrows(
            KubernetesClientException.class,
            () ->
                KueueWorkloadUtils.resolvePodSetFlavors(
                    kubernetesClient,
                    admittedWorkload(Map.of("executor", Map.of("cpu", "missing-flavor")))));
    Assertions.assertEquals(404, e.getCode());
  }

  @Test
  @SuppressWarnings("unchecked")
  void rejectedAdmissionWithFailedReleaseIsRetried() {
    // The Workload of a resource which will not start must not keep holding the quota, so the
    // release is retried instead of failing the resource while its Workload is still admitted.
    Workload desired = workloadWithNodeSelector(Map.of("pool", "on-demand"));
    KubernetesClient client = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    // Mockito cannot deep-stub the generic list, so it is stubbed without any priority class
    when(client.scheduling().v1().priorityClasses().list()).thenReturn(new PriorityClassList());
    when(client.resource(any(Workload.class)).get())
        .thenReturn(admittedWorkload(Map.of("executor", Map.of("cpu", "spot-flavor"))));
    MixedOperation<ResourceFlavor, KubernetesResourceList<ResourceFlavor>, Resource<ResourceFlavor>>
        flavors = mock(MixedOperation.class);
    Resource<ResourceFlavor> flavorResource = mock(Resource.class);
    when(client.resources(ResourceFlavor.class)).thenReturn(flavors);
    when(flavors.withName("spot-flavor")).thenReturn(flavorResource);
    when(flavorResource.get())
        .thenReturn(flavorOf("spot-flavor", Map.of("pool", "spot"), List.of()));
    MixedOperation<Workload, KubernetesResourceList<Workload>, Resource<Workload>> workloads =
        mock(MixedOperation.class);
    NonNamespaceOperation<Workload, KubernetesResourceList<Workload>, Resource<Workload>>
        namespaced = mock(NonNamespaceOperation.class);
    Resource<Workload> workloadResource = mock(Resource.class);
    when(client.resources(Workload.class)).thenReturn(workloads);
    when(workloads.inNamespace("default")).thenReturn(namespaced);
    when(namespaced.withName(NAME)).thenReturn(workloadResource);
    when(workloadResource.delete())
        .thenThrow(new KubernetesClientException("forbidden", 403, null));
    SparkAppContext context = mock(SparkAppContext.class);
    when(context.getClient()).thenReturn(client);
    when(context.getResource()).thenReturn(owner());
    when(context.getEventRecorder()).thenReturn(mock(ResourceEventRecorder.class));

    Assertions.assertEquals(
        Optional.of(ReconcileProgress.completeAndDefaultRequeue()),
        KueueWorkloadUtils.holdForAdmission(context, desired, "driver"));
    verify(context, never()).setKueuePodSetFlavors(any());
  }

  @Test
  void admittedFlavorsAreAppliedWithoutRequestingTheAdmission() {
    // A resource whose driver or master exists applies its resources again, so the flavors of the
    // Workload which was admitted before are resolved again rather than dropped.
    createFlavor("spot-flavor", Map.of("pool", "spot"), List.of(toleration("spot")));
    KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1));
    admit(admittedWorkload(Map.of("executor", Map.of("cpu", "spot-flavor"))).getStatus());
    SparkAppContext context = context(kubernetesClient);

    Assertions.assertEquals(Optional.empty(), KueueWorkloadUtils.applyAdmittedFlavors(context));

    verify(context)
        .setKueuePodSetFlavors(
            Map.of(
                "executor",
                new KueuePodSetFlavor(Map.of("pool", "spot"), List.of(toleration("spot")))));
  }

  @Test
  void admittedFlavorsOfAPendingOrMissingWorkloadAreNotApplied() {
    SparkAppContext context = context(kubernetesClient);

    // The Workload is gone, e.g. evicted and deleted, which must not hold the running resources
    Assertions.assertEquals(Optional.empty(), KueueWorkloadUtils.applyAdmittedFlavors(context));

    KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1));
    Assertions.assertEquals(Optional.empty(), KueueWorkloadUtils.applyAdmittedFlavors(context));
    verify(context, never()).setKueuePodSetFlavors(any());
  }

  @Test
  void admittedFlavorsWithUnreadableFlavorAreRetried() {
    KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1));
    admit(admittedWorkload(Map.of("executor", Map.of("cpu", "missing-flavor"))).getStatus());
    SparkAppContext context = context(kubernetesClient);

    // Applying no flavors would drop them from the resources which are applied again
    Assertions.assertEquals(
        Optional.of(ReconcileProgress.completeAndDefaultRequeue()),
        KueueWorkloadUtils.applyAdmittedFlavors(context));
    verify(context, never()).setKueuePodSetFlavors(any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void admittedFlavorsRejectedByRbacReportTheClusterRole() {
    KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1));
    admit(admittedWorkload(Map.of("executor", Map.of("cpu", "spot-flavor"))).getStatus());
    KubernetesClient client = mock(KubernetesClient.class, delegatesTo(kubernetesClient));
    MixedOperation<ResourceFlavor, KubernetesResourceList<ResourceFlavor>, Resource<ResourceFlavor>>
        flavors = mock(MixedOperation.class);
    Resource<ResourceFlavor> flavorResource = mock(Resource.class);
    when(client.resources(ResourceFlavor.class)).thenReturn(flavors);
    when(flavors.withName("spot-flavor")).thenReturn(flavorResource);
    when(flavorResource.get()).thenThrow(new KubernetesClientException("forbidden", 403, null));
    ResourceEventRecorder eventRecorder = mock(ResourceEventRecorder.class);
    SparkAppContext context = context(client);
    when(context.getEventRecorder()).thenReturn(eventRecorder);

    Assertions.assertEquals(
        Optional.of(ReconcileProgress.completeAndDefaultRequeue()),
        KueueWorkloadUtils.applyAdmittedFlavors(context));

    ArgumentCaptor<EventRecord> captor = ArgumentCaptor.forClass(EventRecord.class);
    verify(eventRecorder).record(captor.capture());
    Assertions.assertEquals(
        EventUtils.REASON_KUEUE_RESOURCE_FLAVOR_READ_FAILED, captor.getValue().reason());
    Assertions.assertTrue(
        captor.getValue().message().contains("ClusterRole"), captor.getValue().message());
  }

  private SparkAppContext context(final KubernetesClient client) {
    SparkAppContext context = mock(SparkAppContext.class);
    when(context.getClient()).thenReturn(client);
    when(context.getResource()).thenReturn(owner());
    when(context.getEventRecorder()).thenReturn(mock(ResourceEventRecorder.class));
    return context;
  }

  private void admit(final WorkloadStatus status) {
    Workload workload = getWorkload();
    workload.setStatus(status);
    kubernetesClient.resource(workload).update();
  }

  private void createFlavor(
      final String name, final Map<String, String> nodeLabels, final List<Toleration> tolerations) {
    kubernetesClient.resource(flavorOf(name, nodeLabels, tolerations)).create();
  }

  private static ResourceFlavor flavorOf(
      final String name, final Map<String, String> nodeLabels, final List<Toleration> tolerations) {
    ResourceFlavor flavor = new ResourceFlavor();
    flavor.setMetadata(new ObjectMetaBuilder().withName(name).build());
    flavor.setSpec(
        ResourceFlavorSpec.builder().nodeLabels(nodeLabels).tolerations(tolerations).build());
    return flavor;
  }

  private static Workload admittedWorkload(final Map<String, Map<String, String>> podSetFlavors) {
    Workload workload = workload("owner-uid-1", 1);
    WorkloadStatus status = status("Admitted", "True");
    status.setAdmission(
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
            .build());
    workload.setStatus(status);
    return workload;
  }

  private static Toleration toleration(final String key) {
    return new Toleration("NoSchedule", key, "Exists", null, null);
  }

  @Test
  void workloadPriorityClassLabelTakesPrecedence() {
    createWorkloadPriorityClass("high", 1000);
    createPriorityClass("driver-priority", 100, false);
    Workload desired = workloadWithPriorityClasses("driver-priority", null);
    desired.getMetadata().setLabels(Map.of(Constants.LABEL_WORKLOAD_PRIORITY_CLASS, "high"));

    Assertions.assertEquals(
        AdmissionResult.PENDING, KueueWorkloadUtils.requestAdmission(kubernetesClient, desired)
            .result());
    Assertions.assertEquals(
        new PriorityClassRef("kueue.x-k8s.io", "WorkloadPriorityClass", "high"),
        getWorkload().getSpec().getPriorityClassRef());
    Assertions.assertEquals(1000, getWorkload().getSpec().getPriority());
  }

  @Test
  void priorityClassOfTheFirstPodSetIsUsed() {
    createPriorityClass("driver-priority", 100, false);
    createPriorityClass("executor-priority", 200, false);

    Workload desired = workloadWithPriorityClasses("driver-priority", "executor-priority");
    KueueWorkloadPriority.setPriority(kubernetesClient, desired);
    Assertions.assertEquals(
        new PriorityClassRef("scheduling.k8s.io", "PriorityClass", "driver-priority"),
        desired.getSpec().getPriorityClassRef());
    Assertions.assertEquals(100, desired.getSpec().getPriority());

    // A pod set without a priority class is skipped
    desired = workloadWithPriorityClasses(null, "executor-priority");
    KueueWorkloadPriority.setPriority(kubernetesClient, desired);
    Assertions.assertEquals(
        new PriorityClassRef("scheduling.k8s.io", "PriorityClass", "executor-priority"),
        desired.getSpec().getPriorityClassRef());
    Assertions.assertEquals(200, desired.getSpec().getPriority());
  }

  @Test
  void globalDefaultPriorityClassIsUsedWithoutPriorityClass() {
    createPriorityClass("not-default", 10, false);
    createPriorityClass("default-high", 100, true);
    createPriorityClass("default-low", 50, true);

    Workload desired = workloadWithPriorityClasses(null, null);
    KueueWorkloadPriority.setPriority(kubernetesClient, desired);
    // Like Kueue, the lowest one wins if there are more than one global default
    Assertions.assertEquals(
        new PriorityClassRef("scheduling.k8s.io", "PriorityClass", "default-low"),
        desired.getSpec().getPriorityClassRef());
    Assertions.assertEquals(50, desired.getSpec().getPriority());
  }

  @Test
  void priorityIsZeroWithoutAnyPriorityClass() {
    Workload desired = workloadWithPriorityClasses(null, null);
    KueueWorkloadPriority.setPriority(kubernetesClient, desired);
    Assertions.assertNull(desired.getSpec().getPriorityClassRef());
    Assertions.assertEquals(0, desired.getSpec().getPriority());
  }

  @Test
  void workloadIsNotCreatedWithMissingPriorityClass() {
    Workload desired = workload("owner-uid-1", 1);
    desired.getMetadata().setLabels(Map.of(Constants.LABEL_WORKLOAD_PRIORITY_CLASS, "missing"));
    Assertions.assertThrows(
        IllegalStateException.class,
        () -> KueueWorkloadUtils.requestAdmission(kubernetesClient, desired));
    Assertions.assertNull(getWorkload());

    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            KueueWorkloadUtils.requestAdmission(
                kubernetesClient, workloadWithPriorityClasses("missing", null)));
    Assertions.assertNull(getWorkload());
  }

  @Test
  @SuppressWarnings("unchecked")
  void priorityIsNotSetWithoutPermission() {
    KubernetesClient client = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    KubernetesClientException forbidden = new KubernetesClientException("forbidden", 403, null);
    MixedOperation<
            WorkloadPriorityClass,
            KubernetesResourceList<WorkloadPriorityClass>,
            Resource<WorkloadPriorityClass>>
        operation = mock(MixedOperation.class);
    Resource<WorkloadPriorityClass> resource = mock(Resource.class);
    when(client.resources(WorkloadPriorityClass.class)).thenReturn(operation);
    when(operation.withName("high")).thenReturn(resource);
    when(resource.get()).thenThrow(forbidden);
    when(client.scheduling().v1().priorityClasses().list()).thenThrow(forbidden);

    Workload labeled = workload("owner-uid-1", 1);
    labeled.getMetadata().setLabels(Map.of(Constants.LABEL_WORKLOAD_PRIORITY_CLASS, "high"));
    KueueWorkloadPriority.setPriority(client, labeled);
    Assertions.assertNull(labeled.getSpec().getPriorityClassRef());
    Assertions.assertNull(labeled.getSpec().getPriority());

    Workload unlabeled = workload("owner-uid-1", 1);
    KueueWorkloadPriority.setPriority(client, unlabeled);
    Assertions.assertNull(unlabeled.getSpec().getPriorityClassRef());
    Assertions.assertNull(unlabeled.getSpec().getPriority());

    // Other failures are not ignored
    KubernetesClient unavailableClient = mock(KubernetesClient.class, RETURNS_DEEP_STUBS);
    when(unavailableClient.scheduling().v1().priorityClasses().list())
        .thenThrow(new KubernetesClientException("unavailable", 503, null));
    Assertions.assertThrows(
        KubernetesClientException.class,
        () -> KueueWorkloadPriority.setPriority(unavailableClient, workload("owner-uid-1", 1)));
  }

  @Test
  void pendingWorkloadFollowsPriorityClassChange() {
    createWorkloadPriorityClass("low", 10);
    createWorkloadPriorityClass("high", 1000);
    KueueWorkloadUtils.requestAdmission(kubernetesClient, workloadWithPriorityClass("low"));
    String uid = getWorkload().getMetadata().getUid();
    Assertions.assertEquals(10, getWorkload().getSpec().getPriority());

    Assertions.assertEquals(
        AdmissionResult.PENDING,
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workloadWithPriorityClass("high"))
            .result());
    // The Workload is updated in place so that it keeps its position in the queue
    Workload updated = getWorkload();
    Assertions.assertEquals(uid, updated.getMetadata().getUid());
    Assertions.assertEquals("high", updated.getSpec().getPriorityClassRef().getName());
    Assertions.assertEquals(1000, updated.getSpec().getPriority());
  }

  @Test
  void pendingWorkloadKeepsPriorityOfTheSameClass() {
    createWorkloadPriorityClass("low", 10);
    KueueWorkloadUtils.requestAdmission(kubernetesClient, workloadWithPriorityClass("low"));
    // Like Kueue, a changed value of the class does not affect the existing Workload
    kubernetesClient
        .resources(WorkloadPriorityClass.class)
        .withName("low")
        .edit(
            workloadPriorityClass -> {
              workloadPriorityClass.setValue(20);
              return workloadPriorityClass;
            });

    Assertions.assertEquals(
        AdmissionResult.PENDING,
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workloadWithPriorityClass("low"))
            .result());
    Assertions.assertEquals(10, getWorkload().getSpec().getPriority());
  }

  @Test
  void quotaReservedWorkloadKeepsFrozenPriorityClass() {
    createWorkloadPriorityClass("low", 10);
    KueueWorkloadUtils.requestAdmission(kubernetesClient, workloadWithPriorityClass("low"));
    reserveQuota();

    // Kueue freezes the presence of the priority class once the quota is reserved
    Assertions.assertEquals(
        AdmissionResult.PENDING,
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1)).result());
    Assertions.assertEquals("low", getWorkload().getSpec().getPriorityClassRef().getName());
    Assertions.assertEquals(10, getWorkload().getSpec().getPriority());
  }

  @Test
  void quotaReservedWorkloadFollowsWorkloadPriorityClassChange() {
    createWorkloadPriorityClass("low", 10);
    createWorkloadPriorityClass("high", 1000);
    KueueWorkloadUtils.requestAdmission(kubernetesClient, workloadWithPriorityClass("low"));
    reserveQuota();

    // Unlike its group and kind, the name of a WorkloadPriorityClass stays mutable, so that the
    // priority of a Workload waiting for its admission checks can still be raised
    Assertions.assertEquals(
        AdmissionResult.PENDING,
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workloadWithPriorityClass("high"))
            .result());
    Assertions.assertEquals("high", getWorkload().getSpec().getPriorityClassRef().getName());
    Assertions.assertEquals(1000, getWorkload().getSpec().getPriority());
  }

  @Test
  void pendingWorkloadKeepsItsPriorityWithoutPermission() {
    createWorkloadPriorityClass("high", 1000);
    KueueWorkloadUtils.requestAdmission(kubernetesClient, workloadWithPriorityClass("high"));
    Assertions.assertEquals(1000, getWorkload().getSpec().getPriority());

    // The operator loses the permission while the Workload waits for quota
    Assertions.assertEquals(
        AdmissionResult.PENDING,
        KueueWorkloadUtils.requestAdmission(forbiddenClient(), workloadWithPriorityClass("high"))
            .result());
    Assertions.assertEquals("high", getWorkload().getSpec().getPriorityClassRef().getName());
    Assertions.assertEquals(1000, getWorkload().getSpec().getPriority());
  }

  @Test
  void pendingWorkloadWithPodPriorityClassDoesNotFollowTheLabel() {
    createPriorityClass("default-a", 50, true);
    createWorkloadPriorityClass("high", 1000);
    KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1));

    // Like Kueue, a Workload backed by a Kubernetes PriorityClass does not follow the label
    Assertions.assertEquals(
        AdmissionResult.PENDING,
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workloadWithPriorityClass("high"))
            .result());
    Assertions.assertEquals("default-a", getWorkload().getSpec().getPriorityClassRef().getName());
    Assertions.assertEquals(50, getWorkload().getSpec().getPriority());
  }

  @Test
  void pendingWorkloadKeepsItsPodPriorityClassName() {
    createPriorityClass("default-a", 50, true);
    KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1));

    // The cluster-wide default changed, which Kueue does not apply to an existing Workload
    kubernetesClient.resource(priorityClass("default-a", 50, false)).update();
    createPriorityClass("default-b", 70, true);

    Assertions.assertEquals(
        AdmissionResult.PENDING,
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1)).result());
    Assertions.assertEquals("default-a", getWorkload().getSpec().getPriorityClassRef().getName());
    Assertions.assertEquals(50, getWorkload().getSpec().getPriority());
  }

  @Test
  void quotaReservedWorkloadDoesNotSwitchPriorityClassGroup() {
    createWorkloadPriorityClass("low", 10);
    createPriorityClass("default-a", 50, true);
    KueueWorkloadUtils.requestAdmission(kubernetesClient, workloadWithPriorityClass("low"));
    reserveQuota();

    // Removing the label would switch the group and the kind, which Kueue freezes
    Assertions.assertEquals(
        AdmissionResult.PENDING,
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1)).result());
    Assertions.assertEquals(
        "kueue.x-k8s.io", getWorkload().getSpec().getPriorityClassRef().getGroup());
    Assertions.assertEquals(10, getWorkload().getSpec().getPriority());
  }

  @Test
  void pendingWorkloadWithoutPriorityClassFollowsAnAddedLabel() {
    createWorkloadPriorityClass("high", 1000);
    // No label and no pod priority class, so the Workload is created without a ref
    KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1));
    Assertions.assertNull(getWorkload().getSpec().getPriorityClassRef());

    // The label is added while the Workload waits for quota
    Assertions.assertEquals(
        AdmissionResult.PENDING,
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workloadWithPriorityClass("high"))
            .result());
    Assertions.assertEquals("high", getWorkload().getSpec().getPriorityClassRef().getName());
    Assertions.assertEquals(1000, getWorkload().getSpec().getPriority());
  }

  @Test
  void quotaReservedWorkloadWithoutPriorityClassDoesNotGainOne() {
    createWorkloadPriorityClass("high", 1000);
    // The Workload is created while the operator cannot read the classes, so it has no ref
    KueueWorkloadUtils.requestAdmission(forbiddenClient(), workloadWithPriorityClass("high"));
    Assertions.assertNull(getWorkload().getSpec().getPriorityClassRef());
    reserveQuota();

    // The permission is back, but Kueue no longer accepts adding a priority class
    Assertions.assertEquals(
        AdmissionResult.PENDING,
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workloadWithPriorityClass("high"))
            .result());
    Assertions.assertNull(getWorkload().getSpec().getPriorityClassRef());
  }

  private KubernetesClient forbiddenClient() {
    KubernetesClient client =
        mock(KubernetesClient.class, withSettings().defaultAnswer(delegatesTo(kubernetesClient)));
    KubernetesClientException forbidden = new KubernetesClientException("forbidden", 403, null);
    doThrow(forbidden).when(client).resources(WorkloadPriorityClass.class);
    doThrow(forbidden).when(client).scheduling();
    return client;
  }

  private Workload getWorkload() {
    return kubernetesClient.resources(Workload.class).inNamespace("default").withName(NAME).get();
  }

  private void reserveQuota() {
    Workload workload = getWorkload();
    workload.setStatus(status("QuotaReserved", "True"));
    kubernetesClient.resource(workload).update();
  }

  private void admitWorkload() {
    Workload workload = getWorkload();
    workload.setStatus(
        WorkloadStatus.builder()
            .conditions(
                List.of(new ConditionBuilder().withType("Admitted").withStatus("True").build()))
            .build());
    kubernetesClient.resource(workload).update();
  }

  private static WorkloadStatus status(final String type, final String conditionStatus) {
    return WorkloadStatus.builder()
        .conditions(
            List.of(new ConditionBuilder().withType(type).withStatus(conditionStatus).build()))
        .build();
  }

  private static SparkApplication owner() {
    SparkApplication app = new SparkApplication();
    app.setMetadata(new ObjectMetaBuilder().withName("app-1").withNamespace("default").build());
    return app;
  }

  private static SparkApplication app(final Map<String, String> sparkConf) {
    SparkApplication app = owner();
    ApplicationSpec spec = new ApplicationSpec();
    spec.setSparkConf(sparkConf);
    app.setSpec(spec);
    return app;
  }

  private static String hashPodSets(final SparkApplication app) {
    return KueueWorkloadUtils.hashPodSets(KueueWorkloadFactory.buildWorkload(app));
  }

  private void createWorkloadPriorityClass(final String name, final int value) {
    WorkloadPriorityClass workloadPriorityClass = new WorkloadPriorityClass();
    workloadPriorityClass.setMetadata(new ObjectMetaBuilder().withName(name).build());
    workloadPriorityClass.setValue(value);
    kubernetesClient.resource(workloadPriorityClass).create();
  }

  private void createPriorityClass(
      final String name, final int value, final boolean globalDefault) {
    kubernetesClient.resource(priorityClass(name, value, globalDefault)).create();
  }

  private static PriorityClass priorityClass(
      final String name, final int value, final boolean globalDefault) {
    return new PriorityClassBuilder()
        .withNewMetadata()
        .withName(name)
        .endMetadata()
        .withValue(value)
        .withGlobalDefault(globalDefault)
        .build();
  }

  private static Workload workloadWithPriorityClass(final String workloadPriorityClass) {
    Workload workload = workload("owner-uid-1", 1);
    workload
        .getMetadata()
        .setLabels(Map.of(Constants.LABEL_WORKLOAD_PRIORITY_CLASS, workloadPriorityClass));
    return workload;
  }

  private static Workload workloadWithPriorityClasses(
      final String driverPriorityClass, final String executorPriorityClass) {
    Workload workload = workload("owner-uid-1", 1);
    workload
        .getSpec()
        .setPodSets(
            List.of(
                podSetWithPriorityClass("driver", driverPriorityClass),
                podSetWithPriorityClass("executor", executorPriorityClass)));
    return workload;
  }

  private static PodSet podSetWithPriorityClass(final String name, final String priorityClass) {
    return PodSet.builder()
        .name(name)
        .count(1)
        .template(
            new PodTemplateSpecBuilder()
                .withNewSpec()
                .withPriorityClassName(priorityClass)
                .endSpec()
                .build())
        .build();
  }

  private static Workload workloadWithNodeSelector(final Map<String, String> nodeSelector) {
    Workload workload = workload("owner-uid-1", 1);
    workload
        .getSpec()
        .getPodSets()
        .get(0)
        .setTemplate(
            new PodTemplateSpecBuilder()
                .withNewSpec()
                .withNodeSelector(nodeSelector)
                .endSpec()
                .build());
    return workload;
  }

  private static Workload workload(final String ownerUid, final int executors) {
    Workload workload = new Workload();
    workload.setMetadata(
        new ObjectMetaBuilder()
            .withName(NAME)
            .withNamespace("default")
            .withLabels(Map.of("spark.operator/spark-app-name", "app-1"))
            .withOwnerReferences(
                new OwnerReferenceBuilder()
                    .withName("app-1")
                    .withKind("SparkApplication")
                    .withUid(ownerUid)
                    .withController(true)
                    .build())
            .build());
    workload.setSpec(
        WorkloadSpec.builder()
            .queueName("test-queue")
            .active(true)
            .podSets(List.of(PodSet.builder().name("executor").count(executors).build()))
            .build());
    return workload;
  }
}
