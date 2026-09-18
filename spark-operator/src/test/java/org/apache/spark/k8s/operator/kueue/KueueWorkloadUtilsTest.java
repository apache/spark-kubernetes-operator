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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ConditionBuilder;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NamespaceableResource;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadUtils.AdmissionResult;
import org.apache.spark.k8s.operator.kueue.v1beta2.Admission;
import org.apache.spark.k8s.operator.kueue.v1beta2.PodSet;
import org.apache.spark.k8s.operator.kueue.v1beta2.PodSetAssignment;
import org.apache.spark.k8s.operator.kueue.v1beta2.ResourceFlavor;
import org.apache.spark.k8s.operator.kueue.v1beta2.ResourceFlavorSpec;
import org.apache.spark.k8s.operator.kueue.v1beta2.Workload;
import org.apache.spark.k8s.operator.kueue.v1beta2.WorkloadSpec;
import org.apache.spark.k8s.operator.kueue.v1beta2.WorkloadStatus;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;

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
        AdmissionResult.QUEUED, KueueWorkloadUtils.requestAdmission(kubernetesClient, desired));
    Workload created = getWorkload();
    Assertions.assertNotNull(created);
    Assertions.assertEquals("test-queue", created.getSpec().getQueueName());
    Assertions.assertEquals(
        KueueWorkloadUtils.hashPodSets(desired),
        created.getMetadata().getAnnotations().get(KueueWorkloadUtils.ANNOTATION_POD_SETS_HASH));

    // A second reconciliation reuses the same Workload and still waits for the admission
    Assertions.assertEquals(
        AdmissionResult.PENDING,
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1)));
    Assertions.assertEquals(
        created.getMetadata().getUid(), getWorkload().getMetadata().getUid());
  }

  @Test
  void admittedWorkloadIsReported() {
    KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1));
    admitWorkload();

    Assertions.assertEquals(
        AdmissionResult.ADMITTED,
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1)));
  }

  @Test
  void admittedWorkloadIsKeptEvenIfPodSetsChanged() {
    KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1));
    admitWorkload();

    Assertions.assertEquals(
        AdmissionResult.ADMITTED,
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 5)));
    Assertions.assertNotNull(getWorkload());
  }

  @Test
  void pendingWorkloadWithOutdatedPodSetsIsRecreated() {
    KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1));

    // The spec changed while waiting for the admission
    Assertions.assertEquals(
        AdmissionResult.STALE,
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 5)));
    Assertions.assertNull(getWorkload());

    Assertions.assertEquals(
        AdmissionResult.QUEUED,
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 5)));
    Assertions.assertEquals(5, getWorkload().getSpec().getPodSets().get(0).getCount());
  }

  @Test
  void workloadOwnedByAnotherResourceIsDeleted() {
    KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("stale-owner-uid", 1));
    admitWorkload();

    Assertions.assertEquals(
        AdmissionResult.STALE,
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1)));
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
        KueueWorkloadUtils.requestAdmission(kubernetesClient, workload("owner-uid-1", 1)));
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
  @SuppressWarnings("unchecked")
  void workloadReadFailureIsNotReportedAsQueued() {
    // A failed read must not be taken for a missing Workload, which would report QUEUED again
    KubernetesClient client = mock(KubernetesClient.class);
    NamespaceableResource<Workload> resource = mock(NamespaceableResource.class);
    when(client.resource(any(Workload.class))).thenReturn(resource);
    when(resource.get()).thenThrow(new KubernetesClientException("unavailable", 503, null));

    Assertions.assertThrows(
        KubernetesClientException.class,
        () -> KueueWorkloadUtils.requestAdmission(client, workload("owner-uid-1", 1)));
    verify(resource, never()).create();
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
            admittedWorkload(Map.of("driver", driverFlavors, "executor", executorFlavors)),
            workload("owner-uid-1", 1));

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
        Map.of(),
        KueueWorkloadUtils.resolvePodSetFlavors(
            kubernetesClient, admitted, workload("owner-uid-1", 1)));
    Assertions.assertEquals(
        Map.of(),
        KueueWorkloadUtils.resolvePodSetFlavors(
            kubernetesClient, admittedWorkload(Map.of()), workload("owner-uid-1", 1)));
  }

  @Test
  void resolvePodSetFlavorsAllowsTheSameNodeSelector() {
    createFlavor("cpu-flavor", Map.of("pool", "cpu"), List.of());

    Map<String, KueuePodSetFlavor> flavors =
        KueueWorkloadUtils.resolvePodSetFlavors(
            kubernetesClient,
            admittedWorkload(Map.of("executor", Map.of("cpu", "cpu-flavor"))),
            workloadWithNodeSelector(Map.of("pool", "cpu", "zone", "a")));

    Assertions.assertEquals(Map.of("pool", "cpu"), flavors.get("executor").nodeSelector());
  }

  @Test
  void resolvePodSetFlavorsFailsOnNodeSelectorConflict() {
    createFlavor("cpu-flavor", Map.of("pool", "cpu"), List.of());

    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                KueueWorkloadUtils.resolvePodSetFlavors(
                    kubernetesClient,
                    admittedWorkload(Map.of("executor", Map.of("cpu", "cpu-flavor"))),
                    workloadWithNodeSelector(Map.of("pool", "gpu"))));
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
                    admittedWorkload(Map.of("executor", Map.of("cpu", "missing-flavor"))),
                    workload("owner-uid-1", 1)));
    Assertions.assertEquals(404, e.getCode());
  }

  private void createFlavor(
      final String name, final Map<String, String> nodeLabels, final List<Toleration> tolerations) {
    ResourceFlavor flavor = new ResourceFlavor();
    flavor.setMetadata(new ObjectMetaBuilder().withName(name).build());
    flavor.setSpec(
        ResourceFlavorSpec.builder().nodeLabels(nodeLabels).tolerations(tolerations).build());
    kubernetesClient.resource(flavor).create();
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

  private Workload getWorkload() {
    return kubernetesClient.resources(Workload.class).inNamespace("default").withName(NAME).get();
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
