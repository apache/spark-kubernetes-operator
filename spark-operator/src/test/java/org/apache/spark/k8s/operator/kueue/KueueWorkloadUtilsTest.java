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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ConditionBuilder;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadUtils.AdmissionResult;
import org.apache.spark.k8s.operator.kueue.v1beta2.PodSet;
import org.apache.spark.k8s.operator.kueue.v1beta2.Workload;
import org.apache.spark.k8s.operator.kueue.v1beta2.WorkloadSpec;
import org.apache.spark.k8s.operator.kueue.v1beta2.WorkloadStatus;

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
        AdmissionResult.PENDING, KueueWorkloadUtils.requestAdmission(kubernetesClient, desired));
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
        AdmissionResult.PENDING,
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

  private static SparkApplication owner() {
    SparkApplication app = new SparkApplication();
    app.setMetadata(new ObjectMetaBuilder().withName("app-1").withNamespace("default").build());
    return app;
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
