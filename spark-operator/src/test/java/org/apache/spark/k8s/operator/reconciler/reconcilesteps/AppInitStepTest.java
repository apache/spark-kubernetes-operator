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
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.FieldsV1;
import io.fabric8.kubernetes.api.model.ManagedFieldsEntry;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable;
import io.fabric8.kubernetes.client.dsl.NamespaceableResource;
import io.fabric8.kubernetes.client.dsl.ServerSideApplicable;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;

@EnableKubernetesMockClient(crud = true)
@SuppressFBWarnings(
    value = {"UWF_UNWRITTEN_FIELD", "NP_UNWRITTEN_FIELD", "UUF_UNUSED_FIELD"},
    justification = "Unwritten fields are covered by Kubernetes mock client")
class AppInitStepTest {
  private KubernetesMockServer mockServer;
  private KubernetesClient kubernetesClient;

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
}
