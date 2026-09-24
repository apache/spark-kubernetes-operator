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

package org.apache.spark.k8s.operator.context;

import static org.apache.spark.k8s.operator.utils.Utils.driverLabels;
import static org.apache.spark.k8s.operator.utils.Utils.executorLabels;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.ListOptions;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodListBuilder;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkAppResourceSpec;
import org.apache.spark.k8s.operator.SparkAppSubmissionWorker;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.kueue.KueuePodSetFlavor;

class SparkAppContextTest {
  private final SparkApplication application = buildApplication();
  private final Pod driverPodSpec = driverPod("sparkapp1-1-driver");

  @Test
  void currentAttemptDriverPodIsFoundBehindPreviousAttemptPod() {
    Pod previousAttemptDriver = driverPod("sparkapp1-0-driver");
    SparkAppContext context =
        buildContext(List.of(previousAttemptDriver, driverPodSpec), driverPodSpec);

    Optional<Pod> driverPod = context.getCurrentAttemptDriverPod();

    Assertions.assertTrue(driverPod.isPresent());
    Assertions.assertEquals("sparkapp1-1-driver", driverPod.get().getMetadata().getName());
  }

  @Test
  void terminatingPreviousAttemptPodWithSameNameIsNotCurrentAttemptDriver() {
    // The driver pod name is reused across attempts, e.g. with a user-specified spark.app.id. The
    // previous attempt's pod has been deleted by the clean-up step and is still terminating.
    Pod terminatingPreviousAttemptDriver = terminating(driverPodSpec);
    SparkAppContext context =
        buildContext(List.of(terminatingPreviousAttemptDriver), terminatingPreviousAttemptDriver);

    Assertions.assertTrue(context.getDriverPod().isPresent());
    Assertions.assertTrue(context.getCurrentAttemptDriverPod().isEmpty());
    verify(context, never()).getDriverPodSpec();
  }

  @Test
  void stalePreDeletionSnapshotIsNotCurrentAttemptDriver() {
    // The informer still holds the pre-deletion snapshot of the previous attempt's pod (same name,
    // no deletionTimestamp) while the API server has already removed it.
    SparkAppContext context = buildContext(List.of(driverPodSpec), null);

    Assertions.assertTrue(context.getDriverPod().isPresent());
    Assertions.assertTrue(context.getCurrentAttemptDriverPod().isEmpty());
  }

  @Test
  void podTerminatingOnApiServerIsNotCurrentAttemptDriver() {
    // The informer has not observed the deletion yet, but the API server reports it terminating.
    SparkAppContext context = buildContext(List.of(driverPodSpec), terminating(driverPodSpec));

    Assertions.assertTrue(context.getCurrentAttemptDriverPod().isEmpty());
  }

  @Test
  void apiErrorDuringVerificationIsPropagated() {
    // A failed verification must not be reported as an absent driver, which would let a
    // suspended application release the Kueue quota of its running driver
    SparkAppContext context = buildContext(List.of(driverPodSpec), null);
    when(context.getClient().pods().inNamespace("default").withName(anyString()).get())
        .thenThrow(new KubernetesClientException("boom", 500, null));

    Assertions.assertThrows(
        KubernetesClientException.class, context::getCurrentAttemptDriverPod);
  }

  @Test
  void podWithDifferentNameIsNotCurrentAttemptDriver() {
    SparkAppContext context = buildContext(List.of(driverPod("sparkapp1-0-driver")), null);

    Assertions.assertTrue(context.getDriverPod().isPresent());
    Assertions.assertTrue(context.getCurrentAttemptDriverPod().isEmpty());
    verify(context.getClient(), never()).pods();
  }

  @Test
  void noDriverPodDoesNotBuildDriverSpec() {
    SparkAppContext context = buildContext(List.of(), null);

    Assertions.assertTrue(context.getCurrentAttemptDriverPod().isEmpty());
    verify(context, never()).getDriverPodSpec();
    verify(context.getClient(), never()).pods();
  }

  @Test
  void driverOrExecutorPodRemains() {
    SparkAppContext context = buildContext(List.of(), null);
    FilterWatchListDeletable<Pod, PodList, PodResource> rolePods = stubRolePodList(context);
    ArgumentCaptor<ListOptions> options = ArgumentCaptor.forClass(ListOptions.class);
    // A terminating pod is listed as well
    when(rolePods.list(options.capture()))
        .thenReturn(
            new PodListBuilder()
                .withNewMetadata()
                .endMetadata()
                .withItems(terminating(driverPodSpec))
                .build());

    Assertions.assertTrue(context.hasDriverOrExecutorPods());
    // Only whether any pod remains matters
    Assertions.assertEquals(1L, options.getValue().getLimit());
  }

  @Test
  void cachedDriverOrExecutorPodRemainsWithoutListing() {
    Pod executor =
        new PodBuilder()
            .withNewMetadata()
            .withName("sparkapp1-exec-1")
            .withNamespace("default")
            .withLabels(executorLabels(application))
            .endMetadata()
            .build();
    for (Pod cached : List.of(terminating(driverPodSpec), executor)) {
      SparkAppContext context = buildContext(List.of(cached), null);

      Assertions.assertTrue(context.hasDriverOrExecutorPods());
      verify(context.getClient(), never()).pods();
    }
  }

  @Test
  void noDriverOrExecutorPodRemains() {
    SparkAppContext context = buildContext(List.of(), null);
    when(stubRolePodList(context).list(any(ListOptions.class)))
        .thenReturn(new PodListBuilder().withNewMetadata().endMetadata().build());

    Assertions.assertFalse(context.hasDriverOrExecutorPods());
  }

  @Test
  void continueTokenMeansThatDriverOrExecutorPodsRemain() {
    SparkAppContext context = buildContext(List.of(), null);
    // An empty page with a continue token, as a limited LIST with a label selector may return
    when(stubRolePodList(context).list(any(ListOptions.class)))
        .thenReturn(
            new PodListBuilder().withNewMetadata().withContinue("next").endMetadata().build());

    Assertions.assertTrue(context.hasDriverOrExecutorPods());
  }

  @Test
  void kueuePodSetFlavorsRebuildTheCachedResourceSpec() {
    // The driver pod spec can be built before the Kueue admission, e.g. to find the driver pod of
    // the current attempt, so setting the flavors drops the cached one.
    KubernetesClient client = mock(KubernetesClient.class);
    Context<?> josdkContext = mock(Context.class);
    when(josdkContext.getClient()).thenReturn(client);
    SparkAppResourceSpec resourceSpec = mock(SparkAppResourceSpec.class);
    when(resourceSpec.getConfiguredPod()).thenReturn(driverPod("sparkapp1-1-driver"));
    SparkAppSubmissionWorker worker = mock(SparkAppSubmissionWorker.class);
    when(worker.getResourceSpec(any(), any(), any())).thenReturn(resourceSpec);
    SparkAppContext context = new SparkAppContext(application, josdkContext, worker);

    context.getDriverPodSpec();
    context.getDriverPodSpec();
    verify(worker, times(1)).getResourceSpec(any(), any(), any());

    Toleration toleration = new Toleration("NoSchedule", "spot", "Exists", null, null);
    context.setKueuePodSetFlavors(
        Map.of("driver", new KueuePodSetFlavor(Map.of("pool", "cpu"), List.of(toleration))));
    context.getDriverPodSpec();

    ArgumentCaptor<SparkApplication> captor = ArgumentCaptor.forClass(SparkApplication.class);
    verify(worker, times(2)).getResourceSpec(captor.capture(), any(), any());
    PodSpec driverTemplate =
        captor.getValue().getSpec().getDriverSpec().getPodTemplateSpec().getSpec();
    Assertions.assertEquals(Map.of("pool", "cpu"), driverTemplate.getNodeSelector());
    Assertions.assertEquals(List.of(toleration), driverTemplate.getTolerations());
  }

  /**
   * Builds a context whose informer cache holds the given pods and whose API server returns the
   * given live pod (or nothing) for any pod name.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private SparkAppContext buildContext(List<Pod> cachedPods, Pod livePod) {
    KubernetesClient client = mock(KubernetesClient.class);
    MixedOperation<Pod, PodList, PodResource> pods = mock(MixedOperation.class);
    NonNamespaceOperation<Pod, PodList, PodResource> namespacedPods =
        mock(NonNamespaceOperation.class);
    PodResource podResource = mock(PodResource.class);
    when(client.pods()).thenReturn(pods);
    when(pods.inNamespace("default")).thenReturn(namespacedPods);
    when(namespacedPods.withName(anyString())).thenReturn(podResource);
    when(podResource.get()).thenReturn(livePod);

    Context josdkContext = mock(Context.class);
    when(josdkContext.getSecondaryResourcesAsStream(Pod.class))
        .thenAnswer(invocation -> cachedPods.stream());
    when(josdkContext.getClient()).thenReturn(client);
    SparkAppContext context =
        spy(new SparkAppContext(application, josdkContext, mock(SparkAppSubmissionWorker.class)));
    doReturn(driverPodSpec).when(context).getDriverPodSpec();
    return context;
  }

  /** Stubs the listing of the driver and executor pods of the application by its labels. */
  @SuppressWarnings("unchecked")
  private static FilterWatchListDeletable<Pod, PodList, PodResource> stubRolePodList(
      SparkAppContext context) {
    FilterWatchListDeletable<Pod, PodList, PodResource> labeledPods =
        mock(FilterWatchListDeletable.class);
    FilterWatchListDeletable<Pod, PodList, PodResource> rolePods =
        mock(FilterWatchListDeletable.class);
    when(context
            .getClient()
            .pods()
            .inNamespace("default")
            .withLabel(Constants.LABEL_SPARK_APPLICATION_NAME, "sparkapp1"))
        .thenReturn(labeledPods);
    when(labeledPods.withLabelIn(
            Constants.LABEL_SPARK_ROLE_NAME,
            Constants.LABEL_SPARK_ROLE_DRIVER_VALUE,
            Constants.LABEL_SPARK_ROLE_EXECUTOR_VALUE))
        .thenReturn(rolePods);
    return rolePods;
  }

  private SparkApplication buildApplication() {
    SparkApplication app = new SparkApplication();
    app.setMetadata(new ObjectMetaBuilder().withName("sparkapp1").withNamespace("default").build());
    return app;
  }

  private Pod driverPod(String name) {
    return new PodBuilder()
        .withNewMetadata()
        .withName(name)
        .withNamespace("default")
        .withLabels(driverLabels(application))
        .endMetadata()
        .build();
  }

  private static Pod terminating(Pod pod) {
    return new PodBuilder(pod)
        .editOrNewMetadata()
        .withDeletionTimestamp(Instant.now().toString())
        .endMetadata()
        .build();
  }
}
