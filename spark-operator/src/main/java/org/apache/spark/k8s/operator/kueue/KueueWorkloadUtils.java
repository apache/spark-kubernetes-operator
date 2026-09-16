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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.kueue.v1beta2.Workload;
import org.apache.spark.k8s.operator.kueue.v1beta2.WorkloadStatus;
import org.apache.spark.k8s.operator.utils.ModelUtils;
import org.apache.spark.k8s.operator.utils.ReconcilerUtils;

/** Utilities to create, check and release Kueue Workloads. */
@Slf4j
public final class KueueWorkloadUtils {

  /** Annotation holding the hash of the pod sets which the Workload was created with. */
  public static final String ANNOTATION_POD_SETS_HASH = "spark.operator/kueue-pod-sets-hash";

  /**
   * Requeue interval after {@link AdmissionResult#STALE}. It is short because the stale Workload
   * goes away shortly, while an unchanged admission is watched with the default interval.
   */
  public static final Duration STALE_WORKLOAD_REQUEUE_INTERVAL = Duration.ofSeconds(5);

  private KueueWorkloadUtils() {}

  /** Outcome of {@link #requestAdmission(KubernetesClient, Workload)}. */
  public enum AdmissionResult {
    /** Kueue admitted the Workload, so the requested resources can be created. */
    ADMITTED,
    /** The Workload waits for quota, so the resource creation is held. */
    PENDING,
    /**
     * The existing Workload cannot be used because it is owned by another resource, requests
     * outdated pod sets, or is being deleted. It is deleted so that a later reconciliation creates
     * the Workload of the current spec.
     */
    STALE
  }

  /**
   * Creates the given Workload if it does not exist yet and reports whether Kueue admitted it.
   *
   * @param client The KubernetesClient.
   * @param desired The Workload built for the resource to be admitted.
   * @return The AdmissionResult for the Workload.
   */
  public static AdmissionResult requestAdmission(
      final KubernetesClient client, final Workload desired) {
    String podSetsHash = hashPodSets(desired);
    Map<String, String> desiredAnnotations = new HashMap<>();
    if (desired.getMetadata().getAnnotations() != null) {
      desiredAnnotations.putAll(desired.getMetadata().getAnnotations());
    }
    desiredAnnotations.put(ANNOTATION_POD_SETS_HASH, podSetsHash);
    desired.getMetadata().setAnnotations(desiredAnnotations);
    Optional<Workload> created = ReconcilerUtils.getOrCreateSecondaryResource(client, desired);
    if (created.isEmpty()) {
      throw new IllegalStateException(
          "Failed to request Kueue Workload with name: " + desired.getMetadata().getName());
    }
    Workload workload = created.get();
    if (workload.getMetadata().getDeletionTimestamp() != null) {
      log.debug(
          "Waiting for the Kueue Workload {} to be deleted.", workload.getMetadata().getName());
      return AdmissionResult.STALE;
    }
    if (!Objects.equals(controllerUid(workload), controllerUid(desired))) {
      // A Workload of a deleted resource that had the same name is not garbage collected yet.
      log.info(
          "Deleting the Kueue Workload {} which is owned by another resource.",
          workload.getMetadata().getName());
      deleteWorkload(client, workload);
      return AdmissionResult.STALE;
    }
    WorkloadStatus status = workload.getStatus();
    if (status != null && status.isAdmitted()) {
      return AdmissionResult.ADMITTED;
    }
    Map<String, String> annotations = workload.getMetadata().getAnnotations();
    if (annotations == null || !podSetsHash.equals(annotations.get(ANNOTATION_POD_SETS_HASH))) {
      // The spec changed while waiting, so the queued request no longer matches the resources.
      log.info(
          "Deleting the pending Kueue Workload {} whose pod sets are outdated.",
          workload.getMetadata().getName());
      deleteWorkload(client, workload);
      return AdmissionResult.STALE;
    }
    return AdmissionResult.PENDING;
  }

  /**
   * Deletes the Workload of the given resource so that Kueue releases its quota. Failures are
   * logged only, because the Workload is garbage collected with its owner anyway.
   *
   * @param client The KubernetesClient.
   * @param owner The SparkApplication or SparkCluster owning the Workload.
   */
  public static void releaseWorkload(final KubernetesClient client, final HasMetadata owner) {
    String name = KueueWorkloadFactory.getWorkloadName(owner);
    try {
      client
          .resources(Workload.class)
          .inNamespace(owner.getMetadata().getNamespace())
          .withName(name)
          .delete();
    } catch (KubernetesClientException e) {
      log.warn("Failed to release the Kueue Workload {}.", name, e);
    }
  }

  private static void deleteWorkload(final KubernetesClient client, final Workload workload) {
    // Do not wait for the deletion, which Kueue may delay with its finalizer.
    client.resource(workload).delete();
  }

  /**
   * Hashes the pod sets of the desired Workload with sorted map keys. The hash is stored as an
   * annotation at creation, so the comparison does not depend on the defaulting applied to the
   * stored pod sets.
   */
  static String hashPodSets(final Workload workload) {
    try {
      String json =
          ModelUtils.objectMapper
              .writer()
              .with(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
              .writeValueAsString(workload.getSpec().getPodSets());
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(json.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest);
    } catch (JsonProcessingException | NoSuchAlgorithmException e) {
      throw new IllegalStateException("Failed to hash the pod sets of the Kueue Workload.", e);
    }
  }

  private static String controllerUid(final Workload workload) {
    List<OwnerReference> ownerReferences = workload.getMetadata().getOwnerReferences();
    if (ownerReferences == null) {
      return null;
    }
    return ownerReferences.stream()
        .filter(reference -> Boolean.TRUE.equals(reference.getController()))
        .map(OwnerReference::getUid)
        .findFirst()
        .orElse(null);
  }
}
