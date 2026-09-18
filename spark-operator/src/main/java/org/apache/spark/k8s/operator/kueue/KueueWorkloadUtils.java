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
import java.util.Comparator;
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
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.scheduling.v1.PriorityClass;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.kueue.v1beta2.PodSet;
import org.apache.spark.k8s.operator.kueue.v1beta2.PriorityClassRef;
import org.apache.spark.k8s.operator.kueue.v1beta2.Workload;
import org.apache.spark.k8s.operator.kueue.v1beta2.WorkloadPriorityClass;
import org.apache.spark.k8s.operator.kueue.v1beta2.WorkloadSpec;
import org.apache.spark.k8s.operator.kueue.v1beta2.WorkloadStatus;
import org.apache.spark.k8s.operator.utils.ModelUtils;
import org.apache.spark.k8s.operator.utils.ReconcilerUtils;
import org.apache.spark.k8s.operator.utils.StringUtils;

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

  /** Like Kueue, the priority of a Workload without any priority class. */
  private static final int DEFAULT_PRIORITY = 0;

  private static final int HTTP_FORBIDDEN = 403;

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
   * @param desired The Workload built for the resource to be admitted. The priority and the pod
   *     sets hash annotation are added to it in place.
   * @return The AdmissionResult for the Workload.
   * @throws IllegalStateException if the Workload can neither be read nor created, or if its
   *     priority class does not exist.
   * @throws KubernetesClientException if a stale Workload cannot be deleted. Unlike {@link
   *     #releaseWorkload}, this is not swallowed so that the resource is not created until the
   *     stale Workload is gone.
   */
  public static AdmissionResult requestAdmission(
      final KubernetesClient client, final Workload desired) {
    setPriority(client, desired);
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
    if (isAdmitted(workload)) {
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
    WorkloadSpec spec = workload.getSpec();
    if (desired.getSpec().getPriority() != null
        && !Objects.equals(spec.getPriorityClassRef(), desired.getSpec().getPriorityClassRef())
        && (workload.getStatus() == null || !workload.getStatus().isQuotaReserved())) {
      // Like Kueue, a changed priority class is applied in place so that the Workload keeps its
      // position in the queue. Kueue does not allow the change once the quota is reserved.
      log.info(
          "Updating the priority class of the pending Kueue Workload {}.",
          workload.getMetadata().getName());
      spec.setPriorityClassRef(desired.getSpec().getPriorityClassRef());
      spec.setPriority(desired.getSpec().getPriority());
      client.resource(workload).update();
    }
    return AdmissionResult.PENDING;
  }

  /**
   * Sets the priority of the desired Workload in the same way as Kueue built-in integrations. The
   * WorkloadPriorityClass of the `kueue.x-k8s.io/priority-class` label takes precedence over the
   * PriorityClass of the first pod set which has one, and the global default PriorityClass is used
   * without both. Without any of them, the priority is 0 without a priority class. The priority is
   * left unset if the operator is not allowed to read the cluster-scoped priority classes.
   *
   * @param client The KubernetesClient.
   * @param workload The Workload whose priority is set in place.
   * @throws IllegalStateException if the priority class does not exist.
   */
  static void setPriority(final KubernetesClient client, final Workload workload) {
    WorkloadSpec spec = workload.getSpec();
    try {
      Map<String, String> labels = workload.getMetadata().getLabels();
      String workloadPriorityClassName =
          labels == null ? null : labels.get(Constants.LABEL_WORKLOAD_PRIORITY_CLASS);
      if (StringUtils.isNotEmpty(workloadPriorityClassName)) {
        WorkloadPriorityClass workloadPriorityClass =
            client.resources(WorkloadPriorityClass.class).withName(workloadPriorityClassName).get();
        if (workloadPriorityClass == null) {
          throw new IllegalStateException(
              "Kueue WorkloadPriorityClass " + workloadPriorityClassName + " is not found.");
        }
        spec.setPriorityClassRef(
            new PriorityClassRef(
                Constants.KUEUE_API_GROUP, "WorkloadPriorityClass", workloadPriorityClassName));
        spec.setPriority(workloadPriorityClass.getValue());
        return;
      }
      PriorityClass priorityClass = getPodPriorityClass(client, spec);
      if (priorityClass == null) {
        spec.setPriorityClassRef(null);
        spec.setPriority(DEFAULT_PRIORITY);
      } else {
        spec.setPriorityClassRef(
            new PriorityClassRef(
                "scheduling.k8s.io", "PriorityClass", priorityClass.getMetadata().getName()));
        spec.setPriority(priorityClass.getValue());
      }
    } catch (KubernetesClientException e) {
      if (e.getCode() != HTTP_FORBIDDEN) {
        throw e;
      }
      log.warn(
          "Requesting the Kueue Workload {} without priority because the operator is not allowed "
              + "to read the priority classes.",
          workload.getMetadata().getName());
    }
  }

  /**
   * Returns the PriorityClass of the first pod set which has one, or the global default
   * PriorityClass. Like Kueue, the lowest one wins if there are more than one global default.
   */
  private static PriorityClass getPodPriorityClass(
      final KubernetesClient client, final WorkloadSpec spec) {
    for (PodSet podSet : spec.getPodSets()) {
      PodTemplateSpec template = podSet.getTemplate();
      String name =
          template == null || template.getSpec() == null
              ? null
              : template.getSpec().getPriorityClassName();
      if (StringUtils.isNotEmpty(name)) {
        PriorityClass priorityClass =
            client.scheduling().v1().priorityClasses().withName(name).get();
        if (priorityClass == null) {
          throw new IllegalStateException("PriorityClass " + name + " is not found.");
        }
        return priorityClass;
      }
    }
    return client.scheduling().v1().priorityClasses().list().getItems().stream()
        .filter(priorityClass -> Boolean.TRUE.equals(priorityClass.getGlobalDefault()))
        .min(Comparator.comparing(PriorityClass::getValue))
        .orElse(null);
  }

  /**
   * Checks whether Kueue admitted the given Workload.
   *
   * @param workload The Workload to check.
   * @return True if the Workload has the `Admitted` condition with status `True`.
   */
  public static boolean isAdmitted(final Workload workload) {
    WorkloadStatus status = workload.getStatus();
    return status != null && status.isAdmitted();
  }

  /**
   * Checks whether an update of a Workload changes its admission. The Workload informer passes
   * only such updates, because Kueue updates the status of a pending Workload repeatedly, and the
   * reconciliations for them would use up the per-resource rate limit before the driver or the
   * master is observed.
   *
   * @param newWorkload The Workload after the update.
   * @param oldWorkload The Workload before the update.
   * @return True if exactly one of them is admitted.
   */
  public static boolean isAdmissionChanged(final Workload newWorkload, final Workload oldWorkload) {
    return isAdmitted(newWorkload) != isAdmitted(oldWorkload);
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
