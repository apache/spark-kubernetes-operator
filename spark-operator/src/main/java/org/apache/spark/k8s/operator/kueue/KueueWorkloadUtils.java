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

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.context.BaseContext;
import org.apache.spark.k8s.operator.kueue.v1beta2.PodSet;
import org.apache.spark.k8s.operator.kueue.v1beta2.PodSetAssignment;
import org.apache.spark.k8s.operator.kueue.v1beta2.PriorityClassRef;
import org.apache.spark.k8s.operator.kueue.v1beta2.ResourceFlavor;
import org.apache.spark.k8s.operator.kueue.v1beta2.Workload;
import org.apache.spark.k8s.operator.kueue.v1beta2.WorkloadSpec;
import org.apache.spark.k8s.operator.kueue.v1beta2.WorkloadStatus;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.utils.EventUtils;
import org.apache.spark.k8s.operator.utils.ModelUtils;
import org.apache.spark.k8s.operator.utils.ReconcilerUtils;

/** Utilities to create, check and release Kueue Workloads. */
@Slf4j
public final class KueueWorkloadUtils {

  /** Annotation holding the hash of the pod sets which the Workload was created with. */
  public static final String ANNOTATION_POD_SETS_HASH = "spark.operator/kueue-pod-sets-hash";

  /**
   * Requeue interval after {@link AdmissionResult#STALE} and after a transient API failure. It is
   * short because both go away shortly, while an unchanged admission and a persistent failure are
   * retried with the default interval.
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
   * @param desired The Workload built for the resource to be admitted. The priority and the pod
   *     sets hash annotation are added to it in place.
   * @return The AdmissionResult for the Workload.
   * @throws IllegalStateException if the Workload can neither be read nor created, or if its
   *     priority class does not exist.
   * @throws KubernetesClientException if the Workload cannot be created, or a stale Workload cannot
   *     be deleted. Unlike {@link #releaseWorkload}, this is not swallowed so that the resource is
   *     not created until the stale Workload is gone.
   */
  public static AdmissionResult requestAdmission(
      final KubernetesClient client, final Workload desired) {
    KueueWorkloadPriority.setPriority(client, desired);
    String podSetsHash = hashPodSets(desired);
    Map<String, String> desiredAnnotations = new HashMap<>();
    if (desired.getMetadata().getAnnotations() != null) {
      desiredAnnotations.putAll(desired.getMetadata().getAnnotations());
    }
    desiredAnnotations.put(ANNOTATION_POD_SETS_HASH, podSetsHash);
    desired.getMetadata().setAnnotations(desiredAnnotations);
    Optional<Workload> current = ReconcilerUtils.getOrCreateSecondaryResource(client, desired);
    if (current.isEmpty()) {
      throw new IllegalStateException(
          "Failed to request Kueue Workload with name: " + desired.getMetadata().getName());
    }
    Workload workload = current.get();
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
    PriorityClassRef desiredPriorityClassRef = desired.getSpec().getPriorityClassRef();
    if (desired.getSpec().getPriority() != null
        && !Objects.equals(spec.getPriorityClassRef(), desiredPriorityClassRef)
        && KueueWorkloadPriority.isPriorityClassChangeAllowed(
            workload, desiredPriorityClassRef)) {
      // Like Kueue, a changed priority class is applied in place so that the Workload keeps its
      // position in the queue.
      log.info(
          "Updating the priority class of the pending Kueue Workload {}.",
          workload.getMetadata().getName());
      spec.setPriorityClassRef(desiredPriorityClassRef);
      spec.setPriority(desired.getSpec().getPriority());
      client.resource(workload).update();
    }
    return AdmissionResult.PENDING;
  }

  /**
   * Requests the Kueue admission of the resource of the given context and reports the progress to
   * return until it is granted. The pending event is published on every reconcile while the
   * Workload waits, since it is the only signal a queued first attempt has. A stale Workload is
   * replaced by the operator itself shortly, so it publishes nothing until the new Workload is
   * queued. An API failure of the request is retried rather than failing the resource: a transient
   * one shortly, a persistent one with the default interval, so that its event is not rewritten
   * every few seconds until a user fixes the cause.
   *
   * @param context The context of the resource to be admitted.
   * @param desired The Workload built for the resource. The pod sets hash annotation is added to it
   *     in place.
   * @param requested The resources held until the admission, as named in the events and logs, e.g.
   *     {@code "driver"}.
   * @return The progress to return while the admission is not granted, or empty to proceed.
   */
  public static Optional<ReconcileProgress> holdForAdmission(
      final BaseContext<?> context, final Workload desired, final String requested) {
    AdmissionResult admission;
    try {
      admission = requestAdmission(context.getClient(), desired);
    } catch (KubernetesClientException e) {
      log.warn("Failed to request Kueue admission, will retry.", e);
      // Like a status update failure, a transport level failure is not published, since writing
      // an event would only add load to an API server that is often the cause of the failure.
      // It goes away on its own, so it keeps the short interval.
      if (ReconcilerUtils.isTransientError(e)) {
        return Optional.of(
            ReconcileProgress.completeAndRequeueAfter(STALE_WORKLOAD_REQUEUE_INTERVAL));
      }
      // A persistent failure, such as a missing Kueue or the RBAC rules for it, is retried with
      // the default interval, so that its event is not rewritten every few seconds until a user
      // fixes the cause.
      EventUtils.warn(
          context.getEventRecorder(),
          EventUtils.REASON_KUEUE_ADMISSION_REQUEST_FAILED,
          "Failed to request Kueue admission, will retry. " + EventUtils.describe(e));
      return Optional.of(ReconcileProgress.completeAndDefaultRequeue());
    } catch (IllegalStateException e) {
      log.warn("Failed to request Kueue admission, will retry.", e);
      // A malformed Workload is never transient, so it is reported like the persistent API failure.
      EventUtils.warn(
          context.getEventRecorder(),
          EventUtils.REASON_KUEUE_ADMISSION_REQUEST_FAILED,
          "Failed to request Kueue admission, will retry. " + EventUtils.describe(e));
      return Optional.of(ReconcileProgress.completeAndDefaultRequeue());
    }
    if (admission == AdmissionResult.STALE) {
      return Optional.of(
          ReconcileProgress.completeAndRequeueAfter(STALE_WORKLOAD_REQUEUE_INTERVAL));
    }
    String workloadName = desired.getMetadata().getName();
    if (admission == AdmissionResult.PENDING) {
      // Republished while the Workload waits, rather than once when it is created. The event sink
      // keys the Event on the reason, so a repeat bumps the count of the one Event instead of
      // creating another, and it restores an Event that the API server has already dropped after
      // its retention: a queued first attempt has no persisted status to fall back on.
      EventUtils.normal(
          context.getEventRecorder(),
          EventUtils.REASON_KUEUE_ADMISSION_PENDING,
          "Waiting for Kueue to admit Workload "
              + workloadName
              + " in queue "
              + desired.getSpec().getQueueName()
              + ", "
              + requested
              + " would be requested after the admission.");
      log.debug(
          "Kueue has not admitted the Workload {}, {} would not be requested.",
          workloadName,
          requested);
      return Optional.of(ReconcileProgress.completeAndDefaultRequeue());
    }
    EventUtils.normal(
        context.getEventRecorder(),
        EventUtils.REASON_KUEUE_ADMITTED,
        "Kueue admitted Workload " + workloadName + ", requesting " + requested + ".");
    return Optional.empty();
  }

  /**
   * Resolves the node selector and tolerations of the ResourceFlavors which Kueue assigned to each
   * pod set of the admitted Workload, like Kueue's `podset.FromAssignment`. The Topology Aware
   * Scheduling gate and annotation are not handled. The flavors of a pod set are applied in the
   * order of the resource names, so that a later flavor overwrites a node label deterministically.
   *
   * @param client The KubernetesClient.
   * @param admitted The admitted Workload.
   * @param desired The Workload built for the resource, whose pod set templates have the node
   *     selectors of the pods.
   * @return The KueuePodSetFlavor by the pod set name. A pod set without an assignment is absent.
   * @throws KubernetesClientException if a ResourceFlavor cannot be read.
   * @throws IllegalArgumentException if a node label of the flavors conflicts with the node
   *     selector of the pod set. Like Kueue built-in integrations, this is permanent.
   */
  public static Map<String, KueuePodSetFlavor> resolvePodSetFlavors(
      final KubernetesClient client, final Workload admitted, final Workload desired) {
    Map<String, KueuePodSetFlavor> result = new HashMap<>();
    WorkloadStatus status = admitted.getStatus();
    if (status == null || status.getAdmission() == null) {
      return result;
    }
    Map<String, ResourceFlavor> flavorCache = new HashMap<>();
    for (PodSetAssignment assignment : status.getAdmission().getPodSetAssignments()) {
      Map<String, String> nodeSelector = new HashMap<>();
      List<Toleration> tolerations = new ArrayList<>();
      if (assignment.getFlavors() != null) {
        // Like Kueue, a flavor assigned to several resources is applied once.
        Set<String> flavorNames =
            new LinkedHashSet<>(new TreeMap<>(assignment.getFlavors()).values());
        for (String flavorName : flavorNames) {
          ResourceFlavor flavor =
              flavorCache.computeIfAbsent(flavorName, name -> getResourceFlavor(client, name));
          if (flavor.getSpec().getNodeLabels() != null) {
            nodeSelector.putAll(flavor.getSpec().getNodeLabels());
          }
          if (flavor.getSpec().getTolerations() != null) {
            KueuePodSetFlavor.addTolerations(tolerations, flavor.getSpec().getTolerations());
          }
        }
      }
      checkNoNodeSelectorConflict(
          assignment.getName(), podSetNodeSelector(desired, assignment.getName()), nodeSelector);
      result.put(assignment.getName(), new KueuePodSetFlavor(nodeSelector, tolerations));
    }
    return result;
  }

  private static ResourceFlavor getResourceFlavor(
      final KubernetesClient client, final String name) {
    ResourceFlavor flavor = client.resources(ResourceFlavor.class).withName(name).get();
    if (flavor == null) {
      throw new KubernetesClientException(
          "Kueue ResourceFlavor " + name + " is not found.", HTTP_NOT_FOUND, null);
    }
    return flavor;
  }

  private static Map<String, String> podSetNodeSelector(
      final Workload workload, final String podSetName) {
    return workload.getSpec().getPodSets().stream()
        .filter(podSet -> podSetName.equals(podSet.getName()))
        .map(PodSet::getTemplate)
        .filter(template -> template != null && template.getSpec() != null)
        .map(template -> template.getSpec().getNodeSelector())
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(Map.of());
  }

  /** Like Kueue's podset.Merge, a node label must not change the node selector of the pods. */
  private static void checkNoNodeSelectorConflict(
      final String podSetName,
      final Map<String, String> podNodeSelector,
      final Map<String, String> flavorNodeLabels) {
    for (Map.Entry<String, String> e : podNodeSelector.entrySet()) {
      String flavorValue = flavorNodeLabels.get(e.getKey());
      if (flavorValue != null && !flavorValue.equals(e.getValue())) {
        throw new IllegalArgumentException(
            "The node labels of the Kueue ResourceFlavors conflict with the node selector of the "
                + podSetName
                + " pods for key="
                + e.getKey()
                + ", value1="
                + e.getValue()
                + ", value2="
                + flavorValue);
      }
    }
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
