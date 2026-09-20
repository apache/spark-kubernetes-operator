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

import static java.net.HttpURLConnection.HTTP_FORBIDDEN;

import java.util.Comparator;
import java.util.Map;

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
import org.apache.spark.k8s.operator.utils.StringUtils;

/** Utilities to resolve the priority of a Kueue Workload. */
@Slf4j
public final class KueueWorkloadPriority {

  /** Like Kueue, the priority of a Workload without any priority class. */
  private static final int DEFAULT_PRIORITY = 0;

  /** The API group of the Kubernetes PriorityClass, which a pod template refers to. */
  private static final String SCHEDULING_API_GROUP = "scheduling.k8s.io";

  private KueueWorkloadPriority() {}

  /**
   * Checks whether the priority class of the given Workload is followed and whether Kueue accepts
   * the change. Like Kueue's `classifyWorkloadsForPriorityUpdate`, only a Workload without a
   * priority class or with a WorkloadPriorityClass follows the owner, so the priority of a Workload
   * backed by a Kubernetes PriorityClass is never rewritten, e.g. when the global default changes.
   * Once the quota is reserved, the Workload CEL rules freeze the presence, the group and the kind
   * of the priorityClassRef. The name of a WorkloadPriorityClass stays mutable, which is what Kueue
   * relies on to raise the priority of a Workload waiting for its admission checks.
   */
  static boolean isPriorityClassChangeAllowed(
      final Workload workload, final PriorityClassRef desired) {
    PriorityClassRef current = workload.getSpec().getPriorityClassRef();
    if (current != null && !Constants.KUEUE_API_GROUP.equals(current.getGroup())) {
      return false;
    }
    if (workload.getStatus() == null || !workload.getStatus().isQuotaReserved()) {
      return true;
    }
    return current != null
        && desired != null
        && Constants.KUEUE_API_GROUP.equals(desired.getGroup());
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
                SCHEDULING_API_GROUP, "PriorityClass", priorityClass.getMetadata().getName()));
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
}
