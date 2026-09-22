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

import java.util.Optional;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.autoscaling.v2.HorizontalPodAutoscaler;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.api.model.policy.v1.PodDisruptionBudget;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.SparkClusterResourceSpec;
import org.apache.spark.k8s.operator.SparkClusterSubmissionWorker;
import org.apache.spark.k8s.operator.kueue.KueuePodSetFlavor;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadFactory;
import org.apache.spark.k8s.operator.reconciler.SparkClusterResourceSpecFactory;

/**
 * Context for {@link SparkCluster} resource, including secondary resource(s) and desired secondary
 * resource spec
 */
public class SparkClusterContext extends BaseContext<SparkCluster> {
  private final SparkCluster sparkCluster;
  private final SparkClusterSubmissionWorker submissionWorker;

  /** secondaryResourceSpec is initialized in a lazy fashion - built upon the first attempt */
  private SparkClusterResourceSpec secondaryResourceSpec;

  /**
   * Constructs a context for the given SparkCluster.
   *
   * @param sparkCluster The SparkCluster being reconciled.
   * @param josdkContext The JOSDK context of the current reconciliation.
   * @param submissionWorker The worker that builds the secondary resource spec.
   */
  public SparkClusterContext(
      SparkCluster sparkCluster,
      Context<?> josdkContext,
      SparkClusterSubmissionWorker submissionWorker) {
    super(josdkContext);
    this.sparkCluster = sparkCluster;
    this.submissionWorker = submissionWorker;
  }

  private SparkClusterResourceSpec getSecondaryResourceSpec() {
    synchronized (this) {
      if (secondaryResourceSpec == null) {
        secondaryResourceSpec =
            SparkClusterResourceSpecFactory.buildResourceSpec(sparkCluster, submissionWorker);
        applyKueuePodSetFlavors();
      }
      return secondaryResourceSpec;
    }
  }

  /**
   * Adds the flavors to the StatefulSets of the resource spec, which is not built again for them:
   * unlike the driver and the executors, the master and worker pods are created from the pod
   * templates of these StatefulSets. Applying the same flavors twice is a no-op, so the spec built
   * with them is left as it is.
   */
  @Override
  protected void applyKueuePodSetFlavors() {
    // The caller holds this lock already, which is re-entrant, so that the field is guarded by
    // the same monitor as every other access to it.
    synchronized (this) {
      if (secondaryResourceSpec == null) {
        return;
      }
      applyKueuePodSetFlavor(
          KueueWorkloadFactory.PODSET_MASTER, secondaryResourceSpec.getMasterStatefulSet());
      applyKueuePodSetFlavor(
          KueueWorkloadFactory.PODSET_WORKER, secondaryResourceSpec.getWorkerStatefulSet());
    }
  }

  private void applyKueuePodSetFlavor(String podSetName, StatefulSet statefulSet) {
    KueuePodSetFlavor flavor = kueuePodSetFlavors.get(podSetName);
    if (flavor != null) {
      flavor.applyTo(statefulSet.getSpec().getTemplate().getSpec());
    }
  }

  /**
   * Returns the SparkCluster resource associated with this context.
   *
   * @return The SparkCluster resource.
   */
  @Override
  public SparkCluster getResource() {
    return sparkCluster;
  }

  /**
   * Returns the specification for the master service.
   *
   * @return The Service object for the master.
   */
  public Service getMasterServiceSpec() {
    return getSecondaryResourceSpec().getMasterService();
  }

  /**
   * Returns the specification for the worker service.
   *
   * @return The Service object for the workers.
   */
  public Service getWorkerServiceSpec() {
    return getSecondaryResourceSpec().getWorkerService();
  }

  /**
   * Returns the specification for the master StatefulSet.
   *
   * @return The StatefulSet object for the master.
   */
  public StatefulSet getMasterStatefulSetSpec() {
    return getSecondaryResourceSpec().getMasterStatefulSet();
  }

  /**
   * Returns the specification for the worker StatefulSet.
   *
   * @return The StatefulSet object for the workers.
   */
  public StatefulSet getWorkerStatefulSetSpec() {
    return getSecondaryResourceSpec().getWorkerStatefulSet();
  }

  /**
   * Returns the specification for the worker NetworkPolicy.
   *
   * @return The NetworkPolicy object for the workers.
   */
  public NetworkPolicy getWorkerNetworkPolicySpec() {
    return getSecondaryResourceSpec().getWorkerNetworkPolicy();
  }

  /**
   * Returns the specification for the HorizontalPodAutoscaler, if present.
   *
   * @return An Optional containing the HorizontalPodAutoscaler object.
   */
  public Optional<HorizontalPodAutoscaler> getHorizontalPodAutoscalerSpec() {
    return getSecondaryResourceSpec().getHorizontalPodAutoscaler();
  }

  /**
   * Returns the specification for the PodDisruptionBudget, if present.
   *
   * @return An Optional containing the PodDisruptionBudget object.
   */
  public Optional<PodDisruptionBudget> getPodDisruptionBudgetSpec() {
    return getSecondaryResourceSpec().getPodDisruptionBudget();
  }
}
