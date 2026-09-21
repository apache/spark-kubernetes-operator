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

import static org.apache.spark.k8s.operator.Constants.CLUSTER_READY_MESSAGE;
import static org.apache.spark.k8s.operator.Constants.CLUSTER_SCHEDULE_FAILURE_MESSAGE;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.*;
import static org.apache.spark.k8s.operator.status.ClusterStateSummary.RunningHealthy;
import static org.apache.spark.k8s.operator.status.ClusterStateSummary.SchedulingFailure;
import static org.apache.spark.k8s.operator.utils.SparkExceptionUtils.buildGeneralErrorMessage;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import io.fabric8.kubernetes.client.KubernetesClientException;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.context.SparkClusterContext;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadFactory;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadUtils;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.status.ClusterState;
import org.apache.spark.k8s.operator.status.ClusterStatus;
import org.apache.spark.k8s.operator.utils.EventUtils;
import org.apache.spark.k8s.operator.utils.ReconcilerUtils;
import org.apache.spark.k8s.operator.utils.SparkClusterStatusRecorder;

/** Request cluster master and its resources when starting an attempt. */
@Slf4j
public final class ClusterInitStep extends ClusterReconcileStep {
  /**
   * Reconciles the cluster initialization step, creating master and worker resources.
   *
   * @param context The SparkClusterContext for the cluster.
   * @param statusRecorder The SparkClusterStatusRecorder for recording status updates.
   * @return The ReconcileProgress indicating the next step.
   */
  @Override
  public ReconcileProgress reconcile(
      SparkClusterContext context, SparkClusterStatusRecorder statusRecorder) {
    ClusterState currentState = context.getResource().getStatus().getCurrentState();
    if (!currentState.getCurrentStateSummary().isInitializing()) {
      return proceed();
    }
    SparkCluster cluster = context.getResource();
    // A cluster whose master StatefulSet already exists has been requested before (e.g. the status
    // update to RunningHealthy failed), so let it complete its initialization even if suspended.
    if (cluster.getSpec().isSuspend()) {
      final boolean masterRequested;
      try {
        masterRequested = isMasterRequested(context);
      } catch (KubernetesClientException e) {
        // Whether the master is live is unknown, not answered. Holding would claim in an event
        // that none was requested, and would release the Kueue quota of a running master, so
        // look again with the steady-state interval instead.
        log.warn("Failed to check whether the master of a suspended cluster exists.", e);
        return completeAndDefaultRequeue();
      }
      if (!masterRequested) {
        return SuspendUtils.holdForSuspend(context, "master and workers");
      }
    }
    if (cluster.getStatus().getPreviousAttemptSummary() != null) {
      Instant lastTransitionTime = Instant.parse(currentState.getLastTransitionTime());
      Instant restartTime = lastTransitionTime.plusMillis(300 * 1000);
      Instant now = Instant.now();
      if (restartTime.isAfter(now)) {
        return completeAndRequeueAfter(Duration.between(now, restartTime));
      }
    }
    try {
      Optional<ReconcileProgress> kueueHold = holdForKueueAdmission(context, cluster);
      if (kueueHold.isPresent()) {
        return kueueHold.get();
      }
      Service masterService = context.getMasterServiceSpec();
      context.getClient().services().resource(masterService).forceConflicts().serverSideApply();
      Service workerService = context.getWorkerServiceSpec();
      context.getClient().services().resource(workerService).forceConflicts().serverSideApply();
      StatefulSet masterStatefulSet = context.getMasterStatefulSetSpec();
      context
          .getClient()
          .apps()
          .statefulSets()
          .resource(masterStatefulSet)
          .forceConflicts()
          .serverSideApply();
      StatefulSet workerStatefulSet = context.getWorkerStatefulSetSpec();
      context
          .getClient()
          .apps()
          .statefulSets()
          .resource(workerStatefulSet)
          .forceConflicts()
          .serverSideApply();
      NetworkPolicy workerNetworkPolicy = context.getWorkerNetworkPolicySpec();
      context
          .getClient()
          .network()
          .networkPolicies()
          .resource(workerNetworkPolicy)
          .forceConflicts()
          .serverSideApply();
      var horizontalPodAutoscaler = context.getHorizontalPodAutoscalerSpec();
      if (horizontalPodAutoscaler.isPresent()) {
        context
            .getClient()
            .autoscaling()
            .v2()
            .horizontalPodAutoscalers()
            .resource(horizontalPodAutoscaler.get())
            .forceConflicts()
            .serverSideApply();
      }
      var podDisruptionBudget = context.getPodDisruptionBudgetSpec();
      if (podDisruptionBudget.isPresent()) {
        context
            .getClient()
            .policy()
            .v1()
            .podDisruptionBudget()
            .resource(podDisruptionBudget.get())
            .forceConflicts()
            .serverSideApply();
      }

      ClusterStatus updatedStatus =
          context
              .getResource()
              .getStatus()
              .appendNewState(new ClusterState(RunningHealthy, CLUSTER_READY_MESSAGE));
      statusRecorder.persistStatus(context, updatedStatus);
      return completeAndDefaultRequeue();
    } catch (Exception e) {
      if (log.isErrorEnabled()) {
        log.error("Failed to request master resource.", e);
      }
      String msg = CLUSTER_SCHEDULE_FAILURE_MESSAGE + " StackTrace: " + buildGeneralErrorMessage(e);
      statusRecorder.persistStatus(
          context,
          context
              .getResource()
              .getStatus()
              .appendNewState(new ClusterState(SchedulingFailure, msg)));
      return completeAndImmediateRequeue();
    }
  }

  /**
   * Requests the Kueue admission of a cluster labeled with a queue name. Like the suspend hold, a
   * master requested before must complete its initialization, so the check is skipped then. An
   * unsupported spec fails to build the Workload, which the caller turns into SchedulingFailure.
   * SchedulingFailure is terminal for a cluster, so an API failure of the admission request is
   * retried instead, see {@link KueueWorkloadUtils#holdForAdmission}.
   *
   * @param context The SparkClusterContext for the cluster.
   * @param cluster The SparkCluster.
   * @return The progress to return while the admission is not granted, or empty to proceed.
   */
  private Optional<ReconcileProgress> holdForKueueAdmission(
      SparkClusterContext context, SparkCluster cluster) {
    if (!KueueWorkloadFactory.hasQueueName(cluster)) {
      return Optional.empty();
    }
    try {
      if (isMasterRequested(context)) {
        return Optional.empty();
      }
    } catch (KubernetesClientException e) {
      // Requesting the admission of a master which is already running would be wrong, so the
      // lookup is retried rather than failing the cluster with the terminal SchedulingFailure.
      // Like a failed admission request, a transport level failure is not published, since
      // writing an event would only add load to an API server that is often the cause of the
      // failure, and it goes away on its own, so it keeps the short interval.
      log.warn("Failed to check whether the master exists before requesting admission.", e);
      if (ReconcilerUtils.isTransientError(e)) {
        return Optional.of(
            completeAndRequeueAfter(KueueWorkloadUtils.STALE_WORKLOAD_REQUEUE_INTERVAL));
      }
      // A persistent failure, such as the RBAC rules for reading StatefulSets, is retried with
      // the default interval, so that its event is not rewritten every few seconds until a user
      // fixes the cause.
      EventUtils.warn(
          context.getEventRecorder(),
          EventUtils.REASON_KUEUE_ADMISSION_REQUEST_FAILED,
          "Failed to check whether the master exists before requesting Kueue admission, will "
              + "retry. "
              + EventUtils.describe(e));
      return Optional.of(completeAndDefaultRequeue());
    }
    return KueueWorkloadUtils.holdForAdmission(
        context, KueueWorkloadFactory.buildWorkload(cluster), "master and workers");
  }

  /**
   * Checks whether the master StatefulSet has already been requested, e.g. when the status update
   * to RunningHealthy failed after the resources were created.
   *
   * @param context The SparkClusterContext for the cluster.
   * @return True if the master StatefulSet exists, false otherwise.
   * @throws KubernetesClientException if the lookup fails, so that a running master is not
   *     mistaken for one that was never requested.
   */
  private boolean isMasterRequested(SparkClusterContext context) {
    // Neither ReconcilerUtils read is used here: getResource reports a StatefulSet it could not
    // read as absent, and getResourceStrictly does the same for a transient failure, a 500 or a
    // 429, since the create path it serves re-reads anyway. Either would release the Kueue quota
    // of a running master. Only a 404 may mean absent, which the client reports as null.
    return context.getClient().resource(context.getMasterStatefulSetSpec()).get() != null;
  }
}
