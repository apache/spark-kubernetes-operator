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
import static org.apache.spark.k8s.operator.Constants.CLUSTER_SUSPENDED_MESSAGE;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.KUEUE_ENABLED;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.*;
import static org.apache.spark.k8s.operator.status.ClusterStateSummary.RunningHealthy;
import static org.apache.spark.k8s.operator.status.ClusterStateSummary.SchedulingFailure;
import static org.apache.spark.k8s.operator.status.ClusterStateSummary.Suspended;
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
        // Unlike a first attempt, a cluster resumed from Suspended has persisted this Submitted
        // state, which would keep saying that it is resumed. It goes back to Suspended instead,
        // where ClusterSuspendStep releases whatever it holds only after its pods are gone.
        if (cluster.getStatus().getStateTransitionHistory().lastKey() > 0) {
          return appendStateAndImmediateRequeue(
              context, statusRecorder, new ClusterState(Suspended, CLUSTER_SUSPENDED_MESSAGE));
        }
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
    } catch (KubernetesClientException e) {
      if (ReconcilerUtils.isRetryableError(e)) {
        // SchedulingFailure is terminal for a cluster, so a request which may yet succeed is sent
        // again, e.g. for a cluster resumed from Suspended, while a rejected one fails the cluster.
        log.warn("Failed to request master resource, will retry.", e);
        if (!ReconcilerUtils.isTransientError(e)) {
          // Unlike SchedulingFailure, the retry leaves nothing in the status, so a failure which
          // keeps coming back, e.g. from an admission webhook which is down, is reported.
          EventUtils.warn(
              context.getEventRecorder(),
              EventUtils.REASON_CLUSTER_REQUEST_FAILED,
              "Failed to request the master and workers of the cluster, will retry. "
                  + EventUtils.describe(e));
        }
        return completeAndDefaultRequeue();
      }
      return failScheduling(context, statusRecorder, e);
    } catch (Exception e) {
      return failScheduling(context, statusRecorder, e);
    }
  }

  /**
   * Fails the cluster with SchedulingFailure for the given failure of requesting its resources.
   *
   * @param context The SparkClusterContext for the cluster.
   * @param statusRecorder The SparkClusterStatusRecorder for recording status updates.
   * @param e The failure.
   * @return The ReconcileProgress indicating an immediate re-queue.
   */
  private ReconcileProgress failScheduling(
      SparkClusterContext context, SparkClusterStatusRecorder statusRecorder, Exception e) {
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

  /**
   * Requests the Kueue admission of a cluster labeled with a queue name. Like the suspend hold, a
   * master requested before must complete its initialization, so only the flavors which Kueue
   * assigned to it are applied again then. An
   * unsupported spec fails to build the Workload, which the caller turns into SchedulingFailure.
   * SchedulingFailure is terminal for a cluster, so an API failure of the admission request is
   * retried instead, see {@link KueueWorkloadUtils#holdForAdmission}. A cluster without the label
   * releases the Workload left pending from before the label was removed instead, or applies the
   * flavors of the one admitted before, see {@link KueueWorkloadUtils#releaseDequeuedWorkload}.
   *
   * @param context The SparkClusterContext for the cluster.
   * @param cluster The SparkCluster.
   * @return The progress to return while the admission is not granted, or empty to proceed.
   */
  private Optional<ReconcileProgress> holdForKueueAdmission(
      SparkClusterContext context, SparkCluster cluster) {
    if (!KueueWorkloadFactory.hasQueueName(cluster)) {
      return KueueWorkloadUtils.releaseDequeuedWorkload(context);
    }
    if (!KUEUE_ENABLED.getValue()) {
      KueueWorkloadUtils.warnQueueNameIgnored(context);
      return Optional.empty();
    }
    try {
      if (isMasterRequested(context)) {
        // The master and worker StatefulSets are applied again in this reconcile, so the flavors
        // of the Workload which was admitted before are resolved again instead of dropping them
        // from the pod templates.
        return KueueWorkloadUtils.applyAdmittedFlavors(context);
      }
    } catch (KubernetesClientException e) {
      // Requesting the admission of a master which is already running would be wrong, so the
      // lookup is retried rather than failing the cluster with the terminal SchedulingFailure,
      // and it is reported like the admission request it precedes.
      return Optional.of(
          KueueWorkloadUtils.retryAfterRequestFailure(
              context,
              e,
              "Failed to check whether the master exists before requesting Kueue admission"));
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
