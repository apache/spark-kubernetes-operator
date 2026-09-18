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
import io.javaoperatorsdk.operator.api.event.EventType;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.context.SparkClusterContext;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadFactory;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadUtils;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadUtils.AdmissionResult;
import org.apache.spark.k8s.operator.kueue.v1beta2.Workload;
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
    if (cluster.getSpec().isSuspend() && !isMasterRequested(context)) {
      log.debug("Cluster is suspended, master and worker resources would not be requested.");
      if (KueueWorkloadFactory.hasQueueName(cluster)) {
        // A resource suspended while queued must not keep holding the Kueue quota.
        KueueWorkloadUtils.releaseWorkload(context.getClient(), cluster);
      }
      return completeAndDefaultRequeue();
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
   * retried instead. Events are published only when the Workload is created and admitted, not per
   * requeue while it waits, so that a long queue does not add API calls. A stale Workload is
   * replaced by the operator itself shortly, so it publishes nothing until the new Workload is
   * queued.
   *
   * @param context The SparkClusterContext for the cluster.
   * @param cluster The SparkCluster.
   * @return The progress to return while the admission is not granted, or empty to proceed.
   */
  private Optional<ReconcileProgress> holdForKueueAdmission(
      SparkClusterContext context, SparkCluster cluster) {
    if (!KueueWorkloadFactory.hasQueueName(cluster) || isMasterRequested(context)) {
      return Optional.empty();
    }
    Workload desired = KueueWorkloadFactory.buildWorkload(cluster);
    AdmissionResult admission;
    try {
      admission = KueueWorkloadUtils.requestAdmission(context.getClient(), desired);
    } catch (IllegalStateException | KubernetesClientException e) {
      log.warn("Failed to request Kueue admission, will retry.", e);
      // Like a status update failure, a transport level failure is not published, since writing
      // an event would only add load to an API server that is often the cause of the failure.
      if (!(e instanceof KubernetesClientException kce && ReconcilerUtils.isTransientError(kce))) {
        EventUtils.warn(
            context.getEventRecorder(),
            EventUtils.REASON_KUEUE_ADMISSION_REQUEST_FAILED,
            "Failed to request Kueue admission, will retry. " + EventUtils.describe(e));
      }
      return Optional.of(
          completeAndRequeueAfter(KueueWorkloadUtils.STALE_WORKLOAD_REQUEUE_INTERVAL));
    }
    if (admission == AdmissionResult.STALE) {
      return Optional.of(
          completeAndRequeueAfter(KueueWorkloadUtils.STALE_WORKLOAD_REQUEUE_INTERVAL));
    }
    String workloadName = desired.getMetadata().getName();
    if (admission == AdmissionResult.QUEUED) {
      EventUtils.record(
          context.getEventRecorder(),
          EventType.NORMAL,
          EventUtils.REASON_KUEUE_ADMISSION_PENDING,
          "Waiting for Kueue to admit Workload "
              + workloadName
              + " in queue "
              + desired.getSpec().getQueueName()
              + ", master and workers would be requested after the admission.");
    }
    if (admission == AdmissionResult.QUEUED || admission == AdmissionResult.PENDING) {
      log.debug("Kueue has not admitted the cluster, master would not be requested.");
      return Optional.of(completeAndDefaultRequeue());
    }
    EventUtils.record(
        context.getEventRecorder(),
        EventType.NORMAL,
        EventUtils.REASON_KUEUE_ADMITTED,
        "Kueue admitted Workload " + workloadName + ", requesting master and workers.");
    return Optional.empty();
  }

  /**
   * Checks whether the master StatefulSet has already been requested, e.g. when the status update
   * to RunningHealthy failed after the resources were created.
   *
   * @param context The SparkClusterContext for the cluster.
   * @return True if the master StatefulSet exists, false otherwise.
   */
  private boolean isMasterRequested(SparkClusterContext context) {
    return ReconcilerUtils.getResource(context.getClient(), context.getMasterStatefulSetSpec())
        .isPresent();
  }
}
