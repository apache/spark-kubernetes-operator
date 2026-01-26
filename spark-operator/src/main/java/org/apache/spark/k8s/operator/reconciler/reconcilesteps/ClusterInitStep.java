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

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicy;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.context.SparkClusterContext;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.status.ClusterState;
import org.apache.spark.k8s.operator.status.ClusterStatus;
import org.apache.spark.k8s.operator.utils.SparkClusterStatusRecorder;

/** Request cluster master and its resources when starting an attempt. */
@Slf4j
public class ClusterInitStep extends ClusterReconcileStep {
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
    if (cluster.getStatus().getPreviousAttemptSummary() != null) {
      Instant lastTransitionTime = Instant.parse(currentState.getLastTransitionTime());
      Instant restartTime = lastTransitionTime.plusMillis(300 * 1000);
      Instant now = Instant.now();
      if (restartTime.isAfter(now)) {
        return completeAndRequeueAfter(Duration.between(now, restartTime));
      }
    }
    try {
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
}
