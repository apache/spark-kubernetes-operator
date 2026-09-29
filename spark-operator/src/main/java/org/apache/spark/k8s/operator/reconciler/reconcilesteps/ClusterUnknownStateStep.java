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

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.context.SparkClusterContext;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.status.ClusterState;
import org.apache.spark.k8s.operator.status.ClusterStateSummary;
import org.apache.spark.k8s.operator.utils.SparkClusterStatusRecorder;

/** Abnormal state handler for clusters. */
public final class ClusterUnknownStateStep extends ClusterReconcileStep {
  /**
   * Reconciles the cluster when it is in an unknown state, marking it as failed.
   *
   * @param context The SparkClusterContext for the cluster.
   * @param statusRecorder The SparkClusterStatusRecorder for recording status updates.
   * @return The ReconcileProgress indicating an immediate re-queue, or no re-queue if the cluster
   *     is already failed.
   */
  @Override
  public ReconcileProgress reconcile(
      SparkClusterContext context, SparkClusterStatusRecorder statusRecorder) {
    ClusterStateSummary currentStateSummary =
        context.getResource().getStatus().getCurrentState().getCurrentStateSummary();
    // Appending Failed again would write a new status on every reconcile, forever.
    if (currentStateSummary == ClusterStateSummary.Failed) {
      return ReconcileProgress.completeAndNoRequeue();
    }
    // SchedulingFailure already reports the cause, so Failed points back to it rather than reading
    // as another, unexplained failure.
    String message =
        currentStateSummary == ClusterStateSummary.SchedulingFailure
            ? Constants.CLUSTER_FAILED_AFTER_SCHEDULING_FAILURE_MESSAGE
            : Constants.UNKNOWN_CLUSTER_STATE_MESSAGE;
    ClusterState state = new ClusterState(ClusterStateSummary.Failed, message);
    statusRecorder.persistStatus(context, context.getResource().getStatus().appendNewState(state));
    return ReconcileProgress.completeAndImmediateRequeue();
  }
}
