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

import java.time.Duration;

import org.apache.spark.k8s.operator.context.SparkClusterContext;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.status.ClusterState;
import org.apache.spark.k8s.operator.status.ClusterStatus;
import org.apache.spark.k8s.operator.utils.SparkClusterStatusRecorder;

/** Basic reconcile step for cluster. */
public abstract sealed class ClusterReconcileStep
    permits ClusterInitStep, ClusterSuspendStep, ClusterTerminatedStep, ClusterUnknownStateStep,
        ClusterValidateStep {
  /**
   * Reconciles a specific step for a Spark cluster.
   *
   * @param context The SparkClusterContext for the cluster.
   * @param statusRecorder The SparkClusterStatusRecorder for recording status updates.
   * @return The ReconcileProgress indicating the next step.
   */
  public abstract ReconcileProgress reconcile(
      SparkClusterContext context, SparkClusterStatusRecorder statusRecorder);

  /**
   * Updates the cluster status and re-queues the reconciliation after a specified duration.
   *
   * @param context The SparkClusterContext for the cluster.
   * @param statusRecorder The SparkClusterStatusRecorder for recording status updates.
   * @param updatedStatus The updated ClusterStatus.
   * @param requeueAfter The duration after which to re-queue.
   * @return The ReconcileProgress indicating the re-queue, or the default re-queue if the status
   *     is not persisted.
   */
  protected ReconcileProgress updateStatusAndRequeueAfter(
      SparkClusterContext context,
      SparkClusterStatusRecorder statusRecorder,
      ClusterStatus updatedStatus,
      Duration requeueAfter) {
    // A rejected update, e.g. of a state unknown to an older CRD, would be rejected again right
    // away, so it is retried with the default interval, not immediately.
    return statusRecorder.persistStatus(context, updatedStatus)
        ? ReconcileProgress.completeAndRequeueAfter(requeueAfter)
        : ReconcileProgress.completeAndDefaultRequeue();
  }

  /**
   * Appends a new state to the cluster status, persists it, and re-queues the reconciliation after
   * a specified duration.
   *
   * @param context The SparkClusterContext for the cluster.
   * @param statusRecorder The SparkClusterStatusRecorder for recording status updates.
   * @param newState The new ClusterState to append.
   * @param requeueAfter The duration after which to re-queue.
   * @return The ReconcileProgress indicating the re-queue, or the default re-queue if the state
   *     is not persisted.
   */
  protected ReconcileProgress appendStateAndRequeueAfter(
      SparkClusterContext context,
      SparkClusterStatusRecorder statusRecorder,
      ClusterState newState,
      Duration requeueAfter) {
    // A rejected update, e.g. of a state unknown to an older CRD, would be rejected again right
    // away, so it is retried with the default interval, not immediately.
    return statusRecorder.appendNewStateAndPersist(context, newState)
        ? ReconcileProgress.completeAndRequeueAfter(requeueAfter)
        : ReconcileProgress.completeAndDefaultRequeue();
  }

  /**
   * Appends a new state to the cluster status, persists it, and immediately re-queues the
   * reconciliation.
   *
   * @param context The SparkClusterContext for the cluster.
   * @param statusRecorder The SparkClusterStatusRecorder for recording status updates.
   * @param newState The new ClusterState to append.
   * @return The ReconcileProgress indicating an immediate re-queue.
   */
  protected ReconcileProgress appendStateAndImmediateRequeue(
      SparkClusterContext context,
      SparkClusterStatusRecorder statusRecorder,
      ClusterState newState) {
    return appendStateAndRequeueAfter(context, statusRecorder, newState, Duration.ZERO);
  }
}
