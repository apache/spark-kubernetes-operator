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

import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.completeAndDefaultRequeue;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.completeAndImmediateRequeue;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.proceed;
import static org.apache.spark.k8s.operator.utils.SparkAppStatusUtils.driverUnexpectedRemoved;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.Pod;
import lombok.extern.log4j.Log4j2;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.reconciler.observers.BaseAppDriverObserver;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;

/** Basic reconcile step for application. */
@Log4j2
public abstract class AppReconcileStep {
  /**
   * Reconciles a specific step for a Spark application.
   *
   * @param context The SparkAppContext for the application.
   * @param statusRecorder The SparkAppStatusRecorder for recording status updates.
   * @return The ReconcileProgress indicating the next step.
   */
  public abstract ReconcileProgress reconcile(
      SparkAppContext context, SparkAppStatusRecorder statusRecorder);

  /**
   * Observes the driver pod and updates the application status based on a list of observers.
   *
   * @param context The SparkAppContext for the application.
   * @param statusRecorder The SparkAppStatusRecorder for recording status updates.
   * @param observers A list of BaseAppDriverObserver instances to apply.
   * @return The ReconcileProgress indicating the next step.
   */
  protected ReconcileProgress observeDriver(
      final SparkAppContext context,
      final SparkAppStatusRecorder statusRecorder,
      final List<BaseAppDriverObserver> observers) {
    Optional<Pod> driverPodOptional = context.getDriverPod();
    SparkApplication app = context.getResource();
    ApplicationStatus currentStatus = app.getStatus();
    if (driverPodOptional.isPresent()) {
      List<ApplicationState> stateUpdates =
          observers.stream()
              .map(o -> o.observe(driverPodOptional.get(), app.getSpec(), app.getStatus()))
              .filter(Optional::isPresent)
              .map(Optional::get)
              .toList();
      if (stateUpdates.isEmpty()) {
        return proceed();
      } else {
        for (ApplicationState state : stateUpdates) {
          currentStatus = currentStatus.appendNewState(state);
        }
        return attemptStatusUpdate(
            context, statusRecorder, currentStatus, completeAndDefaultRequeue());
      }
    } else {
      ApplicationStatus updatedStatus = currentStatus.appendNewState(driverUnexpectedRemoved());
      return attemptStatusUpdate(
          context, statusRecorder, updatedStatus, completeAndImmediateRequeue());
    }
  }

  /**
   * Updates the application status - if the status is successfully persisted, proceed with the
   * given progress. Otherwise, completes current reconcile loop immediately and requeue. Latest
   * application status would be fetched from cache in next reconcile attempt.
   *
   * @param context The SparkAppContext for the application.
   * @param statusRecorder The SparkAppStatusRecorder for recording status updates.
   * @param updatedStatus The updated ApplicationStatus.
   * @param progressUponSuccessStatusUpdate The ReconcileProgress if the status update has been
   *     persisted successfully.
   * @return The ReconcileProgress for next steps.
   */
  protected ReconcileProgress attemptStatusUpdate(
      final SparkAppContext context,
      final SparkAppStatusRecorder statusRecorder,
      final ApplicationStatus updatedStatus,
      final ReconcileProgress progressUponSuccessStatusUpdate) {

    if (statusRecorder.persistStatus(context, updatedStatus)) {
      return progressUponSuccessStatusUpdate;
    } else {
      log.warn("Failed to persist status, will retry status update in next reconcile attempt");
      return completeAndImmediateRequeue();
    }
  }

  /**
   * Updates the application status and re-queues the reconciliation after a specified duration. If
   * the status update fails, trigger an immediate requeue.
   *
   * @param context The SparkAppContext for the application.
   * @param statusRecorder The SparkAppStatusRecorder for recording status updates.
   * @param updatedStatus The updated ApplicationStatus.
   * @param requeueAfter The duration after which to re-queue.
   * @return The ReconcileProgress indicating the re-queue.
   */
  protected ReconcileProgress updateStatusAndRequeueAfter(
      SparkAppContext context,
      SparkAppStatusRecorder statusRecorder,
      ApplicationStatus updatedStatus,
      Duration requeueAfter) {
    return attemptStatusUpdate(
        context,
        statusRecorder,
        updatedStatus,
        ReconcileProgress.completeAndRequeueAfter(requeueAfter));
  }

  /**
   * Appends a new state to the application status, persists it, and re-queues the reconciliation
   * after a specified duration. If the status update fails, trigger an immediate requeue.
   *
   * @param context The SparkAppContext for the application.
   * @param statusRecorder The SparkAppStatusRecorder for recording status updates.
   * @param newState The new ApplicationState to append.
   * @param requeueAfter The duration after which to re-queue.
   * @return The ReconcileProgress indicating the re-queue.
   */
  protected ReconcileProgress appendStateAndRequeueAfter(
      SparkAppContext context,
      SparkAppStatusRecorder statusRecorder,
      ApplicationState newState,
      Duration requeueAfter) {
    if (!statusRecorder.appendNewStateAndPersist(context, newState)) {
      log.warn("Status is not persisted successfully, will retry in next reconcile attempt");
      return completeAndImmediateRequeue();
    }
    return ReconcileProgress.completeAndRequeueAfter(requeueAfter);
  }

  /**
   * Appends a new state to the application status, persists it, and immediately re-queues the
   * reconciliation.
   *
   * @param context The SparkAppContext for the application.
   * @param statusRecorder The SparkAppStatusRecorder for recording status updates.
   * @param newState The new ApplicationState to append.
   * @return The ReconcileProgress indicating an immediate re-queue.
   */
  protected ReconcileProgress appendStateAndImmediateRequeue(
      SparkAppContext context, SparkAppStatusRecorder statusRecorder, ApplicationState newState) {
    return appendStateAndRequeueAfter(context, statusRecorder, newState, Duration.ZERO);
  }
}
