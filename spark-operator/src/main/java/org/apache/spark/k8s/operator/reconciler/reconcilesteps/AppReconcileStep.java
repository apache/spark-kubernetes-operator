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

import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.completeAndImmediateRequeue;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.proceed;
import static org.apache.spark.k8s.operator.utils.SparkAppStatusUtils.driverUnexpectedRemoved;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.Pod;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.reconciler.observers.BaseAppDriverObserver;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;

/** Basic reconcile step for application. */
public abstract class AppReconcileStep {
  public abstract ReconcileProgress reconcile(
      SparkAppContext context, SparkAppStatusRecorder statusRecorder);

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
        statusRecorder.persistStatus(context, currentStatus);
        return completeAndImmediateRequeue();
      }
    } else {
      ApplicationStatus updatedStatus = currentStatus.appendNewState(driverUnexpectedRemoved());
      statusRecorder.persistStatus(context, updatedStatus);
      return completeAndImmediateRequeue();
    }
  }

  protected ReconcileProgress updateStatusAndRequeueAfter(
      SparkAppContext context,
      SparkAppStatusRecorder statusRecorder,
      ApplicationStatus updatedStatus,
      Duration requeueAfter) {
    statusRecorder.persistStatus(context, updatedStatus);
    return ReconcileProgress.completeAndRequeueAfter(requeueAfter);
  }

  protected ReconcileProgress appendStateAndRequeueAfter(
      SparkAppContext context,
      SparkAppStatusRecorder statusRecorder,
      ApplicationState newState,
      Duration requeueAfter) {
    statusRecorder.appendNewStateAndPersist(context, newState);
    return ReconcileProgress.completeAndRequeueAfter(requeueAfter);
  }

  protected ReconcileProgress appendStateAndImmediateRequeue(
      SparkAppContext context, SparkAppStatusRecorder statusRecorder, ApplicationState newState) {
    return appendStateAndRequeueAfter(context, statusRecorder, newState, Duration.ZERO);
  }
}
