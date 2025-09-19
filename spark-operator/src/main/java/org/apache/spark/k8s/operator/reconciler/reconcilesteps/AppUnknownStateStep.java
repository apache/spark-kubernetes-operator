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

import java.util.Optional;

import io.fabric8.kubernetes.api.model.Pod;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;

/** Abnormal state handler. */
public class AppUnknownStateStep extends AppReconcileStep {
  /**
   * Reconciles the application when it is in an unknown state, marking it as failed.
   *
   * @param context The SparkAppContext for the application.
   * @param statusRecorder The SparkAppStatusRecorder for recording status updates.
   * @return The ReconcileProgress indicating an immediate re-queue.
   */
  @Override
  public ReconcileProgress reconcile(
      SparkAppContext context, SparkAppStatusRecorder statusRecorder) {
    ApplicationState state =
        new ApplicationState(ApplicationStateSummary.Failed, Constants.UNKNOWN_STATE_MESSAGE);
    Optional<Pod> driver = context.getDriverPod();
    driver.ifPresent(pod -> state.setLastObservedDriverStatus(pod.getStatus()));
    statusRecorder.persistStatus(context, context.getResource().getStatus().appendNewState(state));
    return ReconcileProgress.completeAndImmediateRequeue();
  }
}
