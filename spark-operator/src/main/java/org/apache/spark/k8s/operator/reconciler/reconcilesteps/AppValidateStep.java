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
import static org.apache.spark.k8s.operator.spec.DeploymentMode.ClientMode;
import static org.apache.spark.k8s.operator.utils.SparkAppStatusUtils.isValidApplicationStatus;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;

/** Validates the submitted app. This can be re-factored into webhook in the future. */
@Slf4j
public class AppValidateStep extends AppReconcileStep {
  /**
   * Reconciles the application by validating its configuration and status.
   *
   * @param context The SparkAppContext for the application.
   * @param statusRecorder The SparkAppStatusRecorder for recording status updates.
   * @return The ReconcileProgress indicating the next step.
   */
  @Override
  public ReconcileProgress reconcile(
      SparkAppContext context, SparkAppStatusRecorder statusRecorder) {
    if (!isValidApplicationStatus(context.getResource())) {
      log.warn("Spark application found with empty status. Resetting to initial state.");
      return attemptStatusUpdate(context, statusRecorder, new ApplicationStatus(), proceed());
    }
    if (ClientMode.equals(context.getResource().getSpec().getDeploymentMode())) {
      ApplicationState failure =
          new ApplicationState(ApplicationStateSummary.Failed, "Client mode is not supported yet.");
      return attemptStatusUpdate(
          context,
          statusRecorder,
          context.getResource().getStatus().appendNewState(failure),
          completeAndImmediateRequeue());
    }
    return proceed();
  }
}
