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

import static org.apache.spark.k8s.operator.Constants.*;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.completeAndDefaultRequeue;

import java.util.List;
import java.util.Map;
import java.util.Set;

import io.fabric8.kubernetes.api.model.Pod;

import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.reconciler.observers.AppDriverRunningObserver;
import org.apache.spark.k8s.operator.spec.ExecutorInstanceConfig;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.utils.PodUtils;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;

/** Observe whether app acquires enough executors as configured in spec. */
public class AppRunningStep extends AppReconcileStep {
  /**
   * Reconciles the application's running state, checking if enough executors are acquired.
   *
   * @param context The SparkAppContext for the application.
   * @param statusRecorder The SparkAppStatusRecorder for recording status updates.
   * @return The ReconcileProgress indicating the next step.
   */
  @Override
  public ReconcileProgress reconcile(
      SparkAppContext context, SparkAppStatusRecorder statusRecorder) {
    ExecutorInstanceConfig executorInstanceConfig =
        context.getResource().getSpec().getApplicationTolerations().getInstanceConfig();
    ApplicationStateSummary prevStateSummary =
        context.getResource().getStatus().getCurrentState().getCurrentStateSummary();
    ApplicationStateSummary proposedStateSummary;
    String stateMessage = context.getResource().getStatus().getCurrentState().getMessage();
    if (executorInstanceConfig == null
        || executorInstanceConfig.getInitExecutors() == 0L
        || !prevStateSummary.isStarting() && executorInstanceConfig.getMinExecutors() == 0L) {
      proposedStateSummary = ApplicationStateSummary.RunningHealthy;
      stateMessage = RUNNING_HEALTHY_MESSAGE;
    } else {
      Set<Pod> executors = context.getExecutorsForApplication();
      long runningExecutors = executors.stream().filter(PodUtils::isPodReady).count();
      if (prevStateSummary.isStarting()) {
        if (runningExecutors >= executorInstanceConfig.getInitExecutors()) {
          if (!isDynamicAllocationEnabled(context)
              && executorInstanceConfig.getMaxExecutors() > 0
              && runningExecutors < executorInstanceConfig.getMaxExecutors()) {
            proposedStateSummary = ApplicationStateSummary.RunningWithPartialCapacity;
            stateMessage = RUNNING_WITH_PARTIAL_CAPACITY_MESSAGE;
          } else {
            proposedStateSummary = ApplicationStateSummary.RunningHealthy;
            stateMessage = RUNNING_HEALTHY_MESSAGE;
          }
        } else if (runningExecutors > 0L) {
          proposedStateSummary = ApplicationStateSummary.InitializedBelowThresholdExecutors;
          stateMessage = INITIALIZED_WITH_BELOW_THRESHOLD_EXECUTORS_MESSAGE;
        } else {
          // keep previous state for 0 executor
          proposedStateSummary = prevStateSummary;
        }
      } else {
        if (runningExecutors >= executorInstanceConfig.getMinExecutors()) {
          if (!isDynamicAllocationEnabled(context)
              && executorInstanceConfig.getMaxExecutors() > 0
              && runningExecutors < executorInstanceConfig.getMaxExecutors()) {
            proposedStateSummary = ApplicationStateSummary.RunningWithPartialCapacity;
            stateMessage = RUNNING_WITH_PARTIAL_CAPACITY_MESSAGE;
          } else {
            proposedStateSummary = ApplicationStateSummary.RunningHealthy;
            stateMessage = RUNNING_HEALTHY_MESSAGE;
          }
        } else {
          proposedStateSummary = ApplicationStateSummary.RunningWithBelowThresholdExecutors;
          stateMessage = RUNNING_WITH_BELOW_THRESHOLD_EXECUTORS_MESSAGE;
        }
      }
    }
    if (proposedStateSummary == prevStateSummary) {
      return observeDriver(context, statusRecorder, List.of(new AppDriverRunningObserver()));
    } else {
      ApplicationStatus updatedStatus =
          context
              .getResource()
              .getStatus()
              .appendNewState(new ApplicationState(proposedStateSummary, stateMessage));
      return attemptStatusUpdate(
          context, statusRecorder, updatedStatus, completeAndDefaultRequeue());
    }
  }

  /**
   * Checks if dynamic allocation is enabled for the application.
   *
   * @param context The SparkAppContext
   * @return true if dynamic allocation is enabled, false otherwise
   */
  private boolean isDynamicAllocationEnabled(SparkAppContext context) {
      Map<String, String> sparkConf = context.getSparkConf();
      return "true".equalsIgnoreCase(
          sparkConf.getOrDefault("spark.dynamicAllocation.enabled", "false"));
  }
}
