/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.kubernetes.operator.reconciler.reconcilesteps;

import java.util.Collections;
import java.util.Set;

import io.fabric8.kubernetes.api.model.Pod;

import org.apache.spark.kubernetes.operator.Constants;
import org.apache.spark.kubernetes.operator.controller.SparkAppContext;
import org.apache.spark.kubernetes.operator.reconciler.ReconcileProgress;
import org.apache.spark.kubernetes.operator.reconciler.observers.AppDriverRunningObserver;
import org.apache.spark.kubernetes.operator.spec.InstanceConfig;
import org.apache.spark.kubernetes.operator.status.ApplicationState;
import org.apache.spark.kubernetes.operator.status.ApplicationStateSummary;
import org.apache.spark.kubernetes.operator.utils.PodUtils;
import org.apache.spark.kubernetes.operator.utils.SparkAppStatusRecorder;

import static org.apache.spark.kubernetes.operator.reconciler.ReconcileProgress.completeAndDefaultRequeue;

/**
 * Observe whether app acquires enough executors as configured in spec
 */
public class AppRunningStep extends AppReconcileStep {
  @Override
  public ReconcileProgress reconcile(SparkAppContext context,
                                     SparkAppStatusRecorder statusRecorder) {
    InstanceConfig instanceConfig = context.getResource().getSpec().getApplicationTolerations()
            .getInstanceConfig();
    ApplicationStateSummary prevStateSummary = context.getResource().getStatus().getCurrentState()
            .getCurrentStateSummary();
    ApplicationStateSummary proposedStateSummary;
    String stateMessage = context.getResource().getStatus().getCurrentState().getMessage();
    if (instanceConfig == null
        || instanceConfig.getInitExecutors() == 0L
        || (!prevStateSummary.isStarting() && instanceConfig.getMinExecutors() == 0L)) {
      proposedStateSummary = ApplicationStateSummary.RUNNING_HEALTHY;
      stateMessage = Constants.RunningHealthyMessage;
    } else {
      Set<Pod> executors = context.getExecutorsForApplication();
      long runningExecutors = executors.stream()
          .filter(PodUtils::isPodReady)
          .count();
      if (prevStateSummary.isStarting()) {
        if (runningExecutors >= instanceConfig.getInitExecutors()) {
          proposedStateSummary = ApplicationStateSummary.RUNNING_HEALTHY;
          stateMessage = Constants.RunningHealthyMessage;
        } else if (runningExecutors > 0L) {
          proposedStateSummary =
              ApplicationStateSummary.INITIALIZED_BELOW_THRESHOLD_EXECUTORS;
          stateMessage = Constants.InitializedWithBelowThresholdExecutorsMessage;
        } else {
          // keep previous state for 0 executor
          proposedStateSummary = prevStateSummary;
        }
      } else {
        if (runningExecutors >= instanceConfig.getMinExecutors()) {
          proposedStateSummary = ApplicationStateSummary.RUNNING_HEALTHY;
          stateMessage = Constants.RunningHealthyMessage;
        } else {
          proposedStateSummary =
              ApplicationStateSummary.RUNNING_WITH_BELOW_THRESHOLD_EXECUTORS;
          stateMessage = Constants.RunningWithBelowThresholdExecutorsMessage;
        }
      }
    }
    if (!proposedStateSummary.equals(prevStateSummary)) {
      statusRecorder.persistStatus(context, context.getResource().getStatus()
          .appendNewState(new ApplicationState(proposedStateSummary, stateMessage)));
      return completeAndDefaultRequeue();
    } else {
      return observeDriver(context, statusRecorder,
          Collections.singletonList(new AppDriverRunningObserver()));
    }
  }
}
