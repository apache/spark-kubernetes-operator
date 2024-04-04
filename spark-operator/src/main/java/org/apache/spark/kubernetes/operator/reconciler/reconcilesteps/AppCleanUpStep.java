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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import org.apache.spark.kubernetes.operator.config.SparkOperatorConf;
import org.apache.spark.kubernetes.operator.controller.SparkApplicationContext;
import org.apache.spark.kubernetes.operator.reconciler.ReconcileProgress;
import org.apache.spark.kubernetes.operator.reconciler.SparkApplicationReconcileUtils;
import org.apache.spark.kubernetes.operator.reconciler.SparkReconcilerUtils;
import org.apache.spark.kubernetes.operator.spec.ApplicationTolerations;
import org.apache.spark.kubernetes.operator.spec.RestartPolicy;
import org.apache.spark.kubernetes.operator.status.ApplicationState;
import org.apache.spark.kubernetes.operator.status.ApplicationStateSummary;
import org.apache.spark.kubernetes.operator.status.ApplicationStatus;
import org.apache.spark.kubernetes.operator.utils.StatusRecorder;

/**
 * Cleanup all secondary resources when application is deleted, or at the end of each attempt
 * Update Application status to indicate whether another attempt would be made
 */
@AllArgsConstructor
@NoArgsConstructor
@Slf4j
public class AppCleanUpStep extends AppReconcileStep {
  private Supplier<ApplicationState> cleanUpSuccessStateSupplier;

  @Override
  public ReconcileProgress reconcile(SparkApplicationContext context,
                                     StatusRecorder statusRecorder) {
    ApplicationStatus currentStatus = context.getSparkApplication().getStatus();
    ApplicationTolerations tolerations =
        context.getSparkApplication().getSpec().getApplicationTolerations();
    String stateMessage = null;
    if (!tolerations.getDeleteOnTermination()) {
      if (tolerations.getRestartConfig() != null
          && !RestartPolicy.Never.equals(
          tolerations.getRestartConfig().getRestartPolicy())) {
        stateMessage =
            "Application is configured to restart, resources created in current " +
                "attempt would be force released.";
        log.warn(stateMessage);
      } else {
        ApplicationStatus updatedStatus = currentStatus.appendNewState(
            new ApplicationState(
                ApplicationStateSummary.TERMINATED_WITHOUT_RELEASE_RESOURCES,
                "Application is terminated without releasing resources " +
                    "as configured."));
        long requeueAfterMillis = tolerations.getApplicationTimeoutConfig()
            .getTerminationRequeuePeriodMillis();
        return updateStateAndProceed(context, statusRecorder, updatedStatus,
            requeueAfterMillis);
      }
    }
    List<HasMetadata> resourcesToRemove = new ArrayList<>();
    if (ApplicationStateSummary.SCHEDULING_FAILURE.equals(
        currentStatus.getCurrentState().getCurrentStateSummary())) {
      // if app failed at scheduling, re-compute all spec and delete as they may not be fully
      // owned by driver
      try {
        resourcesToRemove.addAll(context.getDriverPreResourcesSpec());
        resourcesToRemove.add(context.getDriverPodSpec());
        resourcesToRemove.addAll(context.getDriverResourcesSpec());
      } catch (Exception e) {
        if (log.isErrorEnabled()) {
          log.error("Failed to build resources for application.", e);
        }
        ApplicationStatus updatedStatus = currentStatus.appendNewState(
            new ApplicationState(ApplicationStateSummary.RESOURCE_RELEASED,
                "Cannot build Spark spec for given application, " +
                    "consider all resources as released."));
        long requeueAfterMillis = tolerations.getApplicationTimeoutConfig()
            .getTerminationRequeuePeriodMillis();

        return updateStateAndProceed(context, statusRecorder, updatedStatus,
            requeueAfterMillis);
      }
    } else {
      Optional<Pod> driver = context.getDriverPod();
      driver.ifPresent(resourcesToRemove::add);
    }
    boolean forceDelete =
        SparkApplicationReconcileUtils.enableForceDelete(context.getSparkApplication());
    for (HasMetadata resource : resourcesToRemove) {
      SparkReconcilerUtils.deleteResourceIfExists(context.getClient(), resource, forceDelete);
    }
    ApplicationStatus updatedStatus;
    if (cleanUpSuccessStateSupplier != null) {
      ApplicationState state = cleanUpSuccessStateSupplier.get();
      if (StringUtils.isNotEmpty(stateMessage)) {
        state.setMessage(stateMessage);
      }
      updatedStatus = currentStatus.appendNewState(state);
      long requeueAfterMillis = tolerations.getApplicationTimeoutConfig()
          .getTerminationRequeuePeriodMillis();
      return updateStateAndProceed(context, statusRecorder, updatedStatus,
          requeueAfterMillis);
    } else {
      updatedStatus =
          currentStatus.terminateOrRestart(tolerations.getRestartConfig(), stateMessage,
              SparkOperatorConf.TrimAttemptStateTransitionHistory.getValue());
      long requeueAfterMillis = tolerations.getApplicationTimeoutConfig()
          .getTerminationRequeuePeriodMillis();
      if (ApplicationStateSummary.SCHEDULED_TO_RESTART.equals(updatedStatus.getCurrentState()
          .getCurrentStateSummary())) {
        requeueAfterMillis = tolerations.getRestartConfig().getRestartBackoffMillis();
      }
      return updateStateAndProceed(context, statusRecorder, updatedStatus,
          requeueAfterMillis);
    }

  }

  private ReconcileProgress updateStateAndProceed(SparkApplicationContext context,
                                                  StatusRecorder statusRecorder,
                                                  ApplicationStatus updatedStatus,
                                                  long requeueAfterMillis) {
    statusRecorder.persistStatus(context, updatedStatus);
    return ReconcileProgress.completeAndRequeueAfter(Duration.ofMillis(requeueAfterMillis));
  }
}
