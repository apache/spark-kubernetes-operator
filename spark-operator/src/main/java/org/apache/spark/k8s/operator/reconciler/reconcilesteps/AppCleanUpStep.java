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
import java.time.Instant;
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

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.config.SparkOperatorConf;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.spec.ApplicationTolerations;
import org.apache.spark.k8s.operator.spec.ResourceRetainPolicy;
import org.apache.spark.k8s.operator.spec.RestartPolicy;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.utils.ReconcilerUtils;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;

/**
 * Cleanup all secondary resources when application is deleted, or at the end of each attempt.
 * Update Application status to indicate whether another attempt would be made
 */
@AllArgsConstructor
@NoArgsConstructor
@Slf4j
public class AppCleanUpStep extends AppReconcileStep {
  private Supplier<ApplicationState> cleanUpSuccessStateSupplier;

  @Override
  public ReconcileProgress reconcile(
      SparkAppContext context, SparkAppStatusRecorder statusRecorder) {
    ApplicationStatus currentStatus = context.getResource().getStatus();
    ApplicationTolerations tolerations =
        context.getResource().getSpec().getApplicationTolerations();
    ResourceRetainPolicy resourceRetainPolicy = tolerations.getResourceRetainPolicy();
    String stateMessage = null;

    if (retainReleaseResource(resourceRetainPolicy, currentStatus.getCurrentState())) {
      if (tolerations.getRestartConfig() != null
          && !RestartPolicy.Never.equals(tolerations.getRestartConfig().getRestartPolicy())) {
        stateMessage =
            "Application is configured to restart, resources created in current "
                + "attempt would be force released.";
        log.warn(stateMessage);
      } else {
        ApplicationState terminationState =
            new ApplicationState(
                ApplicationStateSummary.TerminatedWithoutReleaseResources,
                "Application is terminated without releasing resources as configured.");
        long requeueAfterMillis =
            tolerations.getApplicationTimeoutConfig().getTerminationRequeuePeriodMillis();
        return appendStateAndRequeueAfter(
            context, statusRecorder, terminationState, Duration.ofMillis(requeueAfterMillis));
      }
    }
    List<HasMetadata> resourcesToRemove = new ArrayList<>();
    if (ApplicationStateSummary.SchedulingFailure.equals(
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
        ApplicationState updatedState =
            new ApplicationState(
                ApplicationStateSummary.ResourceReleased,
                "Cannot build Spark spec for given application, "
                    + "consider all resources as released.");
        long requeueAfterMillis =
            tolerations.getApplicationTimeoutConfig().getTerminationRequeuePeriodMillis();
        return appendStateAndRequeueAfter(
            context, statusRecorder, updatedState, Duration.ofMillis(requeueAfterMillis));
      }
    } else {
      Optional<Pod> driver = context.getDriverPod();
      driver.ifPresent(resourcesToRemove::add);
    }
    boolean forceDelete = enableForceDelete(context.getResource());
    for (HasMetadata resource : resourcesToRemove) {
      ReconcilerUtils.deleteResourceIfExists(context.getClient(), resource, forceDelete);
    }
    ApplicationStatus updatedStatus;
    if (cleanUpSuccessStateSupplier != null) {
      ApplicationState state = cleanUpSuccessStateSupplier.get();
      if (StringUtils.isNotEmpty(stateMessage)) {
        state.setMessage(stateMessage);
      }
      long requeueAfterMillis =
          tolerations.getApplicationTimeoutConfig().getTerminationRequeuePeriodMillis();
      return appendStateAndRequeueAfter(
          context, statusRecorder, state, Duration.ofMillis(requeueAfterMillis));
    } else {
      updatedStatus =
          currentStatus.terminateOrRestart(
              tolerations.getRestartConfig(),
              tolerations.getResourceRetainPolicy(),
              stateMessage,
              SparkOperatorConf.TRIM_ATTEMPT_STATE_TRANSITION_HISTORY.getValue());
      long requeueAfterMillis =
          tolerations.getApplicationTimeoutConfig().getTerminationRequeuePeriodMillis();
      if (ApplicationStateSummary.ScheduledToRestart.equals(
          updatedStatus.getCurrentState().getCurrentStateSummary())) {
        requeueAfterMillis = tolerations.getRestartConfig().getRestartBackoffMillis();
      }
      return updateStatusAndRequeueAfter(
          context, statusRecorder, updatedStatus, Duration.ofMillis(requeueAfterMillis));
    }
  }

  protected boolean retainReleaseResource(
      ResourceRetainPolicy resourceRetainPolicy, ApplicationState currentState) {
    return switch (resourceRetainPolicy) {
      case Never -> false;
      case OnFailure -> currentState.getCurrentStateSummary().isFailure();
      default -> true;
    };
  }

  protected boolean enableForceDelete(SparkApplication app) {
    long timeoutThreshold =
        app.getSpec()
            .getApplicationTolerations()
            .getApplicationTimeoutConfig()
            .getForceTerminationGracePeriodMillis();
    Instant lastTransitionTime =
        Instant.parse(app.getStatus().getCurrentState().getLastTransitionTime());
    return lastTransitionTime.plusMillis(timeoutThreshold).isBefore(Instant.now());
  }
}
