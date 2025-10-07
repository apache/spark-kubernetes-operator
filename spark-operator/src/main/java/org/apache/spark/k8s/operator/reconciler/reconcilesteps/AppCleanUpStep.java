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
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.function.Supplier;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

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
import org.apache.spark.k8s.operator.utils.SparkAppStatusUtils;
import org.apache.spark.k8s.operator.utils.StringUtils;

/**
 * Cleanup all secondary resources when application is deleted, or at the end of each attempt.
 * Update Application status to indicate whether another attempt would be made.
 */
@NoArgsConstructor
@Slf4j
public class AppCleanUpStep extends AppReconcileStep {
  private Supplier<ApplicationState> onDemandCleanUpReason;
  private String stateUpdateMessage;

  /**
   * Constructs an AppCleanUpStep with a specific reason for on-demand cleanup.
   *
   * @param onDemandCleanUpReason A Supplier that provides the ApplicationState for on-demand
   *     cleanup.
   */
  public AppCleanUpStep(Supplier<ApplicationState> onDemandCleanUpReason) {
    super();
    this.onDemandCleanUpReason = onDemandCleanUpReason;
  }

  /**
   * Cleanup secondary resources for an application if needed and updates application status
   * accordingly. This step would be performed right after validation step in each reconcile as a
   * sanity check. It may end the reconciliation if no more actions are needed. In addition, it can
   * be performed on demand with a reason for cleanup secondary resources.
   *
   * <p>An app expects its secondary resources to be released if any of the below is true:
   *
   * <ul>
   *   <li>When the application is being deleted on demand(e.g. being deleted) with a reason
   *   <li>When the application is stopping
   *   <li>When the application has terminated without releasing resources, but it has exceeded
   *       configured retention duration
   * </ul>
   *
   * <p>It would proceed to next steps with no actions for application in other states. Note that
   * when even the reconciler decides to proceed with clean up, sub-resources may still be retained
   * based on tolerations.
   *
   * @param context The SparkAppContext for the application.
   * @param statusRecorder The SparkAppStatusRecorder for recording status updates.
   * @return The ReconcileProgress indicating the next step.
   */
  @Override
  public ReconcileProgress reconcile(
      SparkAppContext context, SparkAppStatusRecorder statusRecorder) {
    SparkApplication application = context.getResource();
    ApplicationStatus currentStatus = application.getStatus();
    ApplicationState currentState = currentStatus.getCurrentState();
    ApplicationTolerations tolerations = application.getSpec().getApplicationTolerations();
    if (currentState.getCurrentStateSummary().isTerminated()) {
      Optional<ReconcileProgress> terminatedAppProgress =
          checkEarlyExitForTerminatedApp(context.getClient(), application, statusRecorder);
      if (terminatedAppProgress.isPresent()) {
        return terminatedAppProgress.get();
      }
    } else if (isOnDemandCleanup()) {
      log.info("Releasing secondary resources for application on demand.");
    } else if (currentState.getCurrentStateSummary().isStopping()) {
      if (retainReleaseResourceForPolicyAndState(
          tolerations.getResourceRetainPolicy(), currentState)) {
        if (tolerations.getRestartConfig() != null
            && !RestartPolicy.Never.equals(tolerations.getRestartConfig().getRestartPolicy())) {
          stateUpdateMessage =
              "Application is configured to restart, resources created in current "
                  + "attempt would be force released.";
          log.warn(stateUpdateMessage);
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
    } else {
      log.debug("Clean up is not expected for app, proceeding to next step.");
      return ReconcileProgress.proceed();
    }

    List<HasMetadata> resourcesToRemove = new ArrayList<>();
    if (isReleasingResourcesForSchedulingFailureAttempt(currentStatus)) {
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
    boolean forceDelete = enableForceDelete(application);
    for (HasMetadata resource : resourcesToRemove) {
      ReconcilerUtils.deleteResourceIfExists(context.getClient(), resource, forceDelete);
    }
    ApplicationStatus updatedStatus;
    if (onDemandCleanUpReason != null) {
      ApplicationState state = onDemandCleanUpReason.get();
      if (StringUtils.isNotEmpty(stateUpdateMessage)) {
        state.setMessage(stateUpdateMessage);
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
              stateUpdateMessage,
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

  /**
   * Clears the status cache and indicates that reconciliation for the application is complete.
   *
   * @param application The SparkApplication to clear cache for.
   * @param statusRecorder The SparkAppStatusRecorder.
   * @return An Optional containing a ReconcileProgress to complete and not re-queue.
   */
  protected Optional<ReconcileProgress> clearCacheAndFinishReconcileForApplication(
      final SparkApplication application, final SparkAppStatusRecorder statusRecorder) {
    log.debug("Cleaning up status cache and stop reconciling for application.");
    statusRecorder.removeCachedStatus(application);
    return Optional.of(ReconcileProgress.completeAndNoRequeue());
  }

  /**
   * Checks if an early exit from reconciliation is possible for a terminated application, based on
   * resource retention policies and TTL settings.
   *
   * @param client The KubernetesClient.
   * @param application The SparkApplication.
   * @param statusRecorder The SparkAppStatusRecorder.
   * @return An Optional containing a ReconcileProgress if an early exit is determined, otherwise
   *     empty.
   */
  protected Optional<ReconcileProgress> checkEarlyExitForTerminatedApp(
      final KubernetesClient client,
      final SparkApplication application,
      final SparkAppStatusRecorder statusRecorder) {
    ApplicationStatus currentStatus = application.getStatus();
    ApplicationState currentState = currentStatus.getCurrentState();
    ApplicationTolerations tolerations = application.getSpec().getApplicationTolerations();
    Instant now = Instant.now();
    if (ApplicationStateSummary.ResourceReleased.equals(currentState.getCurrentStateSummary())) {
      // Perform TTL check after removing all secondary resources, if enabled
      if (isOnDemandCleanup() || !tolerations.isTTLEnabled()) {
        // all secondary resources have been released, no more reconciliations needed
        return clearCacheAndFinishReconcileForApplication(application, statusRecorder);
      } else {
        ApplicationState lastObservedStateBeforeTermination =
            getLastObservedStateBeforeTermination(currentStatus);
        Duration nextCheckDuration =
            Duration.between(
                now,
                Instant.parse(lastObservedStateBeforeTermination.getLastTransitionTime())
                    .plusMillis(tolerations.getTtlAfterStopMillis()));
        if (nextCheckDuration.isNegative()) {
          log.info("Garbage collecting application exceeded given ttl.");
          ReconcilerUtils.deleteResourceIfExists(client, application, true);
          return clearCacheAndFinishReconcileForApplication(application, statusRecorder);
        } else {
          log.info(
              "Application has yet expired, reconciliation would be resumed in {} millis.",
              nextCheckDuration.toMillis());
          return Optional.of(ReconcileProgress.completeAndRequeueAfter(nextCheckDuration));
        }
      }
    }
    if (isOnDemandCleanup()) {
      return Optional.empty();
    }
    if (ApplicationStateSummary.TerminatedWithoutReleaseResources.equals(
        currentState.getCurrentStateSummary())) {
      if (tolerations.isRetainDurationEnabled()) {
        if (tolerations.exceedRetainDurationAtInstant(currentState, now)) {
          log.info("Garbage collecting secondary resources for application");
          onDemandCleanUpReason = SparkAppStatusUtils::appExceededRetainDuration;
          return Optional.empty();
        } else {
          Duration nextCheckDuration =
              Duration.between(
                  now,
                  Instant.parse(currentState.getLastTransitionTime())
                      .plusMillis(tolerations.computeEffectiveRetainDurationMillis()));
          log.info(
              "Application is within retention, reconciliation would be resumed in {} millis.",
              nextCheckDuration.toMillis());
          return Optional.of(ReconcileProgress.completeAndRequeueAfter(nextCheckDuration));
        }
      } else {
        log.info("Retention duration check is not enabled for application.");
        return clearCacheAndFinishReconcileForApplication(application, statusRecorder);
      }
    }
    return Optional.empty();
  }

  /**
   * Checks if an on-demand cleanup has been requested.
   *
   * @return True if on-demand cleanup is requested, false otherwise.
   */
  protected boolean isOnDemandCleanup() {
    return onDemandCleanUpReason != null;
  }

  /**
   * Returns the last observed state of the application before it terminated.
   *
   * @param status The current ApplicationStatus.
   * @return The ApplicationState before termination, or the current state if not terminated.
   */
  protected ApplicationState getLastObservedStateBeforeTermination(final ApplicationStatus status) {
    ApplicationState lastObservedState = status.getCurrentState();
    if (lastObservedState.getCurrentStateSummary().isTerminated()) {
      NavigableMap<Long, ApplicationState> navMap =
          (NavigableMap<Long, ApplicationState>) status.getStateTransitionHistory();
      Map.Entry<Long, ApplicationState> terminateState = navMap.lastEntry();
      return navMap.lowerEntry(terminateState.getKey()).getValue();
    }
    return lastObservedState;
  }

  /**
   * Determines if resources should be released due to a scheduling failure attempt.
   *
   * @param status The current ApplicationStatus.
   * @return True if resources should be released due to scheduling failure, false otherwise.
   */
  protected boolean isReleasingResourcesForSchedulingFailureAttempt(
      final ApplicationStatus status) {
    ApplicationState lastObservedState = getLastObservedStateBeforeTermination(status);
    return ApplicationStateSummary.SchedulingFailure.equals(
        lastObservedState.getCurrentStateSummary());
  }

  /**
   * Determines whether to retain or release resources based on the resource retention policy and
   * current application state.
   *
   * @param resourceRetainPolicy The ResourceRetainPolicy configured for the application.
   * @param currentState The current ApplicationState.
   * @return True if resources should be retained, false if they should be released.
   */
  protected boolean retainReleaseResourceForPolicyAndState(
      ResourceRetainPolicy resourceRetainPolicy, ApplicationState currentState) {
    return switch (resourceRetainPolicy) {
      case Never -> false;
      case OnFailure -> currentState.getCurrentStateSummary().isFailure();
      default -> true;
    };
  }

  /**
   * Determines if force deletion should be enabled for the given SparkApplication.
   *
   * @param app The SparkApplication to check.
   * @return True if force deletion is enabled, false otherwise.
   */
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
