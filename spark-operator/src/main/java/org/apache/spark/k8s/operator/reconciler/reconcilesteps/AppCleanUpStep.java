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

import static org.apache.spark.k8s.operator.config.SparkOperatorConf.KUEUE_ENABLED;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.config.SparkOperatorConf;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadFactory;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadUtils;
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
public final class AppCleanUpStep extends AppReconcileStep {
  /**
   * The stopping states whose driver pod reached a terminal phase, so that the Kueue Workload of a
   * retained application can be finished. An evicted driver is a `Failed` driver pod which is only
   * told apart by its reason, see `BaseAppDriverObserver`, so it occupies no capacity either. The
   * start timeouts and `SchedulingFailure` are absent because they can leave a live driver behind.
   */
  private static final Set<ApplicationStateSummary> FINISHED_KUEUE_STATES =
      Set.of(
          ApplicationStateSummary.Succeeded,
          ApplicationStateSummary.Failed,
          ApplicationStateSummary.DriverEvicted);

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
            && RestartPolicy.Never != tolerations.getRestartConfig().getRestartPolicy()) {
          stateUpdateMessage =
              "Application is configured to restart, resources created in current "
                  + "attempt would be force released.";
          log.warn(stateUpdateMessage);
        } else {
          ApplicationState terminationState =
              new ApplicationState(
                  ApplicationStateSummary.TerminatedWithoutReleaseResources,
                  "Application is terminated without releasing resources as configured.");
          finishKueueWorkloadOfRetainedApp(context, currentState, terminationState);
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
        // Nothing can be deleted without the spec, but the pods deleted by an earlier pass may
        // still be terminating.
        if (!releaseResources(context, application, List.of())) {
          return ReconcileProgress.completeAndRequeueAfter(
              Duration.ofMillis(
                  tolerations.getApplicationTimeoutConfig().getTerminationRequeuePeriodMillis()));
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
    if (!releaseResources(context, application, resourcesToRemove)) {
      // The application stays in its state until the pods are gone and the Workload is released
      return ReconcileProgress.completeAndRequeueAfter(
          Duration.ofMillis(
              tolerations.getApplicationTimeoutConfig().getTerminationRequeuePeriodMillis()));
    }
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

      // The resources have been released above. An application retaining them has returned
      // already, so it cannot terminate as `TerminatedWithoutReleaseResources` here even if the
      // retain policy applies and no more attempt is made.
      ApplicationStatus updatedStatus =
          currentStatus.terminateOrRestart(
              tolerations.getRestartConfig(),
              stateUpdateMessage,
              SparkOperatorConf.TRIM_ATTEMPT_STATE_TRANSITION_HISTORY.getValue());
      long requeueAfterMillis =
          tolerations.getApplicationTimeoutConfig().getTerminationRequeuePeriodMillis();
      if (ApplicationStateSummary.ScheduledToRestart ==
          updatedStatus.getCurrentState().getCurrentStateSummary()) {
        // Check if current state is a failure before restarting
        ApplicationStateSummary currentStateSummary =
            currentStatus.getCurrentState().getCurrentStateSummary();
        requeueAfterMillis =
            tolerations.getRestartConfig().getEffectiveRestartBackoffMillis(currentStateSummary);
      }
      return updateStatusAndRequeueAfter(
          context, statusRecorder, updatedStatus, Duration.ofMillis(requeueAfterMillis));
    }
  }

  /**
   * Deletes the given resources and then releases the Kueue Workload of the application, once its
   * driver and executor pods are gone. Kueue would admit another workload into the quota which the
   * terminating pods still occupy. The deletion of each pod is observed by the pod informer, which
   * reconciles again.
   *
   * <p>The Workload is released even if it was admitted before the queue label was removed. It is
   * released before a restart is scheduled, since the Workload name is fixed per application and
   * the next attempt would otherwise run on this admission. A restarted attempt is queued again
   * with a new Workload only if the label is present.
   *
   * <p>Every resource is asked to be deleted even if another one fails, so that a resource which
   * cannot be deleted, e.g. a ConfigMap the operator may no longer delete, does not keep the
   * driver running. A failed deletion is reported only after the Workload is released, unless pods
   * remain, in which case it is retried with them, so that such a resource does not hold the quota
   * once no pod uses it. A failed release of the Workload is retried as well, keeping the state of
   * the application, since the next attempt or the terminated application would otherwise keep
   * the admission. Like a suspended SparkCluster, a persistent failure, e.g. a revoked access to
   * the Workloads, holds the application until it is resolved.
   *
   * @param context The SparkAppContext for the application.
   * @param application The SparkApplication.
   * @param resources The resources to delete.
   * @return True if the Workload was released, false while the pods remain or the release failed.
   * @throws KubernetesClientException if a resource cannot be deleted and no pod remains.
   */
  private boolean releaseResources(
      final SparkAppContext context,
      final SparkApplication application,
      final List<HasMetadata> resources) {
    boolean forceDelete = enableForceDelete(application);
    KubernetesClientException deleteFailure = null;
    for (HasMetadata resource : resources) {
      try {
        ReconcilerUtils.deleteResourceIfExists(context.getClient(), resource, forceDelete);
      } catch (KubernetesClientException e) {
        if (deleteFailure == null) {
          deleteFailure = e;
        } else {
          deleteFailure.addSuppressed(e);
        }
      }
    }
    if (isWaitingForPods(context, application)) {
      if (deleteFailure != null) {
        log.warn("Failed to delete the resources of the application, will retry.", deleteFailure);
      }
      return false;
    }
    try {
      KueueWorkloadUtils.deleteWorkloadOf(context.getClient(), application);
    } catch (KubernetesClientException e) {
      log.warn("Failed to release the Kueue Workload of the application, will retry.", e);
      return false;
    }
    if (deleteFailure != null) {
      throw deleteFailure;
    }
    return true;
  }

  /**
   * Checks whether the application holds Kueue quota which its driver and executor pods still
   * occupy. An application without a Workload, e.g. one which was never queued or whose operator
   * runs without the Kueue integration, holds no quota, so its pods are not listed. The informer
   * cache keeps finding the Workload by the application label after the queue label was removed,
   * while the queue label covers a Workload which was created too recently to be cached.
   *
   * <p>The wait ends `forceTerminationGracePeriodMillis` after the clean up started, so that a pod
   * stuck in terminating, e.g. on a lost node, does not hold the application forever. A failure to
   * list the pods which may clear on its own is taken as pods remaining, and looked at again.
   *
   * @param context The SparkAppContext for the application.
   * @param application The SparkApplication.
   * @return True while the Workload has to wait for the pods.
   */
  private boolean isWaitingForPods(
      final SparkAppContext context, final SparkApplication application) {
    boolean holdsKueueQuota =
        context.getCachedKueueWorkload().isPresent()
            || KUEUE_ENABLED.getValue() && KueueWorkloadFactory.hasQueueName(application);
    if (!holdsKueueQuota || !Instant.now().isBefore(getWaitForPodsDeadline(application))) {
      return false;
    }
    try {
      if (!context.hasDriverOrExecutorPods()) {
        return false;
      }
    } catch (KubernetesClientException e) {
      if (!ReconcilerUtils.isRetryableError(e)) {
        throw e;
      }
      log.warn("Failed to list the driver and executor pods, will retry.", e);
    }
    log.debug("Waiting for the driver and executor pods to be deleted.");
    return true;
  }

  /**
   * Returns when the wait for the pods of the application ends. The clean up starts when the
   * application is deleted, when its retention expires, or else when the attempt stopped. Unlike
   * {@link #enableForceDelete}, which the application is deleted with right away once it has been
   * running for longer than `forceTerminationGracePeriodMillis`, the wait always gets that period.
   * A force deleted pod is removed from the API server at once, so the wait cannot observe it while
   * the kubelet stops its containers, which takes seconds.
   *
   * @param application The SparkApplication.
   * @return The Instant at which the Workload is released even if pods remain.
   */
  Instant getWaitForPodsDeadline(final SparkApplication application) {
    ApplicationTolerations tolerations = application.getSpec().getApplicationTolerations();
    ApplicationState currentState = application.getStatus().getCurrentState();
    Instant start;
    if (application.getMetadata().getDeletionTimestamp() != null) {
      start = Instant.parse(application.getMetadata().getDeletionTimestamp());
    } else if (ApplicationStateSummary.TerminatedWithoutReleaseResources
        == currentState.getCurrentStateSummary()) {
      start =
          Instant.parse(currentState.getLastTransitionTime())
              .plusMillis(tolerations.computeEffectiveRetainDurationMillis());
    } else {
      start = Instant.parse(currentState.getLastTransitionTime());
    }
    return start.plusMillis(
        tolerations.getApplicationTimeoutConfig().getForceTerminationGracePeriodMillis());
  }

  /**
   * Records the Kueue `Finished` condition on the Workload of an application which terminated
   * without releasing its resources. The Workload is retained with them, so Kueue releases its
   * quota on that condition instead of the deletion which the other paths rely on. The
   * application does not restart on this path, so no later attempt would find the finished
   * Workload. The Workload of an application whose queue label was removed is deleted instead.
   *
   * <p>Only an application in one of the {@link #FINISHED_KUEUE_STATES} is finished. The other
   * stopping states, the start timeouts and `SchedulingFailure`, leave a retained driver running,
   * and it would keep occupying the capacity which Kueue reclaims on the condition.
   *
   * @param context The SparkAppContext for the application.
   * @param currentState The state which the application terminated with.
   * @param terminationState The state which the application is updated to.
   */
  private void finishKueueWorkloadOfRetainedApp(
      final SparkAppContext context,
      final ApplicationState currentState,
      final ApplicationState terminationState) {
    ApplicationStateSummary stateSummary = currentState.getCurrentStateSummary();
    if (!FINISHED_KUEUE_STATES.contains(stateSummary)) {
      return;
    }
    SparkApplication application = context.getResource();
    if (KueueWorkloadFactory.hasQueueName(application)) {
      KueueWorkloadUtils.finishWorkload(
          context.getClient(),
          application,
          ApplicationStateSummary.Succeeded == stateSummary,
          terminationState.getMessage());
    } else {
      // A Workload admitted before the queue label was removed is deleted instead, since the
      // application left Kueue.
      KueueWorkloadUtils.releaseWorkload(context.getClient(), application);
    }
  }

  /**
   * Clears the status cache and indicates that reconciliation for the application is complete.
   *
   * @param application The SparkApplication to clear cache for.
   * @param statusRecorder The SparkAppStatusRecorder.
   * @return An Optional containing a ReconcileProgress to complete and not re-queue.
   */
  Optional<ReconcileProgress> clearCacheAndFinishReconcileForApplication(
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
  Optional<ReconcileProgress> checkEarlyExitForTerminatedApp(
      final KubernetesClient client,
      final SparkApplication application,
      final SparkAppStatusRecorder statusRecorder) {
    ApplicationStatus currentStatus = application.getStatus();
    ApplicationState currentState = currentStatus.getCurrentState();
    ApplicationTolerations tolerations = application.getSpec().getApplicationTolerations();
    Instant now = Instant.now();
    if (ApplicationStateSummary.ResourceReleased == currentState.getCurrentStateSummary()) {
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
    if (ApplicationStateSummary.TerminatedWithoutReleaseResources ==
        currentState.getCurrentStateSummary()) {
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
  boolean isOnDemandCleanup() {
    return onDemandCleanUpReason != null;
  }

  /**
   * Returns the last observed state of the application before it terminated.
   *
   * @param status The current ApplicationStatus.
   * @return The ApplicationState before termination, or the current state if not terminated.
   */
  ApplicationState getLastObservedStateBeforeTermination(final ApplicationStatus status) {
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
  boolean isReleasingResourcesForSchedulingFailureAttempt(final ApplicationStatus status) {
    ApplicationState lastObservedState = getLastObservedStateBeforeTermination(status);
    return ApplicationStateSummary.SchedulingFailure == lastObservedState.getCurrentStateSummary();
  }

  /**
   * Determines whether to retain or release resources based on the resource retention policy and
   * current application state.
   *
   * @param resourceRetainPolicy The ResourceRetainPolicy configured for the application.
   * @param currentState The current ApplicationState.
   * @return True if resources should be retained, false if they should be released.
   */
  boolean retainReleaseResourceForPolicyAndState(
      ResourceRetainPolicy resourceRetainPolicy, ApplicationState currentState) {
    return switch (resourceRetainPolicy) {
      case Always -> true;
      case Never -> false;
      case OnFailure -> currentState.getCurrentStateSummary().isFailure();
    };
  }

  /**
   * Determines if force deletion should be enabled for the given SparkApplication.
   *
   * @param app The SparkApplication to check.
   * @return True if force deletion is enabled, false otherwise.
   */
  boolean enableForceDelete(SparkApplication app) {
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
