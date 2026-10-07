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

import static org.apache.spark.k8s.operator.Constants.APP_RESUMED_MESSAGE;
import static org.apache.spark.k8s.operator.Constants.APP_SUSPENDED_MESSAGE;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_APPLICATION_NAME;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_ROLE_DRIVER_VALUE;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_ROLE_EXECUTOR_VALUE;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_ROLE_NAME;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.SUSPEND_HOLD_REQUEUE_INTERVAL_SECONDS;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.TRIM_ATTEMPT_STATE_TRANSITION_HISTORY;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.completeAndDefaultRequeue;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.completeAndImmediateRequeue;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.completeAndRequeueAfter;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.proceed;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadUtils;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.reconciler.observers.AppDriverRunningObserver;
import org.apache.spark.k8s.operator.spec.ApplicationTimeoutConfig;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.utils.EventUtils;
import org.apache.spark.k8s.operator.utils.ReconcilerUtils;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;

/**
 * Suspends an application whose driver is requested by spec.suspend, and resumes it. An
 * application suspended before its driver is requested is held by {@link AppInitStep} instead.
 *
 * <p>An application enters Suspended before anything is released, and its driver is released only
 * in Suspended. Releasing it first would leave an application without its driver in a running state
 * when spec.suspend is cleared in the meantime, and the driver observers would fail it. A resumed
 * application leaves Suspended only after everything is released, and then starts a new attempt
 * from Submitted, where {@link AppInitStep} requests its driver and a new Kueue admission. Resuming
 * earlier would let the new attempt run on the admitted Workload of the suspended one, whose name
 * is fixed per application, or meet the driver resources of the suspended one, whose names stay
 * the same if spark.app.id is set. Like a restart, the new attempt gets the next attempt id, while
 * it does not count against the restart limits, see {@link
 * org.apache.spark.k8s.operator.status.ApplicationStatus#resume}.
 *
 * <p>It runs after the driver is observed, so that an attempt whose driver completed or failed
 * meanwhile ends as usual, rather than being suspended and run again from scratch on resume. The
 * observers read the informer cache, which may not show such a driver yet, so the driver is
 * observed once more on the API server before the application is suspended.
 */
@Slf4j
public final class AppSuspendStep extends AppReconcileStep {
  /** The states in which the driver is requested, and the attempt has not stopped. */
  private static final Set<ApplicationStateSummary> DRIVER_REQUESTED_STATES =
      EnumSet.range(
          ApplicationStateSummary.DriverRequested,
          ApplicationStateSummary.RunningWithBelowThresholdExecutors);

  /**
   * Reconciles an application whose driver is requested, or which is suspended, against
   * spec.suspend.
   *
   * @param context The SparkAppContext for the application.
   * @param statusRecorder The SparkAppStatusRecorder for recording status updates.
   * @return The ReconcileProgress indicating the next step.
   */
  @Override
  public ReconcileProgress reconcile(
      SparkAppContext context, SparkAppStatusRecorder statusRecorder) {
    SparkApplication app = context.getResource();
    boolean suspend = app.getSpec().isSuspend();
    ApplicationStateSummary summary = app.getStatus().getCurrentState().getCurrentStateSummary();
    if (DRIVER_REQUESTED_STATES.contains(summary) && suspend) {
      Optional<ApplicationState> termination;
      try {
        termination = observeDriverTermination(context);
      } catch (KubernetesClientException e) {
        log.warn("Failed to look up the driver of the application to suspend, will retry.", e);
        return completeAndDefaultRequeue();
      }
      if (termination.isPresent()) {
        return appendStateAndImmediateRequeue(context, statusRecorder, termination.get());
      }
      // A rejected update, e.g. of a state unknown to an older CRD, would be rejected again right
      // away, so it is retried with the default interval, not immediately.
      return statusRecorder.appendNewStateAndPersist(
              context,
              new ApplicationState(ApplicationStateSummary.Suspended, APP_SUSPENDED_MESSAGE))
          ? completeAndImmediateRequeue()
          : completeAndDefaultRequeue();
    }
    if (summary != ApplicationStateSummary.Suspended) {
      return proceed();
    }
    Optional<ReconcileProgress> waiting = releaseResources(context);
    if (waiting.isPresent()) {
      return waiting.get();
    }
    if (suspend) {
      return completeAndRequeueAfter(
          Duration.ofSeconds(SUSPEND_HOLD_REQUEUE_INTERVAL_SECONDS.getValue()));
    }
    return statusRecorder.persistStatus(
            context,
            app.getStatus()
                .resume(APP_RESUMED_MESSAGE, TRIM_ATTEMPT_STATE_TRANSITION_HISTORY.getValue()))
        ? completeAndImmediateRequeue()
        : completeAndDefaultRequeue();
  }

  /**
   * Observes whether the driver of the attempt completed or failed, as the API server knows it. A
   * driver which is being deleted, e.g. one of an earlier attempt, is not looked at, see {@link
   * SparkAppContext#isLiveDriverPod}.
   *
   * @param context The SparkAppContext for the application.
   * @return The state which ends the attempt if its driver completed or failed, or empty.
   * @throws KubernetesClientException if the driver pods cannot be listed.
   */
  private static Optional<ApplicationState> observeDriverTermination(SparkAppContext context) {
    SparkApplication app = context.getResource();
    AppDriverRunningObserver observer = new AppDriverRunningObserver();
    return listPods(context, LABEL_SPARK_ROLE_DRIVER_VALUE).stream()
        .filter(pod -> SparkAppContext.isLiveDriverPod(app, pod))
        .map(pod -> observer.observe(pod, app.getSpec(), app.getStatus()))
        .flatMap(Optional::stream)
        .findFirst();
  }

  /**
   * Releases the driver of a suspended application, and then its Kueue Workload. Like at the end
   * of an attempt, only the driver pod is deleted, whatever the resourceRetainPolicy is, and its
   * executors and the resources which it owns are deleted with it. The pods are listed on the API
   * server, so that every driver pod is deleted, including one which the informer cache lost, or
   * one besides a driver of an earlier attempt which is still terminating. Unlike at the end of an
   * attempt, the deletion is not waited for here, since it is observed by the pod informer, which
   * reconciles again.
   *
   * <p>The Workload is released only after the driver and executor pods are gone, since Kueue would
   * admit another workload into the quota which the terminating pods still occupy, even if the
   * queue label was removed after the admission. Every application waits for its pods, with or
   * without a Workload, since the resumed attempt starts without a restart backoff, and its driver
   * observers would take a driver of the suspended attempt which is still terminating for theirs.
   * A pod which is still terminating `forceTerminationGracePeriodMillis` after its grace period
   * ended, e.g. on a lost node, no longer holds the application, and a driver pod is force deleted
   * then, so that it does not keep its pod object. Like for a suspended SparkCluster, it is
   * measured per pod rather than since Suspended, so that a release which failed or was delayed
   * for that long still waits for the pods it deletes.
   *
   * <p>Everything here is idempotent, so pods that are not gone yet, or a release that failed, are
   * simply looked at again. A failure which is not expected to clear on its own is reported, since
   * Suspended is a steady state which would not tell it apart from waiting for the pods to go.
   *
   * @param context The SparkAppContext for the application.
   * @return Empty once everything is released, or the ReconcileProgress to retry with while pods
   *     remain or a release failed.
   */
  private Optional<ReconcileProgress> releaseResources(SparkAppContext context) {
    SparkApplication app = context.getResource();
    ApplicationTimeoutConfig timeoutConfig =
        app.getSpec().getApplicationTolerations().getApplicationTimeoutConfig();
    KubernetesClient client = context.getClient();
    try {
      List<Pod> pods =
          listPods(context, LABEL_SPARK_ROLE_DRIVER_VALUE, LABEL_SPARK_ROLE_EXECUTOR_VALUE);
      Instant now = Instant.now();
      boolean waiting = false;
      Instant nextDeadline = Instant.MAX;
      for (Pod pod : pods) {
        Instant deadline = getPodReleaseDeadline(pod, timeoutConfig);
        boolean holding = now.isBefore(deadline);
        boolean driver =
            LABEL_SPARK_ROLE_DRIVER_VALUE.equals(
                pod.getMetadata().getLabels().get(LABEL_SPARK_ROLE_NAME));
        if (driver && !holding) {
          client.resource(pod).withGracePeriod(0L).delete();
        } else if (driver && pod.getMetadata().getDeletionTimestamp() == null) {
          // A driver which is terminating already is not deleted again until it is force deleted
          client.resource(pod).withPropagationPolicy(DeletionPropagation.FOREGROUND).delete();
        }
        if (holding && deadline.isBefore(nextDeadline)) {
          nextDeadline = deadline;
        }
        waiting |= holding;
      }
      if (waiting) {
        // The deletion of each pod is observed by the pod informer, while the end of the wait for
        // a pod which may send no more events is observed by the requeue
        log.debug("Waiting for the driver and executor pods of the suspended app to be deleted.");
        ReconcileProgress defaultRequeue = completeAndDefaultRequeue();
        Duration remaining = Duration.between(now, nextDeadline);
        return Optional.of(
            remaining.compareTo(defaultRequeue.getRequeueAfterDuration()) < 0
                ? completeAndRequeueAfter(remaining)
                : defaultRequeue);
      }
      // A Workload admitted before the queue label was removed is released as well.
      KueueWorkloadUtils.deleteWorkloadOf(client, app);
    } catch (KubernetesClientException e) {
      log.warn("Failed to release the resources of the suspended application, will retry.", e);
      if (!ReconcilerUtils.isTransientError(e)) {
        EventUtils.warn(
            context.getEventRecorder(),
            EventUtils.REASON_SUSPEND_RELEASE_FAILED,
            "Failed to release the driver and executors or the Kueue Workload of the suspended "
                + "application, will retry. "
                + EventUtils.describe(e));
      }
      return Optional.of(completeAndDefaultRequeue());
    }
    return Optional.empty();
  }

  /**
   * Lists the pods of the application with the given Spark roles on the API server, including the
   * ones which are being deleted. Other pods which carry the application label do not count.
   *
   * @param context The SparkAppContext for the application.
   * @param roles The values of the Spark role label of the pods to list.
   * @return The pods of the application with the given roles.
   * @throws KubernetesClientException if the pods cannot be listed.
   */
  private static List<Pod> listPods(SparkAppContext context, String... roles) {
    SparkApplication app = context.getResource();
    return ReconcilerUtils.podsOf(
            context.getClient(),
            app.getMetadata().getNamespace(),
            LABEL_SPARK_APPLICATION_NAME,
            app.getMetadata().getName(),
            roles)
        .list()
        .getItems();
  }

  /**
   * Returns when a driver or executor pod stops holding a suspended application, and a driver pod
   * is force deleted.
   *
   * @param pod The driver or executor pod.
   * @param timeoutConfig The ApplicationTimeoutConfig of the application.
   * @return `forceTerminationGracePeriodMillis` after its deletionTimestamp, when its grace period
   *     ends, or Instant.MAX while it is not being deleted yet.
   */
  private static Instant getPodReleaseDeadline(Pod pod, ApplicationTimeoutConfig timeoutConfig) {
    String deletionTimestamp = pod.getMetadata().getDeletionTimestamp();
    return deletionTimestamp == null
        ? Instant.MAX
        : Instant.parse(deletionTimestamp)
            .plusMillis(timeoutConfig.getForceTerminationGracePeriodMillis());
  }
}
