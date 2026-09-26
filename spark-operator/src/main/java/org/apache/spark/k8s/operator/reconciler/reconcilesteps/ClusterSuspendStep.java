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

import static org.apache.spark.k8s.operator.Constants.CLUSTER_EVICTED_MESSAGE;
import static org.apache.spark.k8s.operator.Constants.CLUSTER_REQUEUED_MESSAGE;
import static org.apache.spark.k8s.operator.Constants.CLUSTER_RESUMED_MESSAGE;
import static org.apache.spark.k8s.operator.Constants.CLUSTER_SUSPENDED_MESSAGE;
import static org.apache.spark.k8s.operator.Constants.CLUSTER_SUSPENDED_WITH_STUCK_PODS_MESSAGE;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_CLUSTER_NAME;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_ROLE_MASTER_VALUE;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_ROLE_WORKER_VALUE;
import static org.apache.spark.k8s.operator.SparkClusterResourceSpec.getMasterStatefulSetName;
import static org.apache.spark.k8s.operator.SparkClusterResourceSpec.getWorkerHorizontalPodAutoscalerName;
import static org.apache.spark.k8s.operator.SparkClusterResourceSpec.getWorkerPodDisruptionBudgetName;
import static org.apache.spark.k8s.operator.SparkClusterResourceSpec.getWorkerStatefulSetName;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.SUSPEND_HOLD_REQUEUE_INTERVAL_SECONDS;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.TRIM_ATTEMPT_STATE_TRANSITION_HISTORY;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.completeAndDefaultRequeue;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.completeAndRequeueAfter;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.proceed;

import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.Condition;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.context.SparkClusterContext;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadUtils;
import org.apache.spark.k8s.operator.kueue.v1beta2.Workload;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.status.ClusterState;
import org.apache.spark.k8s.operator.status.ClusterStateSummary;
import org.apache.spark.k8s.operator.utils.EventUtils;
import org.apache.spark.k8s.operator.utils.ReconcilerUtils;
import org.apache.spark.k8s.operator.utils.SparkClusterStatusRecorder;

/**
 * Suspends a running cluster by spec.suspend, and resumes it. A cluster runs until it is deleted,
 * so suspending it is the only way to give its quota back without deleting it.
 *
 * <p>A running cluster enters Suspended before anything is released, and the master and workers
 * are released only in Suspended. Releasing them first would leave a cluster without them in
 * RunningHealthy when spec.suspend is cleared in the meantime, and nothing recreates them there.
 * A resumed cluster leaves Suspended only after everything is released, and then starts over from
 * Submitted, where {@link ClusterInitStep} applies all of its resources again and requests a new
 * Kueue admission. Resuming earlier would let it keep the StatefulSets or the admitted Workload of
 * the previous spec, so that a spec changed in the meantime would run on the old admission.
 *
 * <p>A running cluster whose Kueue Workload is evicted, e.g. to preempt it, is suspended the same
 * way without spec.suspend, so that it is queued again once everything is released.
 */
@Slf4j
public final class ClusterSuspendStep extends ClusterReconcileStep {
  /**
   * How long a suspended cluster waits for a terminating master or worker pod past the end of its
   * grace period, i.e. its deletionTimestamp, before it releases its Kueue Workload anyway. It is
   * the same as the default `forceTerminationGracePeriodMillis` of an application. A pod on a lost
   * node stays terminating until the node is deleted, which could otherwise hold the quota forever.
   */
  private static final Duration POD_RELEASE_TIMEOUT = Duration.ofMinutes(5);

  /**
   * Reconciles a running or suspended cluster against spec.suspend and the eviction of its Kueue
   * Workload.
   *
   * @param context The SparkClusterContext for the cluster.
   * @param statusRecorder The SparkClusterStatusRecorder for recording status updates.
   * @return The ReconcileProgress indicating the next step.
   */
  @Override
  public ReconcileProgress reconcile(
      SparkClusterContext context, SparkClusterStatusRecorder statusRecorder) {
    SparkCluster cluster = context.getResource();
    boolean suspend = cluster.getSpec().isSuspend();
    ClusterStateSummary summary = cluster.getStatus().getCurrentState().getCurrentStateSummary();
    if (summary == ClusterStateSummary.RunningHealthy && suspend) {
      return appendStateAndImmediateRequeue(
          context,
          statusRecorder,
          new ClusterState(ClusterStateSummary.Suspended, CLUSTER_SUSPENDED_MESSAGE));
    }
    if (summary == ClusterStateSummary.RunningHealthy) {
      return suspendOnKueueEviction(context, statusRecorder);
    }
    if (summary == ClusterStateSummary.Suspended) {
      boolean evicted = isSuspendedByEviction(cluster);
      if (evicted && suspend) {
        // The cluster is held by spec.suspend from now on, so that its status no longer says that
        // it is queued again, and a later resume is reported as such.
        return appendStateAndImmediateRequeue(
            context,
            statusRecorder,
            new ClusterState(ClusterStateSummary.Suspended, CLUSTER_SUSPENDED_MESSAGE));
      }
      Optional<Duration> keepWorkload = suspend ? Optional.empty() : keepKueueWorkload(context);
      Optional<ReconcileProgress> waiting =
          releaseResources(context, statusRecorder, keepWorkload.isEmpty());
      if (waiting.isPresent()) {
        return waiting.get();
      }
      if (keepWorkload.isPresent()) {
        return completeAndRequeueAfter(keepWorkload.get());
      }
      if (!suspend) {
        String message = evicted ? CLUSTER_REQUEUED_MESSAGE : CLUSTER_RESUMED_MESSAGE;
        // Like a restarted application attempt, the resumed cluster drops the history of the run
        // which is over, so that suspending and resuming it again and again keeps it bounded.
        return updateStatusAndRequeueAfter(
            context,
            statusRecorder,
            cluster
                .getStatus()
                .appendNewState(
                    new ClusterState(ClusterStateSummary.Submitted, message),
                    TRIM_ATTEMPT_STATE_TRANSITION_HISTORY.getValue()),
            Duration.ZERO);
      }
      return completeAndRequeueAfter(
          Duration.ofSeconds(SUSPEND_HOLD_REQUEUE_INTERVAL_SECONDS.getValue()));
    }
    return proceed();
  }

  /**
   * Suspends a running cluster whose Kueue Workload is evicted, so that its master and workers and
   * then its Workload are released like on spec.suspend. Kueue keeps the quota of an evicted
   * Workload until then. As spec.suspend is not set, the cluster is queued again once everything is
   * released, see {@link #keepKueueWorkload} for when that waits. An eviction which {@link
   * KueueWorkloadUtils#isKeptOnEviction} is only reported, and the cluster keeps running.
   *
   * @param context The SparkClusterContext for the cluster.
   * @param statusRecorder The SparkClusterStatusRecorder for recording status updates.
   * @return The ReconcileProgress indicating the next step.
   */
  private ReconcileProgress suspendOnKueueEviction(
      SparkClusterContext context, SparkClusterStatusRecorder statusRecorder) {
    Optional<Workload> cached = context.getCachedKueueWorkload();
    Optional<Condition> eviction = cached.flatMap(KueueWorkloadUtils::findEviction);
    if (eviction.isEmpty()) {
      return proceed();
    }
    Workload workload = cached.get();
    String cause = eviction.get().getReason() + ": " + eviction.get().getMessage();
    if (KueueWorkloadUtils.isKeptOnEviction(workload)) {
      EventUtils.warn(
          context.getEventRecorder(),
          EventUtils.REASON_KUEUE_EVICTION_IGNORED,
          "Kueue evicted Workload "
              + workload.getMetadata().getName()
              + " ("
              + cause
              + "), which the operator does not act on, so the master and workers keep running "
              + "and holding its quota. Set spec.suspend to true or delete the cluster to release "
              + "them.");
      return proceed();
    }
    log.info("Kueue evicted the Workload of the cluster ({}), suspending it.", cause);
    return appendStateAndImmediateRequeue(
        context,
        statusRecorder,
        new ClusterState(ClusterStateSummary.Suspended, CLUSTER_EVICTED_MESSAGE + " " + cause));
  }

  /**
   * Checks whether the cluster is suspended by the eviction of its Kueue Workload rather than by
   * spec.suspend. The Suspended state which names its stuck pods, if any, follows the one which
   * says why it is suspended.
   *
   * @param cluster The suspended SparkCluster.
   * @return True if the cluster was suspended by an eviction and not held by spec.suspend since.
   */
  private static boolean isSuspendedByEviction(SparkCluster cluster) {
    NavigableMap<Long, ClusterState> history =
        (NavigableMap<Long, ClusterState>) cluster.getStatus().getStateTransitionHistory();
    for (ClusterState state : history.descendingMap().values()) {
      String message = state.getMessage();
      if (state.getCurrentStateSummary() != ClusterStateSummary.Suspended
          || CLUSTER_SUSPENDED_MESSAGE.equals(message)) {
        return false;
      }
      if (message != null && message.startsWith(CLUSTER_EVICTED_MESSAGE)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns how long the Kueue Workload of a cluster which is suspended without spec.suspend is
   * kept after its master and workers are released, or empty to release it right away and queue
   * the cluster again. A deactivated Workload is kept until it is reactivated, which the Workload
   * informer reconciles right away: Kueue no longer counts its quota, and a new Workload of the
   * cluster would come back active. An evicted Workload is kept until the requeue backoff which
   * Kueue records on it elapses, see {@link KueueWorkloadUtils#remainingRequeueBackoff}.
   *
   * @param context The SparkClusterContext for the cluster.
   * @return The requeue interval while the Workload is kept, or empty to release it.
   */
  private static Optional<Duration> keepKueueWorkload(SparkClusterContext context) {
    Optional<Workload> workload = context.getCachedKueueWorkload();
    if (workload.isEmpty()) {
      return Optional.empty();
    }
    if (KueueWorkloadUtils.isDeactivated(workload.get())) {
      return Optional.of(Duration.ofSeconds(SUSPEND_HOLD_REQUEUE_INTERVAL_SECONDS.getValue()));
    }
    Duration backoff = KueueWorkloadUtils.remainingRequeueBackoff(workload.get());
    return backoff.isZero() ? Optional.empty() : Optional.of(backoff);
  }

  /**
   * Releases the master and workers of a suspended cluster, and then its Kueue Workload. The
   * Services and the NetworkPolicy are kept, since they hold no quota and are applied again on
   * resume. Everything is deleted by name rather than from the current spec. The spec may no longer
   * ask for the HorizontalPodAutoscaler or the PodDisruptionBudget of the workers, and it may not
   * even build after being edited while suspended, which would otherwise hold the pods and the
   * quota until the spec is fixed.
   *
   * <p>The Workload is released only after the master and worker pods are gone, since Kueue would
   * admit another workload into the quota which the terminating pods still occupy. A pod which is
   * still terminating {@link #POD_RELEASE_TIMEOUT} after its grace period ended no longer holds the
   * Workload, and the wait is requeued to end right then, since such a pod may send no more events.
   * It is measured per pod rather than since Suspended, so that a release which failed or was
   * delayed for that long still waits for the pods it deletes. The state of the cluster names such
   * pods, and a resumed cluster still waits for them, since they keep the names of the master and
   * workers it would create again. Their deletion is observed by the pod informer, so they are
   * looked at again only as often as a suspended cluster otherwise. Unlike the pods of an
   * application, which are force deleted past `forceTerminationGracePeriodMillis`, such a pod is
   * not force deleted, since its container may still run on a partitioned node while a pod of the
   * same StatefulSet identity is created again. Other pods which carry the cluster label, e.g. to
   * reach the workers through their NetworkPolicy, do not count. Everything here is idempotent, so
   * pods that are not gone yet, or a release that failed, are simply looked at again. A failure
   * which is not expected to clear on its own is reported, since Suspended is a steady state which
   * would not tell it apart from waiting for the pods to go.
   *
   * @param context The SparkClusterContext for the cluster.
   * @param statusRecorder The SparkClusterStatusRecorder for recording status updates.
   * @param releaseWorkload Whether the Kueue Workload is released as well.
   * @return Empty once everything is released, or the ReconcileProgress to retry with while pods
   *     remain or a release failed.
   */
  private Optional<ReconcileProgress> releaseResources(
      SparkClusterContext context,
      SparkClusterStatusRecorder statusRecorder,
      boolean releaseWorkload) {
    SparkCluster cluster = context.getResource();
    String namespace = cluster.getMetadata().getNamespace();
    String name = cluster.getMetadata().getName();
    KubernetesClient client = context.getClient();
    try {
      client
          .apps()
          .statefulSets()
          .inNamespace(namespace)
          .withName(getWorkerStatefulSetName(name))
          .delete();
      client
          .apps()
          .statefulSets()
          .inNamespace(namespace)
          .withName(getMasterStatefulSetName(name))
          .delete();
      client
          .autoscaling()
          .v2()
          .horizontalPodAutoscalers()
          .inNamespace(namespace)
          .withName(getWorkerHorizontalPodAutoscalerName(name))
          .delete();
      client
          .policy()
          .v1()
          .podDisruptionBudget()
          .inNamespace(namespace)
          .withName(getWorkerPodDisruptionBudgetName(name))
          .delete();
      List<Pod> pods =
          ReconcilerUtils.podsOf(
                  client,
                  namespace,
                  LABEL_SPARK_CLUSTER_NAME,
                  name,
                  LABEL_SPARK_ROLE_MASTER_VALUE,
                  LABEL_SPARK_ROLE_WORKER_VALUE)
              .list()
              .getItems();
      Instant now = Instant.now();
      Instant podReleaseDeadline =
          pods.stream()
              .map(ClusterSuspendStep::getPodReleaseDeadline)
              .max(Comparator.naturalOrder())
              .orElse(now);
      if (now.isBefore(podReleaseDeadline)) {
        // The deletion of each pod is observed by the pod informer, which reconciles again, while
        // the timeout of a pod which may send no more events is observed by the requeue.
        log.debug("Waiting for the pods of the suspended cluster to be deleted.");
        ReconcileProgress defaultRequeue = completeAndDefaultRequeue();
        Duration remaining = Duration.between(now, podReleaseDeadline);
        return Optional.of(
            remaining.compareTo(defaultRequeue.getRequeueAfterDuration()) < 0
                ? completeAndRequeueAfter(remaining)
                : defaultRequeue);
      }
      if (releaseWorkload) {
        // A Workload admitted before the queue label was removed is released as well.
        KueueWorkloadUtils.deleteWorkloadOf(client, cluster);
      }
      if (!pods.isEmpty()) {
        log.debug("Waiting for the stuck pods of the suspended cluster to be deleted.");
        // This may last until a lost node is deleted, so the state says why, once per set of pods
        String message =
            String.format(
                CLUSTER_SUSPENDED_WITH_STUCK_PODS_MESSAGE,
                pods.stream()
                    .map(pod -> pod.getMetadata().getName())
                    .sorted()
                    .collect(Collectors.joining(", ")));
        Duration holdInterval =
            Duration.ofSeconds(SUSPEND_HOLD_REQUEUE_INTERVAL_SECONDS.getValue());
        if (message.equals(cluster.getStatus().getCurrentState().getMessage())) {
          return Optional.of(completeAndRequeueAfter(holdInterval));
        }
        return Optional.of(
            appendStateAndRequeueAfter(
                context,
                statusRecorder,
                new ClusterState(ClusterStateSummary.Suspended, message),
                holdInterval));
      }
    } catch (KubernetesClientException e) {
      log.warn("Failed to release the resources of the suspended cluster, will retry.", e);
      if (!ReconcilerUtils.isTransientError(e)) {
        EventUtils.warn(
            context.getEventRecorder(),
            EventUtils.REASON_SUSPEND_RELEASE_FAILED,
            "Failed to release the master and workers or the Kueue Workload of the suspended "
                + "cluster, will retry. "
                + EventUtils.describe(e));
      }
      return Optional.of(completeAndDefaultRequeue());
    }
    return Optional.empty();
  }

  /**
   * Returns when a master or worker pod stops holding the Kueue Workload of a suspended cluster.
   *
   * @param pod The master or worker pod.
   * @return {@link #POD_RELEASE_TIMEOUT} after its deletionTimestamp, when its grace period
   *     ends, or Instant.MAX while it is not being deleted yet.
   */
  private static Instant getPodReleaseDeadline(Pod pod) {
    String deletionTimestamp = pod.getMetadata().getDeletionTimestamp();
    return deletionTimestamp == null
        ? Instant.MAX
        : Instant.parse(deletionTimestamp).plus(POD_RELEASE_TIMEOUT);
  }
}
