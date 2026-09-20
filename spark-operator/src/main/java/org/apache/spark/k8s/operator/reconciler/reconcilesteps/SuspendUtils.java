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

import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.completeAndRequeueAfter;

import java.time.Duration;

import io.fabric8.kubernetes.api.model.HasMetadata;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.context.BaseContext;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadFactory;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadUtils;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.utils.EventUtils;

/** Utilities to hold the resources of a suspended SparkApplication or SparkCluster. */
@Slf4j
final class SuspendUtils {

  /**
   * Interval at which a suspended resource is reconciled, so that its event is republished well
   * within the one hour that the API server retains events by default. It is deliberately much
   * coarser than the steady-state reconcile interval: a suspended resource has nothing to observe,
   * while each republish costs a read and a write on the API server for as long as the hold lasts.
   * Nothing waits for this interval, since clearing spec.suspend arrives as a watch event that
   * reconciles the resource right away.
   */
  static final Duration SUSPEND_HOLD_REQUEUE_INTERVAL = Duration.ofMinutes(30);

  private SuspendUtils() {}

  /**
   * Holds the resources of a resource suspended by {@code spec.suspend} and reports the progress
   * to return. A resource suspended while queued releases its Kueue Workload first, so that it
   * does not keep holding the quota, and its event then says so, since the pending event it was
   * queued with outlives the Workload. Callers keep their own guard for resources requested
   * before, which must complete their initialization instead of being held.
   *
   * <p>Like the Kueue pending event, the event is republished while the hold lasts rather than
   * once when it starts. The event sink keys the Event on the reason, so a repeat bumps the count
   * of the one Event instead of creating another and refreshes it, so that the hold stays visible
   * past the event retention of the API server. A suspended first attempt has no persisted status
   * to fall back on, since its initial Submitted state is never written. An application held
   * later, in ScheduledToRestart, does have the status of the previous attempt, but that status
   * does not say that the next attempt is withheld by spec.suspend, so it gets the same event.
   *
   * <p>Unlike the Kueue hold, which ends when quota arrives, this one ends only when a user clears
   * spec.suspend, so the republishing is paced by {@link #SUSPEND_HOLD_REQUEUE_INTERVAL} rather
   * than by the steady-state reconcile interval.
   *
   * @param context The context of the suspended resource.
   * @param requested The resources held until the resource is resumed, as named in the event and
   *     the log, e.g. {@code "driver"}.
   * @return The progress to return while the resource is suspended, requeued after {@link
   *     #SUSPEND_HOLD_REQUEUE_INTERVAL}.
   */
  static ReconcileProgress holdForSuspend(final BaseContext<?> context, final String requested) {
    HasMetadata resource = context.getResource();
    log.debug("{} is suspended, {} would not be requested.", resource.getKind(), requested);
    String message =
        "The "
            + resource.getKind()
            + " is suspended by spec.suspend, "
            + requested
            + " would not be requested. Set spec.suspend to false to resume it.";
    if (KueueWorkloadFactory.hasQueueName(resource)) {
      KueueWorkloadUtils.releaseWorkload(context.getClient(), resource);
      // The pending event of a resource that was queued before stays until the API server drops
      // it, and the operator may not delete events, so say that it no longer applies rather than
      // leaving a contradicting pair behind.
      message +=
          " It holds no Kueue Workload while suspended, so an earlier "
              + EventUtils.REASON_KUEUE_ADMISSION_PENDING
              + " event no longer applies.";
    }
    EventUtils.normal(context.getEventRecorder(), EventUtils.REASON_SUSPEND_HELD, message);
    return completeAndRequeueAfter(SUSPEND_HOLD_REQUEUE_INTERVAL);
  }
}
