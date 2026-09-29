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

import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.proceed;

import java.util.Optional;

import io.fabric8.kubernetes.api.model.Condition;

import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadUtils;
import org.apache.spark.k8s.operator.kueue.v1beta2.Workload;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.utils.EventUtils;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;

/**
 * Reports an eviction or a deactivation of the Kueue Workload of an application whose driver is
 * requested, which the operator does not act on yet. Unlike before the driver is requested, see
 * {@link AppInitStep}, the driver and executors keep running. The status does not show it, so the
 * event is published on every reconciliation while it lasts, like the pending event of a queued
 * application, and its message stays the same meanwhile, so that {@link
 * org.apache.spark.k8s.operator.config.SparkOperatorConf#KUBERNETES_EVENTS_MIN_INTERVAL_SECONDS}
 * paces the repeats.
 */
public final class AppKueueEvictionStep extends AppReconcileStep {
  /**
   * Publishes the event if needed and always proceeds, so that the driver is still observed.
   *
   * @param context The SparkAppContext for the application.
   * @param statusRecorder The SparkAppStatusRecorder for recording status updates.
   * @return The ReconcileProgress to proceed with.
   */
  @Override
  public ReconcileProgress reconcile(
      SparkAppContext context, SparkAppStatusRecorder statusRecorder) {
    Optional<Workload> workload = context.getCachedKueueWorkload();
    // Kueue marks a deactivated Workload as evicted too, but only once it sees the deactivation
    Optional<Condition> eviction = workload.flatMap(KueueWorkloadUtils::findEviction);
    boolean deactivated = workload.map(KueueWorkloadUtils::isDeactivated).orElse(false);
    if (eviction.isEmpty() && !deactivated) {
      return proceed();
    }
    String name = workload.get().getMetadata().getName();
    String cause =
        eviction.map(c -> " (" + c.getReason() + ": " + c.getMessage() + ")").orElse("");
    EventUtils.warn(
        context.getEventRecorder(),
        EventUtils.REASON_KUEUE_EVICTION_IGNORED,
        deactivated
            ? "Kueue Workload "
                + name
                + " is deactivated"
                + cause
                + ", which the operator does not act on once the driver is requested, so the "
                + "driver and executors keep running although Kueue no longer counts them against "
                + "its quota."
            : "Kueue evicted Workload "
                + name
                + cause
                + ", which the operator does not act on once the driver is requested, so the "
                + "driver and executors keep running and holding its quota until they are "
                + "released. Delete the application to release them.");
    return proceed();
  }
}
