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

import java.util.Collection;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.SortedMap;
import java.util.TreeMap;

import io.fabric8.kubernetes.api.model.Pod;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.config.SparkOperatorConf;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.status.ExecutorState;
import org.apache.spark.k8s.operator.utils.DeletedExecutorPods;
import org.apache.spark.k8s.operator.utils.ExecutorStateUtils;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;
import org.apache.spark.k8s.operator.utils.Utils;

/**
 * Records the number of active and ready executors of an application in {@code
 * status.activeExecutors} and {@code status.readyExecutors}, unless {@code executorCountsEnabled}
 * is off, and, for an application with Spark Dynamic Allocation, its highest executor ID in {@code
 * status.maxExecutorId} and, when {@code executorStateEnabled} is on, the state of each executor in
 * {@code status.executorState}. A client such as a batch scheduler can read the counts to size an
 * application and to tell whether its executors have started, and the rest to follow Dynamic
 * Allocation. The
 * executor pods come from the informer of the application's pods, so a change of an executor pod
 * reconciles the application and is recorded here. The final states of the executor pods which were
 * deleted since come from {@link DeletedExecutorPods}, and are forgotten there once they are
 * recorded. The status is written only when one of them changed.
 */
@Slf4j
@RequiredArgsConstructor
public final class AppExecutorStateStep extends AppReconcileStep {
  private final DeletedExecutorPods deletedExecutorPods;

  /**
   * Reconciles the executor states of the application from its executor pods.
   *
   * @param context The SparkAppContext for the application.
   * @param statusRecorder The SparkAppStatusRecorder for recording status updates.
   * @return The ReconcileProgress indicating the next step.
   */
  @Override
  public ReconcileProgress reconcile(
      SparkAppContext context, SparkAppStatusRecorder statusRecorder) {
    SparkApplication app = context.getResource();
    ResourceID appId = ResourceID.fromResource(app);
    Collection<DeletedExecutorPods.DeletedExecutor> deletedExecutors =
        deletedExecutorPods.get(appId);
    // A suspended application releases its executors, and its executor states start over when it
    // is suspended, so the pods which are being deleted are not recorded again.
    if (ApplicationStateSummary.Suspended
        == app.getStatus().getCurrentState().getCurrentStateSummary()) {
      deletedExecutorPods.remove(appId, deletedExecutors);
      return proceed();
    }
    // The executors of an application without Dynamic Allocation are fixed by its spec, so only
    // its executor counts are recorded, and the final states of its deleted executors are not kept
    boolean dynamicAllocation = Utils.isDynamicAllocationEnabled(app);
    if (!dynamicAllocation) {
      deletedExecutorPods.clear(appId);
    }
    Collection<Pod> executorPods = context.getExecutorsForApplication();
    // Without executor states, the map is left empty, which also clears one recorded before
    SortedMap<String, ExecutorState> executorState =
        dynamicAllocation && SparkOperatorConf.EXECUTOR_STATE_ENABLED.getValue()
            ? ExecutorStateUtils.observe(
                app,
                executorPods,
                deletedExecutors,
                SparkOperatorConf.MAX_TRACKED_EXECUTOR_PER_APP.getValue())
            : new TreeMap<>();
    ApplicationStatus status = app.getStatus();
    // Turning the counts off also clears the counts recorded before
    Integer activeExecutors =
        SparkOperatorConf.EXECUTOR_COUNTS_ENABLED.getValue()
            ? activeExecutors(status, executorPods, app)
            : null;
    // A ready executor is also active, so both are unset until the attempt has an executor pod
    Integer readyExecutors =
        activeExecutors == null ? null : ExecutorStateUtils.countReadyExecutors(app, executorPods);
    Long maxExecutorId = dynamicAllocation ? maxExecutorId(status, executorPods, app) : null;
    if (executorState.equals(status.getExecutorState())
        && Objects.equals(activeExecutors, status.getActiveExecutors())
        && Objects.equals(readyExecutors, status.getReadyExecutors())
        && Objects.equals(maxExecutorId, status.getMaxExecutorId())) {
      deletedExecutorPods.remove(appId, deletedExecutors);
      return proceed();
    }
    log.debug(
        "Executor states changed to {}, {} active, {} ready, highest executor ID {}",
        executorState,
        activeExecutors,
        readyExecutors,
        maxExecutorId);
    ReconcileProgress progress =
        attemptStatusUpdate(
            context,
            statusRecorder,
            status
                .withExecutorState(executorState)
                .withExecutorCounts(activeExecutors, readyExecutors, maxExecutorId),
            proceed());
    // Kept for the next reconciliation if the status could not be written
    if (!progress.isCompleted()) {
      deletedExecutorPods.remove(appId, deletedExecutors);
    }
    return progress;
  }

  /**
   * Returns the number of active executors of the current attempt, or null while the attempt never
   * had any, so that an application without executors is not written to.
   */
  private static Integer activeExecutors(
      final ApplicationStatus status,
      final Collection<Pod> executorPods,
      final SparkApplication app) {
    int active = ExecutorStateUtils.countActiveExecutors(app, executorPods);
    return active == 0 && status.getActiveExecutors() == null ? null : active;
  }

  /**
   * Returns the highest executor ID of the current attempt so far: the higher of the recorded one
   * and the highest of the executor pods, which never decreases when executors are removed.
   */
  private static Long maxExecutorId(
      final ApplicationStatus status,
      final Collection<Pod> executorPods,
      final SparkApplication app) {
    OptionalLong observed = ExecutorStateUtils.highestExecutorId(app, executorPods);
    Long recorded = status.getMaxExecutorId();
    if (observed.isEmpty()) {
      return recorded;
    }
    return recorded == null ? observed.getAsLong() : Math.max(recorded, observed.getAsLong());
  }
}
