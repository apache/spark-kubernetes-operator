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

package org.apache.spark.k8s.operator.reconciler;

import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_APPLICATION_NAME;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.completeAndDefaultRequeue;
import static org.apache.spark.k8s.operator.utils.Utils.commonResourceLabelsStr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.Pod;
import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusHandler;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.Mappers;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.SparkAppSubmissionWorker;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.metrics.healthcheck.SentinelManager;
import org.apache.spark.k8s.operator.reconciler.observers.AppDriverReadyObserver;
import org.apache.spark.k8s.operator.reconciler.observers.AppDriverRunningObserver;
import org.apache.spark.k8s.operator.reconciler.observers.AppDriverStartObserver;
import org.apache.spark.k8s.operator.reconciler.observers.AppDriverTimeoutObserver;
import org.apache.spark.k8s.operator.reconciler.reconcilesteps.AppCleanUpStep;
import org.apache.spark.k8s.operator.reconciler.reconcilesteps.AppInitStep;
import org.apache.spark.k8s.operator.reconciler.reconcilesteps.AppReconcileStep;
import org.apache.spark.k8s.operator.reconciler.reconcilesteps.AppResourceObserveStep;
import org.apache.spark.k8s.operator.reconciler.reconcilesteps.AppRunningStep;
import org.apache.spark.k8s.operator.reconciler.reconcilesteps.AppTerminatedStep;
import org.apache.spark.k8s.operator.reconciler.reconcilesteps.AppUnknownStateStep;
import org.apache.spark.k8s.operator.reconciler.reconcilesteps.AppValidateStep;
import org.apache.spark.k8s.operator.utils.LoggingUtils;
import org.apache.spark.k8s.operator.utils.ReconcilerUtils;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;
import org.apache.spark.k8s.operator.utils.SparkAppStatusUtils;

/**
 * Reconciler for Spark Application. Performs sanity check on the app, identify the reconcile steps
 * based on App status and execute the steps.
 */
@ControllerConfiguration
@Slf4j
@RequiredArgsConstructor
public class SparkAppReconciler
    implements Reconciler<SparkApplication>,
        ErrorStatusHandler<SparkApplication>,
        EventSourceInitializer<SparkApplication>,
        Cleaner<SparkApplication> {
  private final SparkAppSubmissionWorker submissionWorker;
  private final SparkAppStatusRecorder sparkAppStatusRecorder;
  private final SentinelManager<SparkApplication> sentinelManager;

  @Override
  public UpdateControl<SparkApplication> reconcile(
      SparkApplication sparkApplication, Context<SparkApplication> context) throws Exception {
    LoggingUtils.TrackedMDC trackedMDC = new LoggingUtils.TrackedMDC();
    try {
      trackedMDC.set(sparkApplication);
      if (sentinelManager.handleSentinelResourceReconciliation(
          sparkApplication, context.getClient())) {
        return UpdateControl.noUpdate();
      }
      log.debug("Start application reconciliation.");
      sparkAppStatusRecorder.updateStatusFromCache(sparkApplication);
      SparkAppContext ctx = new SparkAppContext(sparkApplication, context, submissionWorker);
      List<AppReconcileStep> reconcileSteps = getReconcileSteps(sparkApplication);
      for (AppReconcileStep step : reconcileSteps) {
        ReconcileProgress progress = step.reconcile(ctx, sparkAppStatusRecorder);
        if (progress.isCompleted()) {
          return ReconcilerUtils.toUpdateControl(sparkApplication, progress);
        }
      }
      return ReconcilerUtils.toUpdateControl(sparkApplication, completeAndDefaultRequeue());

    } finally {
      log.debug("Reconciliation completed.");
      trackedMDC.reset();
    }
  }

  @Override
  public ErrorStatusUpdateControl<SparkApplication> updateErrorStatus(
      SparkApplication sparkApplication, Context<SparkApplication> context, Exception e) {
    LoggingUtils.TrackedMDC trackedMDC = new LoggingUtils.TrackedMDC();
    try {
      trackedMDC.set(sparkApplication);
      context
          .getRetryInfo()
          .ifPresent(
              retryInfo -> {
                if (log.isErrorEnabled()) {
                  log.error(
                      "Failed attempt: {}, last attempt: {}",
                      retryInfo.getAttemptCount(),
                      retryInfo.isLastAttempt());
                }
              });
      return ErrorStatusUpdateControl.noStatusUpdate();
    } finally {
      trackedMDC.reset();
    }
  }

  @Override
  public Map<String, EventSource> prepareEventSources(
      EventSourceContext<SparkApplication> context) {
    EventSource podEventSource =
        new InformerEventSource<>(
            InformerConfiguration.from(Pod.class, context)
                .withSecondaryToPrimaryMapper(Mappers.fromLabel(LABEL_SPARK_APPLICATION_NAME))
                .withLabelSelector(commonResourceLabelsStr())
                .build(),
            context);
    return EventSourceInitializer.nameEventSources(podEventSource);
  }

  protected List<AppReconcileStep> getReconcileSteps(final SparkApplication app) {
    List<AppReconcileStep> steps = new ArrayList<>();
    steps.add(new AppValidateStep());
    steps.add(new AppTerminatedStep());
    switch (app.getStatus().getCurrentState().getCurrentStateSummary()) {
      case Submitted, ScheduledToRestart -> steps.add(new AppInitStep());
      case DriverRequested, DriverStarted -> {
        steps.add(
            new AppResourceObserveStep(
                List.of(new AppDriverStartObserver(), new AppDriverReadyObserver())));
        steps.add(
            new AppResourceObserveStep(Collections.singletonList(new AppDriverRunningObserver())));
        steps.add(
            new AppResourceObserveStep(Collections.singletonList(new AppDriverTimeoutObserver())));
      }
      case DriverReady,
          InitializedBelowThresholdExecutors,
          RunningHealthy,
          RunningWithBelowThresholdExecutors -> {
        steps.add(new AppRunningStep());
        steps.add(
            new AppResourceObserveStep(Collections.singletonList(new AppDriverRunningObserver())));
        steps.add(
            new AppResourceObserveStep(Collections.singletonList(new AppDriverTimeoutObserver())));
      }
      case DriverReadyTimedOut,
              DriverStartTimedOut,
              ExecutorsStartTimedOut,
              Succeeded,
              DriverEvicted,
              Failed,
              SchedulingFailure ->
          steps.add(new AppCleanUpStep());
      default -> steps.add(new AppUnknownStateStep());
    }
    return steps;
  }

  /**
   * Best-effort graceful termination upon delete.
   *
   * @param sparkApplication the resource that is marked for deletion
   * @param context the context with which the operation is executed
   * @return DeleteControl, with requeue if needed
   */
  @Override
  public DeleteControl cleanup(
      SparkApplication sparkApplication, Context<SparkApplication> context) {
    LoggingUtils.TrackedMDC trackedMDC = new LoggingUtils.TrackedMDC();
    try {
      trackedMDC.set(sparkApplication);
      log.info("Cleaning up resources for SparkApp.");
      SparkAppContext ctx = new SparkAppContext(sparkApplication, context, submissionWorker);
      List<AppReconcileStep> cleanupSteps = new ArrayList<>();
      cleanupSteps.add(new AppValidateStep());
      cleanupSteps.add(new AppTerminatedStep());
      cleanupSteps.add(new AppCleanUpStep(SparkAppStatusUtils::appCancelled));
      for (AppReconcileStep step : cleanupSteps) {
        ReconcileProgress progress = step.reconcile(ctx, sparkAppStatusRecorder);
        if (progress.isCompleted()) {
          if (progress.isRequeue()) {
            return DeleteControl.noFinalizerRemoval()
                .rescheduleAfter(progress.getRequeueAfterDuration());
          } else {
            break;
          }
        }
      }
    } finally {
      log.info("Cleanup completed");
      trackedMDC.reset();
    }
    sparkAppStatusRecorder.removeCachedStatus(sparkApplication);
    return DeleteControl.defaultDelete();
  }
}
