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
import static org.apache.spark.k8s.operator.utils.Utils.basicLabelSecondaryToPrimaryMapper;
import static org.apache.spark.k8s.operator.utils.Utils.commonResourceLabelsStr;

import java.util.ArrayList;
import java.util.List;

import io.fabric8.kubernetes.api.model.Pod;
import io.javaoperatorsdk.operator.api.config.informer.InformerEventSourceConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Cleaner;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
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
public class SparkAppReconciler implements Reconciler<SparkApplication>, Cleaner<SparkApplication> {
  private final SparkAppSubmissionWorker submissionWorker;
  private final SparkAppStatusRecorder sparkAppStatusRecorder;
  private final SentinelManager<SparkApplication> sentinelManager;

  /**
   * Reconciles the state of a SparkApplication resource.
   *
   * @param sparkApplication The SparkApplication resource to reconcile.
   * @param context The reconciliation context.
   * @return An UpdateControl object indicating the result of the reconciliation.
   * @throws Exception if an error occurs during reconciliation.
   */
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

  /**
   * Updates the error status of a SparkApplication resource.
   *
   * @param sparkApplication The SparkApplication resource.
   * @param context The reconciliation context.
   * @param e The exception that occurred.
   * @return An ErrorStatusUpdateControl indicating no status update.
   */
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

  /**
   * Prepares the event sources for the SparkApplication reconciler.
   *
   * @param context The EventSourceContext.
   * @return A List of EventSource objects.
   */
  @Override
  public List<EventSource<?, SparkApplication>> prepareEventSources(
      EventSourceContext<SparkApplication> context) {
    EventSource podEventSource =
        new InformerEventSource<>(
            InformerEventSourceConfiguration.from(Pod.class, SparkApplication.class)
                .withSecondaryToPrimaryMapper(
                    basicLabelSecondaryToPrimaryMapper(LABEL_SPARK_APPLICATION_NAME))
                .withLabelSelector(commonResourceLabelsStr())
                .build(),
            context);
    return List.of(podEventSource);
  }

  /**
   * Returns a list of reconciliation steps based on the current state of the SparkApplication.
   *
   * @param app The SparkApplication resource.
   * @return A List of AppReconcileStep objects.
   */
  protected List<AppReconcileStep> getReconcileSteps(final SparkApplication app) {
    List<AppReconcileStep> steps = new ArrayList<>();
    steps.add(new AppValidateStep());
    steps.add(new AppCleanUpStep());
    switch (app.getStatus().getCurrentState().getCurrentStateSummary()) {
      case Submitted, ScheduledToRestart -> steps.add(new AppInitStep());
      case DriverRequested, DriverStarted -> {
        steps.add(
            new AppResourceObserveStep(
                List.of(new AppDriverStartObserver(), new AppDriverReadyObserver())));
        steps.add(new AppResourceObserveStep(List.of(new AppDriverRunningObserver())));
        steps.add(new AppResourceObserveStep(List.of(new AppDriverTimeoutObserver())));
      }
      case DriverReady,
          InitializedBelowThresholdExecutors,
          RunningHealthy,
          RunningWithBelowThresholdExecutors -> {
        steps.add(new AppRunningStep());
        steps.add(new AppResourceObserveStep(List.of(new AppDriverRunningObserver())));
        steps.add(new AppResourceObserveStep(List.of(new AppDriverTimeoutObserver())));
      }
      default -> steps.add(new AppUnknownStateStep());
    }
    return steps;
  }

  /**
   * Performs cleanup operations for a SparkApplication resource when it is marked for deletion.
   *
   * @param sparkApplication The SparkApplication resource that is marked for deletion.
   * @param context The context with which the operation is executed.
   * @return A DeleteControl object, with requeue if needed.
   */
  @Override
  public DeleteControl cleanup(
      SparkApplication sparkApplication, Context<SparkApplication> context) {
    LoggingUtils.TrackedMDC trackedMDC = new LoggingUtils.TrackedMDC();
    try {
      trackedMDC.set(sparkApplication);
      log.info("Cleaning up resources for SparkApp:" + sparkApplication.getMetadata().getName());
      SparkAppContext ctx = new SparkAppContext(sparkApplication, context, submissionWorker);
      List<AppReconcileStep> cleanupSteps = new ArrayList<>();
      cleanupSteps.add(new AppValidateStep());
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
      log.debug("Cleanup completed");
      trackedMDC.reset();
    }
    sparkAppStatusRecorder.removeCachedStatus(sparkApplication);
    return DeleteControl.defaultDelete();
  }
}
