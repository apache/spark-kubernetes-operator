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

import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.SparkClusterSubmissionWorker;
import org.apache.spark.k8s.operator.context.SparkClusterContext;
import org.apache.spark.k8s.operator.metrics.healthcheck.SentinelManager;
import org.apache.spark.k8s.operator.reconciler.reconcilesteps.*;
import org.apache.spark.k8s.operator.utils.LoggingUtils;
import org.apache.spark.k8s.operator.utils.ReconcilerUtils;
import org.apache.spark.k8s.operator.utils.SparkClusterStatusRecorder;

/**
 * Reconciler for Spark Cluster. Performs sanity check on the cluster, identify the reconcile steps
 * based on status and execute the steps.
 */
@ControllerConfiguration
@Slf4j
@RequiredArgsConstructor
public class SparkClusterReconciler implements Reconciler<SparkCluster>, Cleaner<SparkCluster> {
  private final SparkClusterSubmissionWorker submissionWorker;
  private final SparkClusterStatusRecorder sparkClusterStatusRecorder;
  private final SentinelManager<SparkCluster> sentinelManager;

  @Override
  public UpdateControl<SparkCluster> reconcile(
      SparkCluster sparkCluster, Context<SparkCluster> context) throws Exception {
    LoggingUtils.TrackedMDC trackedMDC = new LoggingUtils.TrackedMDC();
    try {
      if (sentinelManager.handleSentinelResourceReconciliation(sparkCluster, context.getClient())) {
        return UpdateControl.noUpdate();
      }
      log.debug("Start cluster reconciliation.");
      sparkClusterStatusRecorder.updateStatusFromCache(sparkCluster);
      SparkClusterContext ctx = new SparkClusterContext(sparkCluster, context, submissionWorker);
      List<ClusterReconcileStep> reconcileSteps = getReconcileSteps(sparkCluster);
      for (ClusterReconcileStep step : reconcileSteps) {
        ReconcileProgress progress = step.reconcile(ctx, sparkClusterStatusRecorder);
        if (progress.isCompleted()) {
          return ReconcilerUtils.toUpdateControl(sparkCluster, progress);
        }
      }
      return ReconcilerUtils.toUpdateControl(sparkCluster, completeAndDefaultRequeue());
    } finally {
      log.debug("Reconciliation completed.");
      trackedMDC.reset();
    }
  }

  @Override
  public ErrorStatusUpdateControl<SparkCluster> updateErrorStatus(
      SparkCluster sparkCluster, Context<SparkCluster> context, Exception e) {
    LoggingUtils.TrackedMDC trackedMDC = new LoggingUtils.TrackedMDC();
    try {
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
  public List<EventSource<?, SparkCluster>> prepareEventSources(
      EventSourceContext<SparkCluster> context) {
    EventSource podEventSource =
        new InformerEventSource<>(
            InformerEventSourceConfiguration.from(Pod.class, SparkCluster.class)
                .withSecondaryToPrimaryMapper(
                    basicLabelSecondaryToPrimaryMapper(LABEL_SPARK_APPLICATION_NAME))
                .withLabelSelector(commonResourceLabelsStr())
                .build(),
            context);
    return List.of(podEventSource);
  }

  protected List<ClusterReconcileStep> getReconcileSteps(final SparkCluster cluster) {
    List<ClusterReconcileStep> steps = new ArrayList<>();
    steps.add(new ClusterValidateStep());
    steps.add(new ClusterTerminatedStep());
    switch (cluster.getStatus().getCurrentState().getCurrentStateSummary()) {
      case Submitted -> steps.add(new ClusterInitStep());
      case RunningHealthy -> {
        // There is nothing to do because Spark Cluster is supposed to run infinitely.
      }
      default -> steps.add(new ClusterUnknownStateStep());
    }
    return steps;
  }

  /**
   * Best-effort graceful termination upon delete.
   *
   * @param sparkCluster the resource that is marked for deletion
   * @param context the context with which the operation is executed
   * @return DeleteControl, with requeue if needed
   */
  @Override
  public DeleteControl cleanup(SparkCluster sparkCluster, Context<SparkCluster> context) {
    LoggingUtils.TrackedMDC trackedMDC = new LoggingUtils.TrackedMDC();
    try {
      log.info("Cleaning up resources for SparkCluster.");
      SparkClusterContext ctx = new SparkClusterContext(sparkCluster, context, submissionWorker);
      List<ClusterReconcileStep> cleanupSteps = new ArrayList<>();
      cleanupSteps.add(new ClusterValidateStep());
      cleanupSteps.add(new ClusterTerminatedStep());
      for (ClusterReconcileStep step : cleanupSteps) {
        ReconcileProgress progress = step.reconcile(ctx, sparkClusterStatusRecorder);
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
    sparkClusterStatusRecorder.removeCachedStatus(sparkCluster);
    return DeleteControl.defaultDelete();
  }
}
