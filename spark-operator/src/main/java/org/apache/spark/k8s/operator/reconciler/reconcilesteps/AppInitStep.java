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

import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.completeAndDefaultRequeue;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.completeAndImmediateRequeue;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.proceed;
import static org.apache.spark.k8s.operator.utils.SparkExceptionUtils.buildGeneralErrorMessage;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.decorators.DriverResourceDecorator;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.utils.ReconcilerUtils;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;

/** Request all driver and its resources when starting an attempt */
@Slf4j
public class AppInitStep extends AppReconcileStep {
  @Override
  public ReconcileProgress reconcile(
      SparkAppContext context, SparkAppStatusRecorder statusRecorder) {
    ApplicationState currentState = context.getResource().getStatus().getCurrentState();
    if (!currentState.getCurrentStateSummary().isInitializing()) {
      return proceed();
    }
    SparkApplication app = context.getResource();
    if (app.getStatus().getPreviousAttemptSummary() != null) {
      Instant lastTransitionTime = Instant.parse(currentState.getLastTransitionTime());
      Instant restartTime =
          lastTransitionTime.plusMillis(
              app.getSpec()
                  .getApplicationTolerations()
                  .getRestartConfig()
                  .getRestartBackoffMillis());
      Instant now = Instant.now();
      if (restartTime.isAfter(now)) {
        return ReconcileProgress.completeAndRequeueAfter(Duration.between(now, restartTime));
      }
    }
    try {
      List<HasMetadata> preResourcesSpec = context.getDriverPreResourcesSpec();
      for (HasMetadata resource : preResourcesSpec) {
        Optional<HasMetadata> createdResource =
            ReconcilerUtils.getOrCreateSecondaryResource(context.getClient(), resource);
        if (createdResource.isEmpty()) {
          return appendStateAndImmediateRequeue(
              context, statusRecorder, creationFailureState(resource));
        }
      }
      Optional<Pod> driverPod =
          ReconcilerUtils.getOrCreateSecondaryResource(
              context.getClient(), context.getDriverPodSpec());
      if (driverPod.isPresent()) {
        DriverResourceDecorator decorator = new DriverResourceDecorator(driverPod.get());
        preResourcesSpec.forEach(decorator::decorate);
        context.getClient().resourceList(preResourcesSpec).forceConflicts().serverSideApply();
        List<HasMetadata> driverResources = context.getDriverResourcesSpec();
        driverResources.forEach(decorator::decorate);
        for (HasMetadata resource : driverResources) {
          Optional<HasMetadata> createdResource =
              ReconcilerUtils.getOrCreateSecondaryResource(context.getClient(), resource);
          if (createdResource.isEmpty()) {
            return appendStateAndImmediateRequeue(
                context, statusRecorder, creationFailureState(resource));
          }
        }
      }
      ApplicationStatus updatedStatus =
          context
              .getResource()
              .getStatus()
              .appendNewState(
                  new ApplicationState(
                      ApplicationStateSummary.DriverRequested, Constants.DRIVER_REQUESTED_MESSAGE));
      statusRecorder.persistStatus(context, updatedStatus);
      return completeAndDefaultRequeue();
    } catch (Exception e) {
      if (log.isErrorEnabled()) {
        log.error("Failed to request driver resource.", e);
      }
      String errorMessage =
          Constants.SCHEDULE_FAILURE_MESSAGE + " StackTrace: " + buildGeneralErrorMessage(e);
      statusRecorder.persistStatus(
          context,
          context
              .getResource()
              .getStatus()
              .appendNewState(
                  new ApplicationState(ApplicationStateSummary.SchedulingFailure, errorMessage)));
      return completeAndImmediateRequeue();
    }
  }

  private ApplicationState creationFailureState(HasMetadata failedResource) {
    return new ApplicationState(
        ApplicationStateSummary.SchedulingFailure,
        "Failed to request resource for driver with kind: "
            + failedResource.getKind()
            + ", name: "
            + failedResource.getMetadata().getName());
  }
}
