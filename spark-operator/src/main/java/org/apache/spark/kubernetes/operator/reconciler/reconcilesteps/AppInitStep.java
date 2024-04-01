/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.kubernetes.operator.reconciler.reconcilesteps;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.kubernetes.operator.Constants;
import org.apache.spark.kubernetes.operator.SparkApplication;
import org.apache.spark.kubernetes.operator.controller.SparkApplicationContext;
import org.apache.spark.kubernetes.operator.decorators.DriverResourceDecorator;
import org.apache.spark.kubernetes.operator.reconciler.ReconcileProgress;
import org.apache.spark.kubernetes.operator.reconciler.SparkReconcilerUtils;
import org.apache.spark.kubernetes.operator.status.ApplicationState;
import org.apache.spark.kubernetes.operator.status.ApplicationStateSummary;
import org.apache.spark.kubernetes.operator.status.ApplicationStatus;
import org.apache.spark.kubernetes.operator.utils.StatusRecorder;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.spark.kubernetes.operator.Constants.ScheduleFailureMessage;
import static org.apache.spark.kubernetes.operator.reconciler.ReconcileProgress.completeAndImmediateRequeue;
import static org.apache.spark.kubernetes.operator.reconciler.ReconcileProgress.proceed;
import static org.apache.spark.kubernetes.operator.reconciler.ReconcileProgress.completeAndDefaultRequeue;
import static org.apache.spark.kubernetes.operator.utils.SparkExceptionUtils.buildGeneralErrorMessage;

/**
 * Request all driver and driver resources when starting an attempt
 */
@Slf4j
public class AppInitStep extends AppReconcileStep {
    @Override
    public ReconcileProgress reconcile(SparkApplicationContext context,
                                       StatusRecorder statusRecorder) {
        ApplicationState currentState = context.getSparkApplication().getStatus().getCurrentState();
        if (!currentState.getCurrentStateSummary().isInitializing()) {
            return proceed();
        }
        SparkApplication app = context.getSparkApplication();
        if (app.getStatus().getPreviousAttemptSummary() != null) {
            Instant lastTransitionTime = Instant.parse(currentState.getLastTransitionTime());
            Instant restartTime = lastTransitionTime.plusMillis(
                    app.getSpec().getApplicationTolerations().getRestartConfig()
                            .getRestartBackoffMillis());
            Instant now = Instant.now();
            if (restartTime.isAfter(now)) {
                return ReconcileProgress.completeAndRequeueAfter(
                        Duration.between(now, restartTime));
            }
        }
        try {
            List<HasMetadata> createdPreResources = new ArrayList<>();
            for (HasMetadata resource : context.getDriverPreResourcesSpec()) {
                Optional<HasMetadata> createdResource =
                        SparkReconcilerUtils.getOrCreateSecondaryResource(context.getClient(),
                                resource);
                if (createdResource.isPresent()) {
                    createdPreResources.add(createdResource.get());
                } else {
                    updateStatusForCreationFailure(context, resource, statusRecorder);
                    return completeAndImmediateRequeue();
                }
            }
            Optional<Pod> driverPod =
                    SparkReconcilerUtils.getOrCreateSecondaryResource(context.getClient(),
                            context.getDriverPodSpec());
            if (driverPod.isPresent()) {
                DriverResourceDecorator decorator = new DriverResourceDecorator(driverPod.get());
                createdPreResources.forEach(decorator::decorate);
                context.getClient().resourceList(createdPreResources).forceConflicts()
                        .serverSideApply();
                List<HasMetadata> driverResources = context.getDriverResourcesSpec();
                driverResources.forEach(decorator::decorate);
                for (HasMetadata resource : driverResources) {
                    Optional<HasMetadata> createdResource =
                            SparkReconcilerUtils.getOrCreateSecondaryResource(context.getClient(),
                                    resource);
                    if (createdResource.isEmpty()) {
                        updateStatusForCreationFailure(context, resource, statusRecorder);
                        return completeAndImmediateRequeue();
                    }
                }
            }
            ApplicationStatus updatedStatus = context.getSparkApplication().getStatus()
                    .appendNewState(new ApplicationState(ApplicationStateSummary.DRIVER_REQUESTED,
                            Constants.DriverRequestedMessage));
            statusRecorder.persistStatus(context, updatedStatus);
            return completeAndDefaultRequeue();
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("Failed to request driver resource.", e);
            }
            String errorMessage = ScheduleFailureMessage +
                    " StackTrace: " +
                    buildGeneralErrorMessage(e);
            statusRecorder.persistStatus(context, context.getSparkApplication().getStatus()
                    .appendNewState(new ApplicationState(ApplicationStateSummary.SCHEDULING_FAILURE,
                            errorMessage)));
            return completeAndImmediateRequeue();
        }
    }

    private void updateStatusForCreationFailure(SparkApplicationContext context,
                                                HasMetadata resourceSpec,
                                                StatusRecorder statusRecorder) {
        if (log.isErrorEnabled()) {
            log.error("Failed all attempts to request driver resource {}.",
                    resourceSpec.getMetadata());
        }
        statusRecorder.persistStatus(context, context.getSparkApplication().getStatus()
                .appendNewState(new ApplicationState(ApplicationStateSummary.SCHEDULING_FAILURE,
                        "Failed to request resource for driver with kind: "
                                + resourceSpec.getKind()
                                + ", name: "
                                + resourceSpec.getMetadata().getName())));
    }
}
