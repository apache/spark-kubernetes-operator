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

import io.fabric8.kubernetes.api.model.Pod;
import org.apache.spark.kubernetes.operator.SparkApplication;
import org.apache.spark.kubernetes.operator.controller.SparkApplicationContext;
import org.apache.spark.kubernetes.operator.reconciler.ReconcileProgress;
import org.apache.spark.kubernetes.operator.reconciler.observers.BaseAppDriverObserver;
import org.apache.spark.kubernetes.operator.status.ApplicationState;
import org.apache.spark.kubernetes.operator.status.ApplicationStatus;
import org.apache.spark.kubernetes.operator.utils.StatusRecorder;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.spark.kubernetes.operator.reconciler.ReconcileProgress.completeAndImmediateRequeue;
import static org.apache.spark.kubernetes.operator.reconciler.ReconcileProgress.proceed;
import static org.apache.spark.kubernetes.operator.utils.ApplicationStatusUtils.driverUnexpectedRemoved;

/**
 * Basic reconcile step for application
 */
public abstract class AppReconcileStep {
    public abstract ReconcileProgress reconcile(SparkApplicationContext context,
                                                StatusRecorder statusRecorder);

    protected ReconcileProgress observeDriver(final SparkApplicationContext context,
                                              final StatusRecorder statusRecorder,
                                              final List<BaseAppDriverObserver> observers) {
        Optional<Pod> driverPodOptional = context.getDriverPod();
        SparkApplication app = context.getSparkApplication();
        ApplicationStatus currentStatus = app.getStatus();
        if (driverPodOptional.isPresent()) {
            List<ApplicationState> stateUpdates = observers.stream()
                    .map(o -> o.observe(driverPodOptional.get(), app.getSpec(), app.getStatus()))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());
            if (stateUpdates.isEmpty()) {
                return proceed();
            } else {
                for (ApplicationState state : stateUpdates) {
                    currentStatus = currentStatus.appendNewState(state);
                }
                statusRecorder.persistStatus(context, currentStatus);
                return completeAndImmediateRequeue();
            }
        } else {
            ApplicationStatus updatedStatus =
                    currentStatus.appendNewState(driverUnexpectedRemoved());
            statusRecorder.persistStatus(context, updatedStatus);
            return completeAndImmediateRequeue();
        }
    }
}
