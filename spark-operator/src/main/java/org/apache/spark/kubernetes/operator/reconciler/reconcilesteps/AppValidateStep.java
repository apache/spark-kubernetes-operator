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

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.kubernetes.operator.controller.SparkApplicationContext;
import org.apache.spark.kubernetes.operator.reconciler.ReconcileProgress;
import org.apache.spark.kubernetes.operator.spec.DeploymentMode;
import org.apache.spark.kubernetes.operator.status.ApplicationState;
import org.apache.spark.kubernetes.operator.status.ApplicationStateSummary;
import org.apache.spark.kubernetes.operator.status.ApplicationStatus;
import org.apache.spark.kubernetes.operator.utils.StatusRecorder;

import static org.apache.spark.kubernetes.operator.reconciler.ReconcileProgress.completeAndImmediateRequeue;
import static org.apache.spark.kubernetes.operator.reconciler.ReconcileProgress.proceed;
import static org.apache.spark.kubernetes.operator.utils.ApplicationStatusUtils.isValidApplicationStatus;

/**
 * Validates the submitted app. This can be re-factored into webhook in future.
 */
@Slf4j
public class AppValidateStep extends AppReconcileStep {
    @Override
    public ReconcileProgress reconcile(SparkApplicationContext context,
                                       StatusRecorder statusRecorder) {
        if (!isValidApplicationStatus(context.getSparkApplication())) {
            log.warn("Spark application found with empty status. Resetting to initial state.");
            statusRecorder.persistStatus(context, new ApplicationStatus());
        }
        if (DeploymentMode.ClientMode.equals(context.getSparkApplication().getSpec())) {
            ApplicationState failure = new ApplicationState(ApplicationStateSummary.FAILED,
                    "Client mode is not supported yet.");
            statusRecorder.persistStatus(context,
                    context.getSparkApplication().getStatus().appendNewState(failure));
            return completeAndImmediateRequeue();
        }
        return proceed();
    }
}
