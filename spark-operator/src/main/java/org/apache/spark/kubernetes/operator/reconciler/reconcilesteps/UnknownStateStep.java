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

import java.util.Optional;

import io.fabric8.kubernetes.api.model.Pod;

import org.apache.spark.kubernetes.operator.controller.SparkApplicationContext;
import org.apache.spark.kubernetes.operator.reconciler.ReconcileProgress;
import org.apache.spark.kubernetes.operator.status.ApplicationState;
import org.apache.spark.kubernetes.operator.status.ApplicationStateSummary;
import org.apache.spark.kubernetes.operator.utils.StatusRecorder;

import static org.apache.spark.kubernetes.operator.Constants.UnknownStateMessage;

/**
 * Abnormal state handler
 */
public class UnknownStateStep extends AppReconcileStep {
  @Override
  public ReconcileProgress reconcile(SparkApplicationContext context,
                                     StatusRecorder statusRecorder) {
    ApplicationState state =
        new ApplicationState(ApplicationStateSummary.FAILED, UnknownStateMessage);
    Optional<Pod> driver = context.getDriverPod();
    driver.ifPresent(pod -> state.setLastObservedDriverStatus(pod.getStatus()));
    statusRecorder.persistStatus(context,
        context.getSparkApplication().getStatus().appendNewState(state));
    return ReconcileProgress.completeAndImmediateRequeue();
  }
}
