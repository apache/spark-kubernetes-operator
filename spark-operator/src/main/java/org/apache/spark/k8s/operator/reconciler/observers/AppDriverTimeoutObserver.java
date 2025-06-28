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

package org.apache.spark.k8s.operator.reconciler.observers;

import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;

import io.fabric8.kubernetes.api.model.Pod;

import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.spec.ApplicationTimeoutConfig;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.utils.SparkAppStatusUtils;

/** Observes driver status and time-out as configured in app spec. */
public class AppDriverTimeoutObserver extends BaseAppDriverObserver {

  /**
   * Operator may proactively terminate application if it has stay in certain state for a while.
   * This helps to avoid resource deadlock when app cannot proceed. Such states include:
   *
   * <ul>
   *   <li>DriverRequested goes to DriverStartTimedOut if driver pod cannot be scheduled or cannot
   *       start running
   *   <li>DriverStarted goes to DriverReadyTimedOut if Spark session cannot be initialized
   *   <li>DriverReady and InitializedBelowThresholdExecutors goes to ExecutorsStartTimedOut if app
   *       cannot acquire at least minimal executors in the given time
   * </ul>
   *
   * <p>Operator will NOT proactively stop the app if it has acquired enough executors and later
   * lose them. User may build additional layers to alert and act on such scenario. Timeout check
   * would be performed at the end of reconcile - and it would be performed only if there's no other
   * updates to be performed in the same reconcile action
   */
  @Override
  public Optional<ApplicationState> observe(
      Pod driver, ApplicationSpec spec, ApplicationStatus status) {
    long timeoutThreshold;
    Supplier<ApplicationState> supplier;
    ApplicationTimeoutConfig timeoutConfig =
        spec.getApplicationTolerations().getApplicationTimeoutConfig();
    ApplicationState state = status.getCurrentState();
    switch (state.getCurrentStateSummary()) {
      case DriverRequested -> {
        timeoutThreshold = timeoutConfig.getDriverStartTimeoutMillis();
        supplier = SparkAppStatusUtils::driverLaunchTimedOut;
      }
      case DriverStarted -> {
        timeoutThreshold = timeoutConfig.getDriverReadyTimeoutMillis();
        supplier = SparkAppStatusUtils::driverReadyTimedOut;
      }
      case DriverReady, InitializedBelowThresholdExecutors -> {
        timeoutThreshold = timeoutConfig.getExecutorStartTimeoutMillis();
        supplier = SparkAppStatusUtils::executorLaunchTimedOut;
      }
      default -> {
        // No timeout check needed for other states
        return Optional.empty();
      }
    }
    Instant lastTransitionTime = Instant.parse(state.getLastTransitionTime());
    if (timeoutThreshold > 0L
        && lastTransitionTime.plusMillis(timeoutThreshold).isBefore(Instant.now())) {
      ApplicationState appState = supplier.get();
      appState.setLastObservedDriverStatus(driver.getStatus());
      return Optional.of(appState);
    }
    return Optional.empty();
  }
}
