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

package org.apache.spark.kubernetes.operator.reconciler.observers;

import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;

import io.fabric8.kubernetes.api.model.Pod;

import org.apache.spark.kubernetes.operator.spec.ApplicationSpec;
import org.apache.spark.kubernetes.operator.spec.ApplicationTimeoutConfig;
import org.apache.spark.kubernetes.operator.status.ApplicationState;
import org.apache.spark.kubernetes.operator.status.ApplicationStatus;
import org.apache.spark.kubernetes.operator.utils.ApplicationStatusUtils;

/**
 * Observes driver status and time-out as configured in app spec
 */
public class AppDriverTimeoutObserver extends BaseAppDriverObserver {

  /**
   * Operator may proactively terminate application if it has stay in certain state for a while.
   * This helps to avoid resource deadlock when app cannot proceed.
   * Such states include
   * - DRIVER_REQUESTED -> goes to DRIVER_LAUNCH_TIMED_OUT if driver pod cannot be scheduled or
   * cannot start running
   * - DRIVER_STARTED -> goes to SPARK_SESSION_INITIALIZATION_TIMED_OUT if Spark session cannot
   * be initialized
   * - DRIVER_READY / EXECUTOR_REQUESTED / EXECUTOR_SCHEDULED /
   * INITIALIZED_BELOW_THRESHOLD_EXECUTORS
   * -> go to EXECUTORS_LAUNCH_TIMED_OUT if app cannot acquire at least minimal executors in
   * given time
   * Operator will NOT proactively stop the app if it has acquired enough executors and later
   * lose them. User may build additional layers to alert and act on such scenario.
   * Timeout check would be performed at the end of reconcile - and it would be performed only
   * if there's no other updates to be performed in the same reconcile action.
   */
  @Override
  public Optional<ApplicationState> observe(Pod driver,
                                            ApplicationSpec spec,
                                            ApplicationStatus currentStatus) {
    Instant lastTransitionTime =
        Instant.parse(currentStatus.getCurrentState().getLastTransitionTime());
    long timeoutThreshold;
    Supplier<ApplicationState> supplier;
    ApplicationTimeoutConfig timeoutConfig =
        spec.getApplicationTolerations().getApplicationTimeoutConfig();
    switch (currentStatus.getCurrentState().getCurrentStateSummary()) {
      case DRIVER_REQUESTED:
        timeoutThreshold = timeoutConfig.getDriverStartTimeoutMillis();
        supplier = ApplicationStatusUtils::driverLaunchTimedOut;
        break;
      case DRIVER_STARTED:
        timeoutThreshold = timeoutConfig.getSparkSessionStartTimeoutMillis();
        supplier = ApplicationStatusUtils::driverReadyTimedOut;
        break;
      case DRIVER_READY:
      case INITIALIZED_BELOW_THRESHOLD_EXECUTORS:
        timeoutThreshold = timeoutConfig.getExecutorStartTimeoutMillis();
        supplier = ApplicationStatusUtils::executorLaunchTimedOut;
        break;
      default:
        // No timeout check needed for other states
        return Optional.empty();
    }
    if (timeoutThreshold > 0L &&
        lastTransitionTime.plusMillis(timeoutThreshold).isBefore(Instant.now())) {
      ApplicationState state = supplier.get();
      state.setLastObservedDriverStatus(driver.getStatus());
      return Optional.of(state);
    }
    return Optional.empty();
  }
}