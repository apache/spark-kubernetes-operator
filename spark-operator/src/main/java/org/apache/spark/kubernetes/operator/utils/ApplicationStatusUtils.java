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

package org.apache.spark.kubernetes.operator.utils;

import org.apache.spark.kubernetes.operator.Constants;
import org.apache.spark.kubernetes.operator.SparkApplication;
import org.apache.spark.kubernetes.operator.status.ApplicationState;
import org.apache.spark.kubernetes.operator.status.ApplicationStateSummary;

/**
 * Handy utils for create & manage Application Status
 */
public class ApplicationStatusUtils {

  public static boolean isValidApplicationStatus(SparkApplication app) {
    // null check
    return app.getStatus() != null
        && app.getStatus().getCurrentState() != null
        && app.getStatus().getCurrentState().getCurrentStateSummary() != null;
  }

  public static ApplicationState driverUnexpectedRemoved() {
    return new ApplicationState(ApplicationStateSummary.FAILED,
        Constants.DriverUnexpectedRemovedMessage);
  }

  public static ApplicationState driverLaunchTimedOut() {
    return new ApplicationState(ApplicationStateSummary.DRIVER_LAUNCH_TIMED_OUT,
        Constants.DriverLaunchTimeoutMessage);
  }

  public static ApplicationState driverReadyTimedOut() {
    return new ApplicationState(ApplicationStateSummary.SPARK_SESSION_INITIALIZATION_TIMED_OUT,
        Constants.DriverLaunchTimeoutMessage);
  }

  public static ApplicationState executorLaunchTimedOut() {
    return new ApplicationState(ApplicationStateSummary.EXECUTORS_LAUNCH_TIMED_OUT,
        Constants.ExecutorLaunchTimeoutMessage);
  }

  public static ApplicationState appCancelled() {
    return new ApplicationState(ApplicationStateSummary.RESOURCE_RELEASED,
        Constants.AppCancelledMessage);
  }

  public static boolean hasReachedState(SparkApplication application,
                                        ApplicationState stateToCheck) {
    if (!isValidApplicationStatus(application)) {
      return false;
    }
    return application.getStatus().getStateTransitionHistory().keySet().parallelStream()
        .anyMatch(stateId -> stateToCheck.equals(
            application.getStatus().getStateTransitionHistory().get(stateId)));
  }
}
