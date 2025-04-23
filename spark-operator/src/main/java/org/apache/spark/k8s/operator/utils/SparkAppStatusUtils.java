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

package org.apache.spark.k8s.operator.utils;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;

/** Handy utils for create and manage Application Status. */
public final class SparkAppStatusUtils {

  private SparkAppStatusUtils() {}

  public static boolean isValidApplicationStatus(SparkApplication app) {
    // null check
    return app.getStatus() != null
        && app.getStatus().getCurrentState() != null
        && app.getStatus().getCurrentState().getCurrentStateSummary() != null;
  }

  public static ApplicationState driverUnexpectedRemoved() {
    return new ApplicationState(
        ApplicationStateSummary.Failed, Constants.DRIVER_UNEXPECTED_REMOVED_MESSAGE);
  }

  public static ApplicationState driverLaunchTimedOut() {
    return new ApplicationState(
        ApplicationStateSummary.DriverStartTimedOut, Constants.DRIVER_LAUNCH_TIMEOUT_MESSAGE);
  }

  public static ApplicationState driverReadyTimedOut() {
    return new ApplicationState(
        ApplicationStateSummary.DriverReadyTimedOut, Constants.DRIVER_LAUNCH_TIMEOUT_MESSAGE);
  }

  public static ApplicationState executorLaunchTimedOut() {
    return new ApplicationState(
        ApplicationStateSummary.ExecutorsStartTimedOut, Constants.EXECUTOR_LAUNCH_TIMEOUT_MESSAGE);
  }

  public static ApplicationState appCancelled() {
    return new ApplicationState(
        ApplicationStateSummary.ResourceReleased, Constants.APP_CANCELLED_MESSAGE);
  }

  public static ApplicationState appExceededRetainDuration() {
    return new ApplicationState(
        ApplicationStateSummary.ResourceReleased, Constants.APP_EXCEEDED_RETAIN_DURATION_MESSAGE);
  }

  public static boolean hasReachedState(
      SparkApplication application, ApplicationState stateToCheck) {
    return isValidApplicationStatus(application)
        && application.getStatus().getStateTransitionHistory().keySet().parallelStream()
            .anyMatch(
                stateId ->
                    stateToCheck.equals(
                        application.getStatus().getStateTransitionHistory().get(stateId)));
  }
}
