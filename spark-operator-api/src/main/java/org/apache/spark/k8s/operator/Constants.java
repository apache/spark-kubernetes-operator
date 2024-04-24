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

package org.apache.spark.k8s.operator;

public class Constants {
  public static final String API_GROUP = "org.apache.spark";
  public static final String API_VERSION = "v1alpha1";
  public static final String LABEL_SPARK_APPLICATION_NAME = "spark.operator/spark-app-name";
  public static final String LABEL_SPARK_OPERATOR_NAME = "spark.operator/name";
  public static final String LABEL_SENTINEL_RESOURCE = "spark.operator/sentinel";
  public static final String LABEL_RESOURCE_NAME = "app.kubernetes.io/name";
  public static final String LABEL_SPARK_ROLE_NAME = "spark-role";
  public static final String LABEL_SPARK_ROLE_DRIVER_VALUE = "driver";
  public static final String LABEL_SPARK_ROLE_EXECUTOR_VALUE = "executor";
  public static final String SENTINEL_RESOURCE_DUMMY_FIELD = "sentinel.dummy.number";

  // Default state messages
  public static final String DriverRequestedMessage = "Requested driver from resource scheduler.";
  public static final String DriverCompletedMessage = "Spark application completed successfully.";
  public static final String DriverTerminatedBeforeInitializationMessage =
      "Driver container is terminated without SparkContext / SparkSession initialization.";
  public static final String DriverFailedInitContainersMessage =
      "Driver has failed init container(s). Refer last observed status for details.";
  public static final String DriverFailedMessage =
      "Driver has one or more failed critical container(s), refer last observed status for "
          + "details.";
  public static final String DriverSucceededMessage =
      "Driver has critical container(s) exited with 0.";
  public static final String DriverRestartedMessage =
      "Driver has one or more critical container(s) restarted unexpectedly, refer last "
          + "observed status for details.";
  public static final String AppCancelledMessage =
      "Spark application has been shutdown as requested.";
  public static final String DriverUnexpectedRemovedMessage =
      "Driver removed. This could caused by 'exit' called in driver process with non-zero "
          + "code, involuntary disruptions or unintentional destroy behavior, check "
          + "Kubernetes events for more details.";
  public static final String DriverLaunchTimeoutMessage =
      "The driver has not responded to the initial health check request within the "
          + "allotted start-up time. This can be configured by setting "
          + ".spec.applicationTolerations.applicationTimeoutConfig.";
  public static final String DriverRunning = "Driver has started running.";
  public static final String DriverReady = "Driver has reached ready state.";
  public static final String SubmittedStateMessage =
      "Spark application has been created on Kubernetes Cluster.";
  public static final String UnknownStateMessage = "Cannot process application status.";
  public static final String ExceedMaxRetryAttemptMessage =
      "The maximum number of restart attempts (%d) has been exceeded.";
  public static final String ScheduleFailureMessage =
      "Failed to request driver from scheduler backend.";
  public static final String RunningHealthyMessage = "Application is running healthy.";
  public static final String InitializedWithBelowThresholdExecutorsMessage =
      "The application is running with less than minimal number of requested initial "
          + "executors.";
  public static final String RunningWithBelowThresholdExecutorsMessage =
      "The Spark application is running with less than minimal number of requested " + "executors.";
  public static final String ExecutorLaunchTimeoutMessage =
      "The Spark application failed to get enough executors in the given time threshold.";
}
