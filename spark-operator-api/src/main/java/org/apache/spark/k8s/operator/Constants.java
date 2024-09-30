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
  public static final String API_GROUP = "spark.apache.org";
  public static final String API_VERSION = "v1alpha1";
  public static final String LABEL_SPARK_APPLICATION_NAME = "spark.operator/spark-app-name";
  public static final String LABEL_SPARK_CLUSTER_NAME = "spark.operator/spark-cluster-name";
  public static final String LABEL_SPARK_OPERATOR_NAME = "spark.operator/name";
  public static final String LABEL_SENTINEL_RESOURCE = "spark.operator/sentinel";
  public static final String LABEL_RESOURCE_NAME = "app.kubernetes.io/name";
  public static final String LABEL_COMPONENT_NAME = "app.kubernetes.io/component";
  public static final String LABEL_SPARK_ROLE_NAME = "spark-role";
  public static final String LABEL_SPARK_ROLE_DRIVER_VALUE = "driver";
  public static final String LABEL_SPARK_ROLE_EXECUTOR_VALUE = "executor";
  public static final String LABEL_SPARK_ROLE_CLUSTER_VALUE = "cluster";
  public static final String LABEL_SPARK_ROLE_MASTER_VALUE = "master";
  public static final String LABEL_SPARK_ROLE_WORKER_VALUE = "worker";
  public static final String LABEL_SPARK_VERSION_NAME = "spark-version";
  public static final String SENTINEL_RESOURCE_DUMMY_FIELD = "sentinel.dummy.number";

  public static final String DRIVER_SPARK_CONTAINER_PROP_KEY =
      "spark.kubernetes.driver.podTemplateContainerName";
  public static final String DRIVER_SPARK_TEMPLATE_FILE_PROP_KEY =
      "spark.kubernetes.driver.podTemplateFile";
  public static final String EXECUTOR_SPARK_TEMPLATE_FILE_PROP_KEY =
      "spark.kubernetes.executor.podTemplateFile";

  // Default state messages
  public static final String DRIVER_REQUESTED_MESSAGE = "Requested driver from resource scheduler.";
  public static final String DRIVER_COMPLETED_MESSAGE = "Spark application completed successfully.";
  public static final String DRIVER_TERMINATED_BEFORE_INITIALIZATION_MESSAGE =
      "Driver container is terminated without SparkContext / SparkSession initialization.";
  public static final String DRIVER_FAILED_INIT_CONTAINERS_MESSAGE =
      "Driver has failed init container(s). Refer last observed status for details.";
  public static final String DRIVER_FAILED_MESSAGE =
      "Driver has one or more failed critical container(s), refer last observed status for "
          + "details.";
  public static final String DRIVER_SUCCEEDED_MESSAGE =
      "Driver has critical container(s) exited with 0.";
  public static final String DRIVER_RESTARTED_MESSAGE =
      "Driver has one or more critical container(s) restarted unexpectedly, refer last "
          + "observed status for details.";
  public static final String APP_CANCELLED_MESSAGE =
      "Spark application has been shutdown as requested.";
  public static final String DRIVER_UNEXPECTED_REMOVED_MESSAGE =
      "Driver removed. This could caused by 'exit' called in driver process with non-zero "
          + "code, involuntary disruptions or unintentional destroy behavior, check "
          + "Kubernetes events for more details.";
  public static final String DRIVER_LAUNCH_TIMEOUT_MESSAGE =
      "The driver has not responded to the initial health check request within the "
          + "allotted start-up time. This can be configured by setting "
          + ".spec.applicationTolerations.applicationTimeoutConfig.";
  public static final String DRIVER_RUNNING_MESSAGE = "Driver has started running.";
  public static final String DRIVER_READY_MESSAGE = "Driver has reached ready state.";
  public static final String SUBMITTED_STATE_MESSAGE =
      "Spark application has been created on Kubernetes Cluster.";
  public static final String UNKNOWN_STATE_MESSAGE = "Cannot process application status.";
  public static final String EXCEED_MAX_RETRY_ATTEMPT_MESSAGE =
      "The maximum number of restart attempts (%d) has been exceeded.";
  public static final String SCHEDULE_FAILURE_MESSAGE =
      "Failed to request driver from scheduler backend.";
  public static final String RUNNING_HEALTHY_MESSAGE = "Application is running healthy.";
  public static final String INITIALIZED_WITH_BELOW_THRESHOLD_EXECUTORS_MESSAGE =
      "The application is running with less than minimal number of requested initial executors.";
  public static final String RUNNING_WITH_BELOW_THRESHOLD_EXECUTORS_MESSAGE =
      "The Spark application is running with less than minimal number of requested executors.";
  public static final String EXECUTOR_LAUNCH_TIMEOUT_MESSAGE =
      "The Spark application failed to get enough executors in the given time threshold.";

  // Spark Cluster Messages
  public static final String CLUSTER_SCHEDULE_FAILURE_MESSAGE =
      "Failed to request Spark cluster from scheduler backend.";
  public static final String CLUSTER_SUBMITTED_STATE_MESSAGE =
      "Spark cluster has been submitted to Kubernetes Cluster.";
  public static final String CLUSTER_READY_MESSAGE = "Cluster has reached ready state.";
  public static final String UNKNOWN_CLUSTER_STATE_MESSAGE = "Cannot process cluster status.";
}
