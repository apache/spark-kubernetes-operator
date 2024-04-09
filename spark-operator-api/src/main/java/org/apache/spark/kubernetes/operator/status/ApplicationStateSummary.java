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

package org.apache.spark.kubernetes.operator.status;

import java.util.Set;

public enum ApplicationStateSummary implements BaseStateSummary {
  /**
   * Spark application is submitted to the cluster but yet scheduled.
   */
  SUBMITTED,

  /**
   * Spark application will be restarted with same configuration
   */
  SCHEDULED_TO_RESTART,

  /**
   * A request has been made to start driver pod in the cluster
   */
  DRIVER_REQUESTED,

  /**
   * Driver pod has reached running state
   */
  DRIVER_STARTED,

  /**
   * Spark session is initialized
   */
  DRIVER_READY,

  /**
   * Less that minimal required executor pods become ready during starting up
   */
  INITIALIZED_BELOW_THRESHOLD_EXECUTORS,

  /**
   * All required executor pods started
   */
  RUNNING_HEALTHY,

  /**
   * The application has lost a fraction of executors for external reasons
   */
  RUNNING_WITH_BELOW_THRESHOLD_EXECUTORS,

  /**
   * The request timed out for driver
   */
  DRIVER_LAUNCH_TIMED_OUT,

  /**
   * The request timed out for executors
   */
  EXECUTORS_LAUNCH_TIMED_OUT,

  /**
   * Timed out waiting for context to be initialized
   */
  SPARK_SESSION_INITIALIZATION_TIMED_OUT,

  /**
   * The application completed successfully, or System.exit is called explicitly with zero state
   */
  SUCCEEDED,

  /**
   * The application has failed, JVM exited abnormally, or System.exit is called explicitly
   * with non-zero state
   */
  FAILED,

  /**
   * The job has failed because of a scheduler side issue. e.g. driver scheduled on node with
   * insufficient resources
   */
  SCHEDULING_FAILURE,

  /**
   * The driver pod was failed with Evicted reason
   */
  DRIVER_EVICTED,

  /**
   * all resources (pods, services .etc have been cleaned up)
   */
  RESOURCE_RELEASED,

  /**
   * If configured, operator may mark app as terminated without releasing resources. While this
   * can be helpful in dev phase, it shall not be enabled for prod use cases.
   */
  TERMINATED_WITHOUT_RELEASE_RESOURCES;

  public boolean isInitializing() {
    return SUBMITTED.equals(this) || SCHEDULED_TO_RESTART.equals(this);
  }

  public boolean isStarting() {
    return SCHEDULED_TO_RESTART.ordinal() < this.ordinal()
        && RUNNING_HEALTHY.ordinal() > this.ordinal();
  }

  public boolean isTerminated() {
    return RESOURCE_RELEASED.equals(this)
        || TERMINATED_WITHOUT_RELEASE_RESOURCES.equals(this);
  }

  public boolean isStopping() {
    return RUNNING_HEALTHY.ordinal() < this.ordinal() && !isTerminated();
  }

  public static final Set<ApplicationStateSummary> infrastructureFailures =
      Set.of(DRIVER_LAUNCH_TIMED_OUT,
          EXECUTORS_LAUNCH_TIMED_OUT, SCHEDULING_FAILURE);

  public static final Set<ApplicationStateSummary> failures = Set.of(DRIVER_LAUNCH_TIMED_OUT,
      EXECUTORS_LAUNCH_TIMED_OUT, SCHEDULING_FAILURE, FAILED,
      SPARK_SESSION_INITIALIZATION_TIMED_OUT);

  @Override
  public boolean isFailure() {
    return failures.contains(this);
  }

  @Override
  public boolean isInfrastructureFailure() {
    return infrastructureFailures.contains(this);
  }

}