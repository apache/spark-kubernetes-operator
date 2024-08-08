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

package org.apache.spark.k8s.operator.status;

import java.util.Set;

public enum ApplicationStateSummary implements BaseStateSummary {
  /** Spark application is submitted to the cluster but yet scheduled */
  Submitted,

  /** Spark application will be restarted with same configuration */
  ScheduledToRestart,

  /** A request has been made to start driver pod in the cluster */
  DriverRequested,

  /**
   * Driver pod has reached 'Running' state and thus bound to a node Refer
   * https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/
   */
  DriverStarted,

  /**
   * Driver pod is ready to serve connections from executors Refer Refer
   * https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/
   */
  DriverReady,

  /**
   * Less that minimal required executor pods reached condition 'Ready' during starting up Note that
   * reaching 'Ready' does not necessarily mean that the executor has successfully registered with
   * driver. This is a best-effort from operator to detect executor status
   */
  InitializedBelowThresholdExecutors,

  /**
   * All required executor pods started reached condition 'Ready' Note that reaching 'Ready' does
   * not necessarily mean that the executor has successfully registered with driver. This is a
   * best-effort from operator to detect executor status
   */
  RunningHealthy,

  /** The application has lost a fraction of executors for external reasons */
  RunningWithBelowThresholdExecutors,

  /** The request timed out for driver */
  DriverStartTimedOut,

  /** The request timed out for executors */
  ExecutorsStartTimedOut,

  /** Timed out waiting for driver to become ready */
  DriverReadyTimedOut,

  /**
   * The application completed successfully, or System.exit() is called explicitly with zero state
   */
  Succeeded,

  /**
   * The application has failed, JVM exited abnormally, or System.exit is called explicitly with
   * non-zero state
   */
  Failed,

  /**
   * Operator failed to orchestrate Spark application in cluster. For example, the given pod
   * template is rejected by API server because it's invalid or does not meet cluster security
   * standard; operator is not able to schedule pods due to insufficient quota or missing RBAC
   */
  SchedulingFailure,

  /** The driver pod was failed with Evicted reason */
  DriverEvicted,

  /** all resources (pods, services .etc have been cleaned up) */
  ResourceReleased,

  /**
   * If configured, operator may mark app as terminated without releasing resources. While this can
   * be helpful in dev phase, it shall not be enabled for prod use cases
   */
  TerminatedWithoutReleaseResources;

  public static final Set<ApplicationStateSummary> infrastructureFailures =
      Set.of(DriverStartTimedOut, ExecutorsStartTimedOut, SchedulingFailure);

  public static final Set<ApplicationStateSummary> failures =
      Set.of(
          DriverStartTimedOut,
          ExecutorsStartTimedOut,
          SchedulingFailure,
          DriverEvicted,
          Failed,
          DriverReadyTimedOut);

  public boolean isInitializing() {
    return Submitted.equals(this) || ScheduledToRestart.equals(this);
  }

  public boolean isStarting() {
    return ScheduledToRestart.ordinal() < this.ordinal()
        && RunningHealthy.ordinal() > this.ordinal();
  }

  /**
   * A state is 'terminated' if and only if no further actions are needed to reconcile it
   *
   * @return true if the state indicates app has terminated
   */
  public boolean isTerminated() {
    return ResourceReleased.equals(this) || TerminatedWithoutReleaseResources.equals(this);
  }

  /**
   * When state is 'stopping', operator releases its resources based on retain policy, and perform
   * retry based on retry policy
   *
   * @return true if an app is stopping
   */
  public boolean isStopping() {
    return RunningWithBelowThresholdExecutors.ordinal() < this.ordinal() && !isTerminated();
  }

  @Override
  public boolean isFailure() {
    return failures.contains(this);
  }

  @Override
  public boolean isInfrastructureFailure() {
    return infrastructureFailures.contains(this);
  }
}
