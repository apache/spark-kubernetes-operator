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

public enum ClusterStateSummary implements BaseStateSummary {
  /** Spark cluster is submitted but yet scheduled */
  Submitted,

  /** Spark cluster fails to schedule. */
  SchedulingFailure,

  /** Cluster is running healthy */
  RunningHealthy,

  /** Cluster failed */
  Failed,

  /** all resources (pods, services .etc have been cleaned up) */
  ResourceReleased;

  public boolean isInitializing() {
    return Submitted.equals(this);
  }

  public boolean isStarting() {
    return RunningHealthy.ordinal() > this.ordinal();
  }

  /**
   * A state is 'terminated' if and only if no further actions are needed to reconcile it.
   *
   * @return true if the state indicates the cluster has terminated
   */
  public boolean isTerminated() {
    return ResourceReleased.equals(this);
  }

  @Override
  public boolean isFailure() {
    return SchedulingFailure.equals(this) || Failed.equals(this);
  }

  @Override
  public boolean isInfrastructureFailure() {
    return SchedulingFailure.equals(this);
  }
}
