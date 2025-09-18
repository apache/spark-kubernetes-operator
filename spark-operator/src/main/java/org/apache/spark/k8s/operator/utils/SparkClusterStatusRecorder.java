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

import java.util.List;

import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.context.SparkClusterContext;
import org.apache.spark.k8s.operator.listeners.SparkClusterStatusListener;
import org.apache.spark.k8s.operator.status.ClusterState;
import org.apache.spark.k8s.operator.status.ClusterStatus;

/** Records the status of a Spark cluster. */
public class SparkClusterStatusRecorder
    extends StatusRecorder<ClusterStatus, SparkCluster, SparkClusterStatusListener> {
  /**
   * Constructs a new SparkClusterStatusRecorder.
   *
   * @param statusListeners A list of SparkClusterStatusListener instances.
   */
  public SparkClusterStatusRecorder(List<SparkClusterStatusListener> statusListeners) {
    super(statusListeners, ClusterStatus.class, SparkCluster.class);
  }

  /**
   * Appends a new state to the cluster status and persists the updated status.
   *
   * @param context The SparkClusterContext for the cluster.
   * @param newState The new ClusterState to append.
   */
  public void appendNewStateAndPersist(SparkClusterContext context, ClusterState newState) {
    ClusterStatus updatedStatus = context.getResource().getStatus().appendNewState(newState);
    persistStatus(context, updatedStatus);
  }
}
