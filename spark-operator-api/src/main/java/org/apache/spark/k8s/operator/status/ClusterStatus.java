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

import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Represents the status of a Spark cluster. */
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterStatus
    extends BaseStatus<ClusterStateSummary, ClusterState, ClusterAttemptSummary> {

  /** Constructs a new, empty ClusterStatus. */
  public ClusterStatus() {
    super(new ClusterState(), new ClusterAttemptSummary());
  }

  /**
   * Constructs a new ClusterStatus with the given state and history.
   *
   * @param currentState The current state of the cluster.
   * @param stateTransitionHistory The history of state transitions.
   * @param previousAttemptSummary Summary of the previous cluster attempt.
   * @param currentAttemptSummary Summary of the current cluster attempt.
   */
  public ClusterStatus(
      ClusterState currentState,
      Map<Long, ClusterState> stateTransitionHistory,
      ClusterAttemptSummary previousAttemptSummary,
      ClusterAttemptSummary currentAttemptSummary) {
    super(currentState, stateTransitionHistory, previousAttemptSummary, currentAttemptSummary);
  }

  /**
   * Appends a new state to the cluster's status history and sets it as the current state.
   *
   * @param state The new ClusterState to append.
   * @return A new ClusterStatus object with the updated state.
   */
  public ClusterStatus appendNewState(ClusterState state) {
    return new ClusterStatus(
        state,
        createUpdatedHistoryWithNewState(state),
        previousAttemptSummary,
        currentAttemptSummary);
  }

  /**
   * Creates an updated state transition history with a new state appended.
   *
   * @param state The new ClusterState to append.
   * @return A Map representing the updated state transition history.
   */
  private Map<Long, ClusterState> createUpdatedHistoryWithNewState(ClusterState state) {
    TreeMap<Long, ClusterState> updatedHistory = new TreeMap<>(stateTransitionHistory);
    updatedHistory.put(updatedHistory.lastKey() + 1L, state);
    return updatedHistory;
  }
}
