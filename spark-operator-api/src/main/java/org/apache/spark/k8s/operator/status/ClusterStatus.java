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

@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterStatus
    extends BaseStatus<ClusterStateSummary, ClusterState, ClusterAttemptSummary> {

  public ClusterStatus() {
    super(new ClusterState(), new ClusterAttemptSummary());
  }

  public ClusterStatus(
      ClusterState currentState,
      Map<Long, ClusterState> stateTransitionHistory,
      ClusterAttemptSummary previousAttemptSummary,
      ClusterAttemptSummary currentAttemptSummary) {
    super(currentState, stateTransitionHistory, previousAttemptSummary, currentAttemptSummary);
  }

  /** Create a new ClusterStatus, set the given latest state as current and update state history */
  public ClusterStatus appendNewState(ClusterState state) {
    return new ClusterStatus(
        state,
        createUpdatedHistoryWithNewState(state),
        previousAttemptSummary,
        currentAttemptSummary);
  }

  private Map<Long, ClusterState> createUpdatedHistoryWithNewState(ClusterState state) {
    TreeMap<Long, ClusterState> updatedHistory = new TreeMap<>(stateTransitionHistory);
    updatedHistory.put(updatedHistory.lastKey() + 1L, state);
    return updatedHistory;
  }
}
