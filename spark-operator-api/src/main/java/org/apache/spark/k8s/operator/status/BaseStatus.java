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
import java.util.SortedMap;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@ToString
@EqualsAndHashCode
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class BaseStatus<S, STATE extends BaseState<S>, AS extends BaseAttemptSummary> {
  @Getter final STATE currentState;
  @Getter final SortedMap<Long, STATE> stateTransitionHistory;
  @Getter final AS previousAttemptSummary;
  @Getter final AS currentAttemptSummary;

  public BaseStatus(STATE initState, AS currentAttemptSummary) {
    this.currentState = initState;
    this.stateTransitionHistory = new TreeMap<>();
    this.stateTransitionHistory.put(0L, initState);
    this.previousAttemptSummary = null;
    this.currentAttemptSummary = currentAttemptSummary;
  }

  public BaseStatus(
      STATE currentState,
      Map<Long, STATE> stateTransitionHistory,
      AS previousAttemptSummary,
      AS currentAttemptSummary) {
    this.currentState = currentState;
    this.stateTransitionHistory = new TreeMap<>(stateTransitionHistory);
    this.previousAttemptSummary = previousAttemptSummary;
    this.currentAttemptSummary = currentAttemptSummary;
  }

  protected long getCurrentStateId() {
    return stateTransitionHistory.lastKey();
  }
}
