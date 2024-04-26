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

import static org.apache.spark.k8s.operator.Constants.EXCEED_MAX_RETRY_ATTEMPT_MESSAGE;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import org.apache.spark.k8s.operator.spec.ResourceRetainPolicy;
import org.apache.spark.k8s.operator.spec.RestartConfig;
import org.apache.spark.k8s.operator.spec.RestartPolicy;

@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ApplicationStatus
    extends BaseStatus<ApplicationStateSummary, ApplicationState, ApplicationAttemptSummary> {

  public ApplicationStatus() {
    super(new ApplicationState(), new ApplicationAttemptSummary());
  }

  public ApplicationStatus(
      ApplicationState currentState,
      Map<Long, ApplicationState> stateTransitionHistory,
      ApplicationAttemptSummary previousAttemptSummary,
      ApplicationAttemptSummary currentAttemptSummary) {
    super(currentState, stateTransitionHistory, previousAttemptSummary, currentAttemptSummary);
  }

  /**
   * Create a new ApplicationStatus, set the given latest state as current and update state history
   */
  public ApplicationStatus appendNewState(ApplicationState state) {
    return new ApplicationStatus(
        state,
        createUpdatedHistoryWithNewState(state),
        previousAttemptSummary,
        currentAttemptSummary);
  }

  /**
   * Create ApplicationStatus to be updated upon termination of current attempt, with respect to
   * current state and restart config.
   *
   * @param restartConfig restart config for the app
   * @param resourceRetainPolicy resourceRetainPolicy for the app
   * @param stateMessageOverride state message to be applied
   * @param trimStateTransitionHistory if enabled, operator would trim the state history, keeping
   *     only previous and current attempt.
   * @return updated ApplicationStatus
   */
  public ApplicationStatus terminateOrRestart(
      final RestartConfig restartConfig,
      final ResourceRetainPolicy resourceRetainPolicy,
      String stateMessageOverride,
      boolean trimStateTransitionHistory) {
    if (!currentState.currentStateSummary.isStopping()) {
      // application is not stopping, skip
      throw new RuntimeException(
          "Spark application cannot be directly terminated unless in stopping "
              + "state, current state is: "
              + currentState);
    }

    if (!RestartPolicy.attemptRestartOnState(
        restartConfig.getRestartPolicy(), currentState.getCurrentStateSummary())) {
      // no restart configured
      ApplicationState state =
          new ApplicationState(ApplicationStateSummary.ResourceReleased, stateMessageOverride);
      if (ResourceRetainPolicy.Always.equals(resourceRetainPolicy)
          || (ResourceRetainPolicy.OnFailure.equals(resourceRetainPolicy)
              && currentState.currentStateSummary.isFailure())) {
        state = terminateAppWithoutReleaseResource(stateMessageOverride);
      }
      return new ApplicationStatus(
          state,
          createUpdatedHistoryWithNewState(state),
          previousAttemptSummary,
          currentAttemptSummary);
    }

    if (currentAttemptSummary.getAttemptInfo().getId() >= restartConfig.getMaxRestartAttempts()) {
      String stateMessage =
          String.format(EXCEED_MAX_RETRY_ATTEMPT_MESSAGE, restartConfig.getMaxRestartAttempts());
      if (StringUtils.isNotEmpty(stateMessageOverride)) {
        stateMessage += stateMessageOverride;
      }
      // max number of restart attempt reached
      ApplicationState state =
          new ApplicationState(ApplicationStateSummary.ResourceReleased, stateMessage);
      if (ResourceRetainPolicy.Always.equals(resourceRetainPolicy)
          || (ResourceRetainPolicy.OnFailure.equals(resourceRetainPolicy)
              && currentState.currentStateSummary.isFailure())) {
        state = terminateAppWithoutReleaseResource(stateMessage);
      }
      // still use previous & current attempt summary - they are to be updated only upon
      // new restart
      return new ApplicationStatus(
          state,
          createUpdatedHistoryWithNewState(state),
          previousAttemptSummary,
          currentAttemptSummary);
    }

    ApplicationAttemptSummary nextAttemptSummary = new ApplicationAttemptSummary();
    nextAttemptSummary.setAttemptInfo(
        currentAttemptSummary.getAttemptInfo().createNextAttemptInfo());
    ApplicationState state =
        new ApplicationState(ApplicationStateSummary.ScheduledToRestart, stateMessageOverride);

    if (trimStateTransitionHistory) {
      currentAttemptSummary.setStateTransitionHistory(stateTransitionHistory);
      return new ApplicationStatus(
          state,
          Collections.singletonMap(getCurrentStateId() + 1, state),
          currentAttemptSummary,
          nextAttemptSummary);
    } else {
      return new ApplicationStatus(
          state,
          createUpdatedHistoryWithNewState(state),
          currentAttemptSummary,
          nextAttemptSummary);
    }
  }

  private ApplicationState terminateAppWithoutReleaseResource(String stateMessageOverride) {
    String stateMessage =
        "Application is terminated without releasing resources as configured."
            + stateMessageOverride;
    return new ApplicationState(
        ApplicationStateSummary.TerminatedWithoutReleaseResources, stateMessage);
  }

  private Map<Long, ApplicationState> createUpdatedHistoryWithNewState(ApplicationState state) {
    TreeMap<Long, ApplicationState> updatedHistory = new TreeMap<>(stateTransitionHistory);
    updatedHistory.put(updatedHistory.lastKey() + 1L, state);
    return updatedHistory;
  }
}
