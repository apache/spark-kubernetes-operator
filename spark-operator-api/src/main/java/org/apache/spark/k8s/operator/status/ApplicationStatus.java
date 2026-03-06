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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.spec.ResourceRetainPolicy;
import org.apache.spark.k8s.operator.spec.RestartConfig;
import org.apache.spark.k8s.operator.spec.RestartPolicy;

/** Represents the status of a Spark application. */
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ApplicationStatus
    extends BaseStatus<ApplicationStateSummary, ApplicationState, ApplicationAttemptSummary> {

  /** Constructs a new, empty ApplicationStatus. */
  public ApplicationStatus() {
    super(new ApplicationState(), new ApplicationAttemptSummary());
  }

  /**
   * Constructs a new ApplicationStatus with the given state and history.
   *
   * @param currentState The current state of the application.
   * @param stateTransitionHistory The history of state transitions.
   * @param previousAttemptSummary Summary of the previous application attempt.
   * @param currentAttemptSummary Summary of the current application attempt.
   */
  public ApplicationStatus(
      ApplicationState currentState,
      Map<Long, ApplicationState> stateTransitionHistory,
      ApplicationAttemptSummary previousAttemptSummary,
      ApplicationAttemptSummary currentAttemptSummary) {
    super(currentState, stateTransitionHistory, previousAttemptSummary, currentAttemptSummary);
  }

  /**
   * Appends a new state to the application's status history and sets it as the current state.
   *
   * @param state The new ApplicationState to append.
   * @return A new ApplicationStatus object with the updated state.
   */
  public ApplicationStatus appendNewState(ApplicationState state) {
    return new ApplicationStatus(
        state,
        createUpdatedHistoryWithNewState(state),
        previousAttemptSummary,
        currentAttemptSummary);
  }

  /**
   * Creates an updated ApplicationStatus based on the termination or restart logic.
   *
   * @param restartConfig The restart configuration for the application.
   * @param resourceRetainPolicy The resource retention policy for the application.
   * @param stateMessageOverride An optional message to override the default state message.
   * @param trimStateTransitionHistory If true, the state transition history will be trimmed.
   * @return An updated ApplicationStatus object.
   */
  public ApplicationStatus terminateOrRestart(
      final RestartConfig restartConfig,
      final ResourceRetainPolicy resourceRetainPolicy,
      String stateMessageOverride,
      boolean trimStateTransitionHistory) {
    if (!currentState.currentStateSummary.isStopping()) {
      // application is not stopping, skip
      throw new IllegalStateException(
          "Spark application cannot be directly terminated unless in stopping "
              + "state, current state is: "
              + currentState);
    }

    if (!RestartPolicy.attemptRestartOnState(
        restartConfig.getRestartPolicy(), currentState.getCurrentStateSummary())) {
      // no restart configured
      ApplicationState state =
          new ApplicationState(ApplicationStateSummary.ResourceReleased, stateMessageOverride);
      if (ResourceRetainPolicy.Always == resourceRetainPolicy
          || ResourceRetainPolicy.OnFailure == resourceRetainPolicy
              && currentState.currentStateSummary.isFailure()) {
        state = terminateAppWithoutReleaseResource(stateMessageOverride);
      }
      return new ApplicationStatus(
          state,
          createUpdatedHistoryWithNewState(state),
          previousAttemptSummary,
          currentAttemptSummary);
    }

    boolean resetRestartCounter = false;
    if (restartConfig.getRestartCounterResetMillis() >= 0L) {
      resetRestartCounter =
          calculateCurrentAttemptDuration()
                  .compareTo(Duration.ofMillis(restartConfig.getRestartCounterResetMillis()))
              >= 0;
    }

    ApplicationStateSummary currentStateSummary = currentState.getCurrentStateSummary();
    ApplicationAttemptInfo nextAttemptInfo = getAttemptInfo(resetRestartCounter,
        currentAttemptSummary.getAttemptInfo(), currentStateSummary);

    boolean exceededLimit =
        nextAttemptInfo.getRestartCounter() > restartConfig.getMaxRestartAttempts();
    String stateMessage = "";

    if (exceededLimit) {
      stateMessage =
          String.format(
              Constants.EXCEED_MAX_RETRY_ATTEMPT_MESSAGE,
              restartConfig.getMaxRestartAttempts());
    } else if (restartConfig.getMaxRestartOnSchedulingFailure() != null
        && ApplicationStateSummary.SchedulingFailure == currentStateSummary) {
      exceededLimit =
          nextAttemptInfo.getSchedulingFailureRestartCounter()
              > restartConfig.getMaxRestartOnSchedulingFailure();
      stateMessage =
          String.format(
              Constants.EXCEED_MAX_RETRY_ATTEMPT_ON_SCHEDULING_FAILURE_MESSAGE,
              restartConfig.getMaxRestartOnSchedulingFailure());
    } else if (restartConfig.getMaxRestartOnFailure() != null
        && currentStateSummary.isFailure()) {
      exceededLimit =
          nextAttemptInfo.getFailureRestartCounter() > restartConfig.getMaxRestartOnFailure();
      stateMessage =
          String.format(
              Constants.EXCEED_MAX_RETRY_ATTEMPT_ON_FAILURE_MESSAGE,
              restartConfig.getMaxRestartOnFailure());
    }

    if (exceededLimit) {
      if (stateMessageOverride != null && !stateMessageOverride.isEmpty()) {
        stateMessage += stateMessageOverride;
      }
      // max number of restart attempt reached
      ApplicationState state =
          new ApplicationState(ApplicationStateSummary.ResourceReleased, stateMessage);
      if (ResourceRetainPolicy.Always == resourceRetainPolicy
          || ResourceRetainPolicy.OnFailure == resourceRetainPolicy
              && currentState.currentStateSummary.isFailure()) {
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

    ApplicationAttemptSummary nextAttemptSummary = new ApplicationAttemptSummary(nextAttemptInfo);
    ApplicationState state =
        new ApplicationState(ApplicationStateSummary.ScheduledToRestart, stateMessageOverride);

    if (trimStateTransitionHistory) {
      // when truncating, put all previous history entries into previous attempt summary
      ApplicationAttemptSummary newPrevSummary =
          new ApplicationAttemptSummary(
              currentAttemptSummary.getAttemptInfo(), stateTransitionHistory);
      return new ApplicationStatus(
          state,
          Map.of(getCurrentStateId() + 1, state),
          newPrevSummary,
          nextAttemptSummary);
    } else {
      // when truncating is disabled, currentAttempt becomes the new 'previous'
      return new ApplicationStatus(
          state,
          createUpdatedHistoryWithNewState(state),
          currentAttemptSummary,
          nextAttemptSummary);
    }
  }

  private ApplicationAttemptInfo getAttemptInfo(boolean resetRestartCounter,
                                                ApplicationAttemptInfo currentAttemptInfo,
                                                ApplicationStateSummary currentStateSummary) {
    long newRestartCounter = resetRestartCounter ? 1L : currentAttemptInfo.getRestartCounter() + 1;
    long newFailureCounter;
    long newSchedulingFailureCounter;
    if (resetRestartCounter) {
      newFailureCounter = 0L;
      newSchedulingFailureCounter = 0L;
    } else if (ApplicationStateSummary.SchedulingFailure == currentStateSummary) {
      newSchedulingFailureCounter = currentAttemptInfo.getSchedulingFailureRestartCounter() + 1;
      newFailureCounter = currentAttemptInfo.getFailureRestartCounter() + 1;
    } else if (currentStateSummary.isFailure()) {
      newFailureCounter = currentAttemptInfo.getFailureRestartCounter() + 1;
      newSchedulingFailureCounter = 0L;
    } else {
      newFailureCounter = 0L;
      newSchedulingFailureCounter = 0L;
    }
    return new ApplicationAttemptInfo(
        currentAttemptInfo.getId() + 1L,
        newRestartCounter,
        newFailureCounter,
        newSchedulingFailureCounter);
  }

  /**
   * Finds the first state of the current application attempt.
   *
   * <p>This method traverses the state transition history in reverse order to find the most recent
   * initializing state (e.g., Submitted or ScheduledToRestart), which marks the beginning of the
   * current attempt. If no initializing state is found, it returns the first entry in the history.
   *
   * @return The ApplicationState representing the start of the current attempt.
   */
  protected ApplicationState findFirstStateOfCurrentAttempt() {
    List<Map.Entry<Long, ApplicationState>> entries =
        new ArrayList<>(stateTransitionHistory.entrySet());
    for (int k = entries.size() - 1; k >= 0; k--) {
      Map.Entry<Long, ApplicationState> entry = entries.get(k);
      if (entry.getValue().getCurrentStateSummary().isInitializing()) {
        return entry.getValue();
      }
    }
    return entries.get(0).getValue();
  }

  /**
   * Calculates the duration of the current application attempt.
   *
   * <p>The duration is calculated as the time between the first state of the current attempt (as
   * determined by {@link #findFirstStateOfCurrentAttempt()}) and the current state's last
   * transition time. This is particularly useful for determining whether the restart counter should
   * be reset based on the configured {@code restartCounterResetMillis}.
   *
   * @return A Duration representing the time elapsed since the start of the current attempt.
   */
  protected Duration calculateCurrentAttemptDuration() {
    ApplicationState firstStateOfCurrentAttempt = findFirstStateOfCurrentAttempt();
    return Duration.between(
        Instant.parse(firstStateOfCurrentAttempt.getLastTransitionTime()),
        Instant.parse(currentState.getLastTransitionTime()));
  }

  /**
   * Creates an ApplicationState indicating that the application is terminated without releasing
   * resources.
   *
   * @param stateMessageOverride An optional message to override the default state message.
   * @return An ApplicationState object for termination without resource release.
   */
  private ApplicationState terminateAppWithoutReleaseResource(String stateMessageOverride) {
    String stateMessage =
        "Application is terminated without releasing resources as configured."
            + stateMessageOverride;
    return new ApplicationState(
        ApplicationStateSummary.TerminatedWithoutReleaseResources, stateMessage);
  }

  /**
   * Creates an updated state transition history with a new state appended.
   *
   * @param state The new ApplicationState to append.
   * @return A Map representing the updated state transition history.
   */
  private Map<Long, ApplicationState> createUpdatedHistoryWithNewState(ApplicationState state) {
    TreeMap<Long, ApplicationState> updatedHistory = new TreeMap<>(stateTransitionHistory);
    updatedHistory.put(updatedHistory.lastKey() + 1L, state);
    return updatedHistory;
  }
}
