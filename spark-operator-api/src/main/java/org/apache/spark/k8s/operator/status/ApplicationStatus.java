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
import java.util.SortedMap;
import java.util.TreeMap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.spec.ResourceRetainPolicy;
import org.apache.spark.k8s.operator.spec.RestartConfig;
import org.apache.spark.k8s.operator.spec.RestartPolicy;

/**
 * Represents the status of a Spark application.
 *
 * @since 0.1.0
 */
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ApplicationStatus
    extends BaseStatus<ApplicationStateSummary, ApplicationState, ApplicationAttemptSummary> {

  /**
   * The state of each executor of the current attempt, by executor pod name, recorded only for an
   * application with Dynamic Allocation. Spark Dynamic Allocation adds and removes executors
   * without changing the spec, so a client such as a batch scheduler can follow them through these
   * operator-declared states. An entry is kept after its pod is gone,
   * and the map starts empty with each attempt and when the application is suspended.
   *
   * @since 1.1.0
   */
  @Getter
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  final SortedMap<String, ExecutorState> executorState;

  /**
   * The number of active executors of the current attempt: its executor pods which are pending,
   * including those held by a scheduling gate, or running, including those being deleted. It is
   * counted from the executor pods, not from {@link #executorState}, so it stays exact when the
   * executor states are bounded. A client can size or charge an application's executors from it.
   * It is unset until the attempt has an executor pod.
   *
   * @since 1.1.0
   */
  @Getter final Integer activeExecutors;

  /**
   * The number of ready executors of the current attempt: its executor pods which are running and
   * ready, and not being deleted. Like {@link #activeExecutors}, it is counted from the executor
   * pods. A client can compare it with the minimum number of executors to tell whether the
   * application has started. It is unset until the attempt has an executor pod.
   *
   * @since 1.1.0
   */
  @Getter final Integer readyExecutors;

  /**
   * The highest executor ID of the current attempt so far. Spark assigns increasing executor IDs
   * within an attempt and never reuses them, so it grows with every executor added and never
   * decreases within the attempt, e.g. for a client to tell scale-ups apart. It is
   * recorded only for an application with Dynamic Allocation, and is unset until the attempt has an
   * executor pod.
   *
   * @since 1.1.0
   */
  @Getter final Long maxExecutorId;

  /**
   * Constructs a new, empty ApplicationStatus.
   *
   * @since 0.1.0
   */
  public ApplicationStatus() {
    super(new ApplicationState(), new ApplicationAttemptSummary());
    this.executorState = new TreeMap<>();
    this.activeExecutors = null;
    this.readyExecutors = null;
    this.maxExecutorId = null;
  }

  /**
   * Constructs a new ApplicationStatus with the given state and history.
   *
   * @param currentState The current state of the application.
   * @param stateTransitionHistory The history of state transitions.
   * @param previousAttemptSummary Summary of the previous application attempt.
   * @param currentAttemptSummary Summary of the current application attempt.
   * @since 0.1.0
   */
  public ApplicationStatus(
      ApplicationState currentState,
      Map<Long, ApplicationState> stateTransitionHistory,
      ApplicationAttemptSummary previousAttemptSummary,
      ApplicationAttemptSummary currentAttemptSummary) {
    this(currentState, stateTransitionHistory, previousAttemptSummary, currentAttemptSummary, null);
  }

  /**
   * Constructs a new ApplicationStatus with the given state, history and executor states.
   *
   * @param currentState The current state of the application.
   * @param stateTransitionHistory The history of state transitions.
   * @param previousAttemptSummary Summary of the previous application attempt.
   * @param currentAttemptSummary Summary of the current application attempt.
   * @param executorState The state of each executor of the current attempt by pod name, or null.
   * @since 1.1.0
   */
  public ApplicationStatus(
      ApplicationState currentState,
      Map<Long, ApplicationState> stateTransitionHistory,
      ApplicationAttemptSummary previousAttemptSummary,
      ApplicationAttemptSummary currentAttemptSummary,
      Map<String, ExecutorState> executorState) {
    this(
        currentState,
        stateTransitionHistory,
        previousAttemptSummary,
        currentAttemptSummary,
        executorState,
        null,
        null,
        null);
  }

  /**
   * Constructs a new ApplicationStatus with the given state, history and executors.
   *
   * @param currentState The current state of the application.
   * @param stateTransitionHistory The history of state transitions.
   * @param previousAttemptSummary Summary of the previous application attempt.
   * @param currentAttemptSummary Summary of the current application attempt.
   * @param executorState The state of each executor of the current attempt by pod name, or null.
   * @param activeExecutors The number of active executors of the current attempt, or null.
   * @param readyExecutors The number of ready executors of the current attempt, or null.
   * @param maxExecutorId The highest executor ID of the current attempt so far, or null.
   * @since 1.1.0
   */
  @JsonCreator
  public ApplicationStatus(
      @JsonProperty("currentState") ApplicationState currentState,
      @JsonProperty("stateTransitionHistory") Map<Long, ApplicationState> stateTransitionHistory,
      @JsonProperty("previousAttemptSummary") ApplicationAttemptSummary previousAttemptSummary,
      @JsonProperty("currentAttemptSummary") ApplicationAttemptSummary currentAttemptSummary,
      @JsonProperty("executorState") Map<String, ExecutorState> executorState,
      @JsonProperty("activeExecutors") Integer activeExecutors,
      @JsonProperty("readyExecutors") Integer readyExecutors,
      @JsonProperty("maxExecutorId") Long maxExecutorId) {
    super(currentState, stateTransitionHistory, previousAttemptSummary, currentAttemptSummary);
    this.executorState = executorState == null ? new TreeMap<>() : new TreeMap<>(executorState);
    this.activeExecutors = activeExecutors;
    this.readyExecutors = readyExecutors;
    this.maxExecutorId = maxExecutorId;
  }

  /**
   * Returns a copy of this status with the given executor states, and everything else unchanged.
   *
   * @param newExecutorState The state of each executor of the current attempt by pod name.
   * @return A new ApplicationStatus object with the given executor states.
   * @since 1.1.0
   */
  public ApplicationStatus withExecutorState(Map<String, ExecutorState> newExecutorState) {
    return new ApplicationStatus(
        currentState,
        stateTransitionHistory,
        previousAttemptSummary,
        currentAttemptSummary,
        newExecutorState,
        activeExecutors,
        readyExecutors,
        maxExecutorId);
  }

  /**
   * Returns a copy of this status with the given active and ready executor counts and highest
   * executor ID, and everything else unchanged.
   *
   * @param newActiveExecutors The number of active executors of the current attempt, or null.
   * @param newReadyExecutors The number of ready executors of the current attempt, or null.
   * @param newMaxExecutorId The highest executor ID of the current attempt so far, or null.
   * @return A new ApplicationStatus object with the given executor counts and ID.
   * @since 1.1.0
   */
  public ApplicationStatus withExecutorCounts(
      Integer newActiveExecutors, Integer newReadyExecutors, Long newMaxExecutorId) {
    return new ApplicationStatus(
        currentState,
        stateTransitionHistory,
        previousAttemptSummary,
        currentAttemptSummary,
        executorState,
        newActiveExecutors,
        newReadyExecutors,
        newMaxExecutorId);
  }

  /**
   * Appends a new state to the application's status history and sets it as the current state.
   *
   * @param state The new ApplicationState to append.
   * @return A new ApplicationStatus object with the updated state.
   * @since 0.1.0
   */
  public ApplicationStatus appendNewState(ApplicationState state) {
    // A suspended application releases its executors, so their states start over, as on resume.
    boolean suspended = ApplicationStateSummary.Suspended == state.getCurrentStateSummary();
    return new ApplicationStatus(
        state,
        createUpdatedHistoryWithNewState(state),
        previousAttemptSummary,
        currentAttemptSummary,
        suspended ? null : executorState,
        suspended ? null : activeExecutors,
        suspended ? null : readyExecutors,
        suspended ? null : maxExecutorId);
  }

  /**
   * Creates an updated ApplicationStatus based on the termination or restart logic.
   *
   * @param restartConfig The restart configuration for the application.
   * @param resourceRetainPolicy Ignored. The resources are expected to be released already.
   * @param stateMessageOverride An optional message to override the default state message.
   * @param trimStateTransitionHistory If true, the state transition history will be trimmed.
   * @return An updated ApplicationStatus object.
   * @since 0.1.0
   * @deprecated since 1.1.0, scheduled for removal. {@code resourceRetainPolicy} is ignored, so a
   *     policy other than {@code Never} no longer terminates the application as {@code
   *     TerminatedWithoutReleaseResources}. Use
   *     {@link #terminateOrRestart(RestartConfig, String, boolean)} instead.
   */
  @Deprecated(since = "1.1.0", forRemoval = true)
  public ApplicationStatus terminateOrRestart(
      final RestartConfig restartConfig,
      final ResourceRetainPolicy resourceRetainPolicy,
      String stateMessageOverride,
      boolean trimStateTransitionHistory) {
    return terminateOrRestart(restartConfig, stateMessageOverride, trimStateTransitionHistory);
  }

  /**
   * Creates an updated ApplicationStatus based on the termination or restart logic. The resources
   * of the current attempt are expected to be released already.
   *
   * @param restartConfig The restart configuration for the application.
   * @param stateMessageOverride An optional message to override the default state message.
   * @param trimStateTransitionHistory If true, the state transition history will be trimmed.
   * @return An updated ApplicationStatus object.
   * @since 1.1.0
   */
  public ApplicationStatus terminateOrRestart(
      final RestartConfig restartConfig,
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
      return new ApplicationStatus(
          state,
          createUpdatedHistoryWithNewState(state),
          previousAttemptSummary,
          currentAttemptSummary,
          executorState,
          activeExecutors,
          readyExecutors,
          maxExecutorId);
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
        stateMessage += " " + stateMessageOverride;
      }
      // max number of restart attempt reached
      ApplicationState state =
          new ApplicationState(ApplicationStateSummary.ResourceReleased, stateMessage);
      // still use previous & current attempt summary - they are to be updated only upon
      // new restart
      return new ApplicationStatus(
          state,
          createUpdatedHistoryWithNewState(state),
          previousAttemptSummary,
          currentAttemptSummary,
          executorState,
          activeExecutors,
          readyExecutors,
          maxExecutorId);
    }

    return startNextAttempt(
        new ApplicationState(ApplicationStateSummary.ScheduledToRestart, stateMessageOverride),
        nextAttemptInfo,
        trimStateTransitionHistory);
  }

  /**
   * Starts a new attempt of a suspended application which is resumed. Like a restart, the new
   * attempt gets the next attempt id, and the history of the suspended attempt moves to the
   * previous attempt summary if trimStateTransitionHistory is set. Unlike a restart, it keeps the
   * restart counters, since the suspended attempt neither failed nor finished, so it does not count
   * against the restart limits, whatever the restart policy is. Only the counter of consecutive
   * scheduling failures starts over, since the driver of the suspended attempt was requested, like
   * after an attempt which failed otherwise.
   *
   * <p>It only creates the status of the new attempt. Like {@link
   * #terminateOrRestart(RestartConfig, String, boolean)}, the resources of the suspended attempt
   * are expected to be released already.
   *
   * @param stateMessage The message of the Submitted state which the new attempt starts with.
   * @param trimStateTransitionHistory If true, the state transition history will be trimmed.
   * @return An updated ApplicationStatus object.
   * @throws IllegalStateException if the application is not Suspended.
   * @since 1.1.0
   */
  public ApplicationStatus resume(String stateMessage, boolean trimStateTransitionHistory) {
    if (ApplicationStateSummary.Suspended != currentState.getCurrentStateSummary()) {
      throw new IllegalStateException(
          "Spark application cannot be resumed unless it is suspended, current state is: "
              + currentState);
    }
    ApplicationAttemptInfo attemptInfo = currentAttemptSummary.getAttemptInfo();
    return startNextAttempt(
        new ApplicationState(ApplicationStateSummary.Submitted, stateMessage),
        new ApplicationAttemptInfo(
            attemptInfo.getId() + 1L,
            attemptInfo.getRestartCounter(),
            attemptInfo.getFailureRestartCounter(),
            0L),
        trimStateTransitionHistory);
  }

  /**
   * Starts the next attempt with the given state as its first state.
   *
   * @param state The first state of the next attempt.
   * @param nextAttemptInfo The attempt info of the next attempt.
   * @param trimStateTransitionHistory If true, the state transition history will be trimmed.
   * @return An updated ApplicationStatus object.
   */
  private ApplicationStatus startNextAttempt(
      ApplicationState state,
      ApplicationAttemptInfo nextAttemptInfo,
      boolean trimStateTransitionHistory) {
    // The next attempt has a new driver and new executors, so its executor states start empty.
    ApplicationAttemptSummary nextAttemptSummary = new ApplicationAttemptSummary(nextAttemptInfo);
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
   * <p>The duration is calculated as the time between the first state after the initializing state
   * of the current attempt (normally DriverRequested) and the current state's last transition time,
   * so that time spent in restart backoff or suspended is not counted. If the history has no
   * initializing state, the first entry is used. This is particularly useful for determining
   * whether the restart counter should be reset based on the configured {@code
   * restartCounterResetMillis}.
   *
   * @return A Duration representing the time elapsed since the current attempt started running.
   */
  protected Duration calculateCurrentAttemptDuration() {
    List<ApplicationState> states = new ArrayList<>(stateTransitionHistory.values());
    ApplicationState attemptStart = states.get(0);
    for (int k = states.size() - 1; k >= 0; k--) {
      if (states.get(k).getCurrentStateSummary().isInitializing()) {
        attemptStart = states.get(Math.min(k + 1, states.size() - 1));
        break;
      }
    }
    return Duration.between(
        Instant.parse(attemptStart.getLastTransitionTime()),
        Instant.parse(currentState.getLastTransitionTime()));
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
