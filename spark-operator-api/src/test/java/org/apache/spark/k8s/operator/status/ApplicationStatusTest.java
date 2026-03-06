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

import static org.apache.spark.k8s.operator.status.ApplicationStateSummary.Submitted;
import static org.apache.spark.k8s.operator.status.ApplicationStateSummary.Succeeded;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.spec.ResourceRetainPolicy;
import org.apache.spark.k8s.operator.spec.RestartConfig;
import org.apache.spark.k8s.operator.spec.RestartPolicy;

class ApplicationStatusTest {

  @Test
  void testInitStatus() {
    ApplicationStatus applicationStatus = new ApplicationStatus();
    assertEquals(Submitted, applicationStatus.currentState.currentStateSummary);
    assertEquals(1, applicationStatus.getStateTransitionHistory().size());
    assertEquals(
        applicationStatus.currentState, applicationStatus.getStateTransitionHistory().get(0L));
  }

  @Test
  void testAppendNewState() {
    ApplicationStatus applicationStatus = new ApplicationStatus();
    ApplicationState newState = new ApplicationState(ApplicationStateSummary.RunningHealthy, "foo");
    ApplicationStatus newStatus = applicationStatus.appendNewState(newState);
    assertEquals(2, newStatus.getStateTransitionHistory().size());
    assertEquals(newState, newStatus.getStateTransitionHistory().get(1L));
  }

  @Test
  void testTerminateOrRestartWithoutRetry() {
    RestartConfig noRetryConfig = new RestartConfig();
    noRetryConfig.setRestartPolicy(RestartPolicy.Never);
    String messageOverride = "foo";

    // without retry
    ApplicationStatus status =
        new ApplicationStatus().appendNewState(new ApplicationState(Succeeded, "bar"));
    ApplicationStatus updatedStatusReleaseResource =
        status.terminateOrRestart(
            noRetryConfig, ResourceRetainPolicy.Never, messageOverride, false);
    assertEquals(
        ApplicationStateSummary.ResourceReleased,
        updatedStatusReleaseResource.getCurrentState().getCurrentStateSummary());
    assertTrue(
        updatedStatusReleaseResource.getCurrentState().getMessage().contains(messageOverride));
    assertEquals(
        0L, updatedStatusReleaseResource.getCurrentAttemptSummary().getAttemptInfo().getId());

    ApplicationStatus updatedStatusRetainResource =
        status.terminateOrRestart(
            noRetryConfig, ResourceRetainPolicy.Always, messageOverride, false);
    assertEquals(
        ApplicationStateSummary.TerminatedWithoutReleaseResources,
        updatedStatusRetainResource.getCurrentState().getCurrentStateSummary());
    assertTrue(
        updatedStatusRetainResource.getCurrentState().getMessage().contains(messageOverride));
    assertEquals(
        0L, updatedStatusRetainResource.getCurrentAttemptSummary().getAttemptInfo().getId());
  }

  @Test
  void testTerminateOrRestartWithRetry() {
    RestartConfig alwaysRetryConfig = new RestartConfig();
    alwaysRetryConfig.setRestartPolicy(RestartPolicy.Always);
    alwaysRetryConfig.setMaxRestartAttempts(1L);
    String messageOverride = "foo";

    ApplicationStatus status =
        new ApplicationStatus().appendNewState(new ApplicationState(Succeeded, "bar"));

    // retry policy set
    ApplicationStatus restartReleaseResource =
        status.terminateOrRestart(
            alwaysRetryConfig, ResourceRetainPolicy.Never, messageOverride, false);
    assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        restartReleaseResource.getCurrentState().getCurrentStateSummary());
    assertTrue(restartReleaseResource.getCurrentState().getMessage().contains(messageOverride));
    assertEquals(1L, restartReleaseResource.getCurrentAttemptSummary().getAttemptInfo().getId());

    ApplicationStatus restartRetainResource =
        status.terminateOrRestart(
            alwaysRetryConfig, ResourceRetainPolicy.Always, messageOverride, false);
    assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        restartRetainResource.getCurrentState().getCurrentStateSummary());
    assertTrue(restartRetainResource.getCurrentState().getMessage().contains(messageOverride));
    assertEquals(1L, restartRetainResource.getCurrentAttemptSummary().getAttemptInfo().getId());

    // trim state history for new restart
    ApplicationStatus restartTrimHistory =
        status.terminateOrRestart(
            alwaysRetryConfig, ResourceRetainPolicy.Never, messageOverride, true);
    assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        restartTrimHistory.getCurrentState().getCurrentStateSummary());
    assertTrue(restartTrimHistory.getCurrentState().getMessage().contains(messageOverride));
    assertEquals(1L, restartTrimHistory.getCurrentAttemptSummary().getAttemptInfo().getId());
    assertEquals(1L, restartTrimHistory.getStateTransitionHistory().size());
    assertNotNull(restartTrimHistory.getPreviousAttemptSummary().getStateTransitionHistory());
    assertEquals(
        status.getStateTransitionHistory(),
        restartTrimHistory.getPreviousAttemptSummary().getStateTransitionHistory());

    ApplicationStatus restartRetainResourceTrimHistory =
        status.terminateOrRestart(
            alwaysRetryConfig, ResourceRetainPolicy.Always, messageOverride, true);
    assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        restartRetainResourceTrimHistory.getCurrentState().getCurrentStateSummary());
    assertTrue(
        restartRetainResourceTrimHistory.getCurrentState().getMessage().contains(messageOverride));
    assertEquals(
        1L, restartRetainResourceTrimHistory.getCurrentAttemptSummary().getAttemptInfo().getId());
    assertNotNull(
        restartRetainResourceTrimHistory.getPreviousAttemptSummary().getStateTransitionHistory());
    assertEquals(
        status.getStateTransitionHistory(),
        restartRetainResourceTrimHistory.getPreviousAttemptSummary().getStateTransitionHistory());

    // retry policy set but max retry attempt reached
    alwaysRetryConfig.setMaxRestartAttempts(1L);
    ApplicationStatus restartFailed =
        restartReleaseResource.appendNewState(
            new ApplicationState(ApplicationStateSummary.Failed, "bar"));
    ApplicationStatus maxRestartExceededReleaseResource =
        restartFailed.terminateOrRestart(
            alwaysRetryConfig, ResourceRetainPolicy.Never, messageOverride, false);
    assertEquals(
        ApplicationStateSummary.ResourceReleased,
        maxRestartExceededReleaseResource.getCurrentState().getCurrentStateSummary());
    assertTrue(
        maxRestartExceededReleaseResource.getCurrentState().getMessage().contains(messageOverride));
    assertEquals(
        1L, maxRestartExceededReleaseResource.getCurrentAttemptSummary().getAttemptInfo().getId());

    ApplicationStatus maxRestartExceededRetainResource =
        restartFailed.terminateOrRestart(
            alwaysRetryConfig, ResourceRetainPolicy.Always, messageOverride, false);
    assertEquals(
        ApplicationStateSummary.TerminatedWithoutReleaseResources,
        maxRestartExceededRetainResource.getCurrentState().getCurrentStateSummary());
    assertTrue(
        maxRestartExceededRetainResource.getCurrentState().getMessage().contains(messageOverride));
    assertEquals(
        1L, maxRestartExceededRetainResource.getCurrentAttemptSummary().getAttemptInfo().getId());
  }

  @Test
  void testTerminateOrRestartWithRestartCounterReset() throws Exception {
    RestartConfig restartConfigWithCounter = new RestartConfig();
    restartConfigWithCounter.setRestartPolicy(RestartPolicy.Always);
    restartConfigWithCounter.setMaxRestartAttempts(1L);
    restartConfigWithCounter.setRestartCounterResetMillis(300000L); // 5 minutes
    String messageOverride = "restart counter test";

    // Create a status with states spanning more than 5 minutes
    Instant now = Instant.now();
    Instant tenMinutesAgo = now.minus(Duration.ofMinutes(10));

    ApplicationStatus status = createInitialStatusWithSubmittedTime(tenMinutesAgo);

    status =
        status
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverReady, ""))
            .appendNewState(new ApplicationState(ApplicationStateSummary.RunningHealthy, ""))
            .appendNewState(new ApplicationState(Succeeded, ""));

    // Test restart with counter reset (duration > restartCounterResetMillis)
    ApplicationStatus restartWithReset =
        status.terminateOrRestart(
            restartConfigWithCounter, ResourceRetainPolicy.Never, messageOverride, false);

    assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        restartWithReset.getCurrentState().getCurrentStateSummary());
    assertTrue(restartWithReset.getCurrentState().getMessage().contains(messageOverride));
    // Counter should be reset in current attempt therefore it's 1 in the new attempt, next attempt
    // ID is also 1
    assertEquals(1L, restartWithReset.getCurrentAttemptSummary().getAttemptInfo().getId());
    assertEquals(
        1L, restartWithReset.getCurrentAttemptSummary().getAttemptInfo().getRestartCounter());

    // Test reset restart counter with previous attempt in history
    Instant tenMinutesLater = now.plus(Duration.ofMinutes(10));
    ApplicationState secondAttemptStoppingState = new ApplicationState(Succeeded, "");
    secondAttemptStoppingState.setLastTransitionTime(tenMinutesLater.toString());
    status =
        restartWithReset
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverReady, ""))
            .appendNewState(new ApplicationState(ApplicationStateSummary.RunningHealthy, ""))
            .appendNewState(secondAttemptStoppingState);

    ApplicationStatus secondAttemptRestart =
        status.terminateOrRestart(
            restartConfigWithCounter, ResourceRetainPolicy.Never, messageOverride, false);
    assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        secondAttemptRestart.getCurrentState().getCurrentStateSummary());
    // Counter should be reset in current attempt therefore it's again 1 in the new attempt, next
    // attempt ID is incremented to 2
    assertEquals(2L, secondAttemptRestart.getCurrentAttemptSummary().getAttemptInfo().getId());
    assertEquals(
        1L, secondAttemptRestart.getCurrentAttemptSummary().getAttemptInfo().getRestartCounter());

    // validate status with history trimmed
    ApplicationStatus secondAttemptRestartTrimmed =
        status.terminateOrRestart(
            restartConfigWithCounter, ResourceRetainPolicy.Never, messageOverride, true);
    assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        secondAttemptRestartTrimmed.getCurrentState().getCurrentStateSummary());
    assertTrue(
        secondAttemptRestartTrimmed.getCurrentState().getMessage().contains(messageOverride));
    assertEquals(
        2L, secondAttemptRestartTrimmed.getCurrentAttemptSummary().getAttemptInfo().getId());
    assertEquals(
        1L,
        secondAttemptRestartTrimmed
            .getCurrentAttemptSummary()
            .getAttemptInfo()
            .getRestartCounter());

    // Test restart without counter reset (duration < restartCounterResetMillis)
    Instant twoMinutesLater = now.plus(Duration.ofMinutes(2));
    ApplicationState thirdAttemptEnd = new ApplicationState(Succeeded, "recent");
    thirdAttemptEnd.setLastTransitionTime(twoMinutesLater.toString());

    status =
        secondAttemptRestart
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(thirdAttemptEnd);
    ApplicationStatus thirdAttemptTerminate =
        status.terminateOrRestart(
            restartConfigWithCounter, ResourceRetainPolicy.Never, messageOverride, false);
    assertEquals(
        ApplicationStateSummary.ResourceReleased,
        thirdAttemptTerminate.getCurrentState().getCurrentStateSummary());
    // Counter should not be reset in current attempt
    assertEquals(2L, thirdAttemptTerminate.getCurrentAttemptSummary().getAttemptInfo().getId());
    assertEquals(
        1L, thirdAttemptTerminate.getCurrentAttemptSummary().getAttemptInfo().getRestartCounter());

    // Test restart without counter reset in a trimmed status
    status =
        secondAttemptRestartTrimmed
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(thirdAttemptEnd);
    ApplicationStatus thirdAttemptTerminateTrimmed =
        status.terminateOrRestart(
            restartConfigWithCounter, ResourceRetainPolicy.Never, messageOverride, true);
    assertEquals(
        ApplicationStateSummary.ResourceReleased,
        thirdAttemptTerminateTrimmed.getCurrentState().getCurrentStateSummary());
    assertTrue(restartWithReset.getCurrentState().getMessage().contains(messageOverride));
    // Counter should not be reset in current attempt
    assertEquals(
        2L, thirdAttemptTerminateTrimmed.getCurrentAttemptSummary().getAttemptInfo().getId());
    assertEquals(
        1L,
        thirdAttemptTerminateTrimmed
            .getCurrentAttemptSummary()
            .getAttemptInfo()
            .getRestartCounter());
  }

  @Test
  void testFindFirstStateOfCurrentAttempt() throws Exception {
    // Test with single state (Submitted)
    ApplicationStatus status = new ApplicationStatus();
    ApplicationState firstState = status.findFirstStateOfCurrentAttempt();
    assertEquals(Submitted, firstState.getCurrentStateSummary());

    // Test with multiple states including initializing state
    ApplicationStatus statusWithRestart =
        new ApplicationStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(new ApplicationState(ApplicationStateSummary.RunningHealthy, ""))
            .appendNewState(new ApplicationState(ApplicationStateSummary.Failed, ""))
            .appendNewState(new ApplicationState(ApplicationStateSummary.ScheduledToRestart, ""));

    ApplicationState firstStateAfterRestart = statusWithRestart.findFirstStateOfCurrentAttempt();
    assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        firstStateAfterRestart.getCurrentStateSummary());

    // Test with states but no initializing state (should return first entry)
    Map<Long, ApplicationState> history = new TreeMap<>();
    history.put(0L, new ApplicationState(ApplicationStateSummary.DriverRequested, ""));
    history.put(1L, new ApplicationState(ApplicationStateSummary.RunningHealthy, ""));
    history.put(2L, new ApplicationState(Succeeded, ""));
    ApplicationStatus statusNoInitializing =
        new ApplicationStatus(
            new ApplicationState(Succeeded, ""),
            history,
            new ApplicationAttemptSummary(),
            new ApplicationAttemptSummary());

    ApplicationState firstStateNoInit = statusNoInitializing.findFirstStateOfCurrentAttempt();
    assertEquals(
        ApplicationStateSummary.DriverRequested, firstStateNoInit.getCurrentStateSummary());
  }

  @Test
  void testCalculateCurrentAttemptDuration() throws Exception {
    // Test with barely empty status
    ApplicationStatus status = new ApplicationStatus();
    Duration duration = status.calculateCurrentAttemptDuration();
    assertNotNull(duration);
    assertTrue(duration.toMillis() >= 0);

    // Test with multiple states
    ApplicationStatus statusWithStates =
        new ApplicationStatus()
            .appendNewState(new ApplicationState(Submitted, ""))
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(new ApplicationState(ApplicationStateSummary.RunningHealthy, ""));

    Duration durationMultipleStates = statusWithStates.calculateCurrentAttemptDuration();
    assertNotNull(durationMultipleStates);
    assertTrue(durationMultipleStates.toMillis() >= 0);

    // Test with restart scenario - duration should be calculated from ScheduledToRestart state
    // Create states with explicit timestamps
    Instant now = Instant.now();
    Instant oneHourAgo = now.minus(Duration.ofHours(1));
    Instant tenMinutesAgo = now.minus(Duration.ofMinutes(10));

    ApplicationState expectedSecondAttemptStart =
        new ApplicationState(ApplicationStateSummary.ScheduledToRestart, "");
    expectedSecondAttemptStart.setLastTransitionTime(tenMinutesAgo.toString());
    ApplicationState expectedSecondAttemptEnd =
        new ApplicationState(ApplicationStateSummary.Failed, "");

    ApplicationStatus statusWithRestarts =
        createInitialStatusWithSubmittedTime(oneHourAgo)
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(new ApplicationState(ApplicationStateSummary.RunningHealthy, ""))
            .appendNewState(new ApplicationState(ApplicationStateSummary.Failed, ""))
            .appendNewState(expectedSecondAttemptStart)
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(expectedSecondAttemptEnd);

    // Verify it finds ScheduledToRestart as the first state of current attempt
    ApplicationState firstState = statusWithRestarts.findFirstStateOfCurrentAttempt();
    assertEquals(expectedSecondAttemptStart, firstState);

    // Verify duration is calculated from ScheduledToRestart state (10 minutes ago)
    Duration durationAfterRestart = statusWithRestarts.calculateCurrentAttemptDuration();
    assertNotNull(durationAfterRestart);

    Duration expectedDuration =
        Duration.between(
            tenMinutesAgo, Instant.parse(expectedSecondAttemptEnd.getLastTransitionTime()));
    assertEquals(expectedDuration, durationAfterRestart);
  }

  private ApplicationStatus createInitialStatusWithSubmittedTime(Instant submittedTime) {
    ApplicationStatus status = new ApplicationStatus();
    ApplicationState submittedState = status.getStateTransitionHistory().get(0L);
    submittedState.setLastTransitionTime(submittedTime.toString());
    return status;
  }

  @Test
  void terminateOrRestartUsesFailureOverrideForFailedState() {
    RestartConfig config =
        RestartConfig.builder()
            .restartPolicy(RestartPolicy.Always)
            .maxRestartAttempts(10L)
            .maxRestartOnFailure(0L)
            .build();

    ApplicationStatus status =
        new ApplicationStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(new ApplicationState(ApplicationStateSummary.RunningHealthy, ""))
            .appendNewState(new ApplicationState(ApplicationStateSummary.Failed, "error"));

    ApplicationStatus restarted =
        status.terminateOrRestart(config, ResourceRetainPolicy.Never, null, false);

    assertEquals(
        ApplicationStateSummary.ResourceReleased,
        restarted.getCurrentState().getCurrentStateSummary());
    assertEquals(0L, restarted.getCurrentAttemptSummary().getAttemptInfo().getId());
  }

  @Test
  void terminateOrRestartUsesGeneralMaxAttemptsForNonFailureState() {
    RestartConfig config =
        RestartConfig.builder()
            .restartPolicy(RestartPolicy.Always)
            .maxRestartAttempts(10L)
            .maxRestartOnFailure(0L)
            .build();

    ApplicationStatus status =
        new ApplicationStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(new ApplicationState(ApplicationStateSummary.RunningHealthy, ""))
            .appendNewState(new ApplicationState(Succeeded, "completed"));

    ApplicationStatus restarted =
        status.terminateOrRestart(config, ResourceRetainPolicy.Never, null, false);

    assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        restarted.getCurrentState().getCurrentStateSummary());
    assertEquals(1L, restarted.getCurrentAttemptSummary().getAttemptInfo().getId());
  }

  @Test
  void terminateOrRestartSchedulingFailureUsesSchedulingOverride() {
    RestartConfig config =
        RestartConfig.builder()
            .restartPolicy(RestartPolicy.Always)
            .maxRestartAttempts(10L)
            .maxRestartOnFailure(10L)
            .maxRestartOnSchedulingFailure(0L)
            .build();

    ApplicationStatus status =
        new ApplicationStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(
                new ApplicationState(ApplicationStateSummary.SchedulingFailure, "quota exceeded"));

    ApplicationStatus restarted =
        status.terminateOrRestart(config, ResourceRetainPolicy.Never, null, false);

    assertEquals(
        ApplicationStateSummary.ResourceReleased,
        restarted.getCurrentState().getCurrentStateSummary());
    assertEquals(0L, restarted.getCurrentAttemptSummary().getAttemptInfo().getId());
  }

  @Test
  void terminateOrRestartSchedulingFailureFallsBackToGeneralFailureOverride() {
    RestartConfig config =
        RestartConfig.builder()
            .restartPolicy(RestartPolicy.Always)
            .maxRestartAttempts(10L)
            .maxRestartOnFailure(0L)
            .build();

    ApplicationStatus status =
        new ApplicationStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(
                new ApplicationState(
                    ApplicationStateSummary.SchedulingFailure, "resources unavailable"));

    ApplicationStatus restarted =
        status.terminateOrRestart(config, ResourceRetainPolicy.Never, null, false);

    assertEquals(
        ApplicationStateSummary.ResourceReleased,
        restarted.getCurrentState().getCurrentStateSummary());
    assertEquals(0L, restarted.getCurrentAttemptSummary().getAttemptInfo().getId());
  }

  @Test
  void consecutiveFailureCounterResetsOnSuccess() {
    RestartConfig config =
        RestartConfig.builder()
            .restartPolicy(RestartPolicy.Always)
            .maxRestartAttempts(10L)
            .maxRestartOnFailure(2L) // Only 2 consecutive failures allowed
            .build();

    ApplicationStatus status = new ApplicationStatus();
    // simulate a F -> S -> F -> F -> F scenario
    // Attempt 1: Fails
    status =
        status
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(new ApplicationState(ApplicationStateSummary.Failed, "error1"));

    status = status.terminateOrRestart(config, ResourceRetainPolicy.Never, null, false);
    assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        status.getCurrentState().getCurrentStateSummary());
    assertEquals(1L, status.getCurrentAttemptSummary().getAttemptInfo().getRestartCounter());
    assertEquals(
        1L, status.getCurrentAttemptSummary().getAttemptInfo().getFailureRestartCounter());

    // Attempt 2: Succeeds - should reset failure counter
    status =
        status
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(new ApplicationState(Succeeded, "success1"));

    status = status.terminateOrRestart(config, ResourceRetainPolicy.Never, null, false);
    assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        status.getCurrentState().getCurrentStateSummary());
    assertEquals(2L, status.getCurrentAttemptSummary().getAttemptInfo().getRestartCounter());
    // Failure counter should be reset to 0
    assertEquals(
        0L, status.getCurrentAttemptSummary().getAttemptInfo().getFailureRestartCounter());

    // Attempt 3: Fails again - failure counter restarts from 1
    status =
        status
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(new ApplicationState(ApplicationStateSummary.Failed, "error2"));

    status = status.terminateOrRestart(config, ResourceRetainPolicy.Never, null, false);
    assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        status.getCurrentState().getCurrentStateSummary());
    assertEquals(3L, status.getCurrentAttemptSummary().getAttemptInfo().getRestartCounter());
    // Failure counter should be 1 (not 2, because success reset it)
    assertEquals(
        1L, status.getCurrentAttemptSummary().getAttemptInfo().getFailureRestartCounter());

    // Attempt 4: Fails again - 2 consecutive failures now
    status =
        status
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(new ApplicationState(ApplicationStateSummary.Failed, "error3"));

    status = status.terminateOrRestart(config, ResourceRetainPolicy.Never, null, false);
    assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        status.getCurrentState().getCurrentStateSummary());
    assertEquals(4L, status.getCurrentAttemptSummary().getAttemptInfo().getRestartCounter());
    assertEquals(
        2L, status.getCurrentAttemptSummary().getAttemptInfo().getFailureRestartCounter());

    // Attempt 5: Fails for third consecutive time - should exceed limit
    status =
        status
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(new ApplicationState(ApplicationStateSummary.Failed, "error4"));

    status = status.terminateOrRestart(config, ResourceRetainPolicy.Never, null, false);
    // Should stop because consecutive failure counter 3 > maxRestartOnFailure 2
    assertEquals(
        ApplicationStateSummary.ResourceReleased,
        status.getCurrentState().getCurrentStateSummary());
  }

  @Test
  void schedulingFailureIncrementsBothConsecutiveCounters() {
    RestartConfig config =
        RestartConfig.builder()
            .restartPolicy(RestartPolicy.Always)
            .maxRestartAttempts(10L)
            .maxRestartOnFailure(2L) // Only 2 consecutive general failures
            .maxRestartOnSchedulingFailure(null) // No specific override
            .build();

    ApplicationStatus status = new ApplicationStatus();

    // Attempt 1: Scheduling failure
    status =
        status
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(
                new ApplicationState(ApplicationStateSummary.SchedulingFailure, "quota1"));

    status = status.terminateOrRestart(config, ResourceRetainPolicy.Never, null, false);
    assertEquals(1L, status.getCurrentAttemptSummary().getAttemptInfo().getFailureRestartCounter());
    assertEquals(
        1L,
        status.getCurrentAttemptSummary().getAttemptInfo().getSchedulingFailureRestartCounter());

    // Attempt 2: Another scheduling failure
    status =
        status
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(
                new ApplicationState(ApplicationStateSummary.SchedulingFailure, "quota2"));

    status = status.terminateOrRestart(config, ResourceRetainPolicy.Never, null, false);
    assertEquals(2L, status.getCurrentAttemptSummary().getAttemptInfo().getFailureRestartCounter());
    assertEquals(
        2L,
        status.getCurrentAttemptSummary().getAttemptInfo().getSchedulingFailureRestartCounter());

    // Attempt 3: Third consecutive scheduling failure - should exceed general failure limit
    status =
        status
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(
                new ApplicationState(ApplicationStateSummary.SchedulingFailure, "quota3"));

    status = status.terminateOrRestart(config, ResourceRetainPolicy.Never, null, false);
    // Should stop because consecutive failure counter 3 > maxRestartOnFailure 2
    assertEquals(
        ApplicationStateSummary.ResourceReleased,
        status.getCurrentState().getCurrentStateSummary());
  }

  @Test
  void generalLimitEnforcedEvenForFailures() {
    RestartConfig config =
        RestartConfig.builder()
            .restartPolicy(RestartPolicy.Always)
            .maxRestartAttempts(2L) // General limit
            .maxRestartOnFailure(10L)
            .build();

    ApplicationStatus status = new ApplicationStatus();

    // Attempt 1: Succeeds
    status =
        status
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(new ApplicationState(Succeeded, "success1"));

    status = status.terminateOrRestart(config, ResourceRetainPolicy.Never, null, false);
    assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        status.getCurrentState().getCurrentStateSummary());
    assertEquals(1L, status.getCurrentAttemptSummary().getAttemptInfo().getRestartCounter());

    // Attempt 2: Succeeds
    status =
        status
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(new ApplicationState(Succeeded, "success2"));

    status = status.terminateOrRestart(config, ResourceRetainPolicy.Never, null, false);
    assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        status.getCurrentState().getCurrentStateSummary());
    assertEquals(2L, status.getCurrentAttemptSummary().getAttemptInfo().getRestartCounter());

    // Attempt 3: Fails - should stop due to general limit even though failure limit is 10
    status =
        status
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(new ApplicationState(ApplicationStateSummary.Failed, "error"));

    status = status.terminateOrRestart(config, ResourceRetainPolicy.Never, null, false);
    // Should stop because general counter >= general limit
    assertEquals(
        ApplicationStateSummary.ResourceReleased,
        status.getCurrentState().getCurrentStateSummary());
    assertTrue(
        status
            .getCurrentState()
            .getMessage()
            .contains("The maximum number of restart attempts (2) has been exceeded."));
  }

  @Test
  void mixedFailureTypesResetIndependently() {
    RestartConfig config =
        RestartConfig.builder()
            .restartPolicy(RestartPolicy.Always)
            .maxRestartAttempts(10L)
            .maxRestartOnFailure(3L)
            .maxRestartOnSchedulingFailure(2L)
            .build();

    ApplicationStatus status = new ApplicationStatus();

    // Attempt 1: Scheduling failure
    status =
        status
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(
                new ApplicationState(ApplicationStateSummary.SchedulingFailure, "quota"));

    status = status.terminateOrRestart(config, ResourceRetainPolicy.Never, null, false);
    assertEquals(1L, status.getCurrentAttemptSummary().getAttemptInfo().getFailureRestartCounter());
    assertEquals(
        1L,
        status.getCurrentAttemptSummary().getAttemptInfo().getSchedulingFailureRestartCounter());

    // Attempt 2: Regular failure (not scheduling)
    status =
        status
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(new ApplicationState(ApplicationStateSummary.Failed, "error"));

    status = status.terminateOrRestart(config, ResourceRetainPolicy.Never, null, false);
    assertEquals(2L, status.getCurrentAttemptSummary().getAttemptInfo().getFailureRestartCounter());
    // Scheduling failure counter reset
    assertEquals(
        0L,
        status.getCurrentAttemptSummary().getAttemptInfo().getSchedulingFailureRestartCounter());

    // Attempt 3: Success - resets both counters
    status =
        status
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""))
            .appendNewState(new ApplicationState(Succeeded, "success"));

    status = status.terminateOrRestart(config, ResourceRetainPolicy.Never, null, false);
    assertEquals(0L, status.getCurrentAttemptSummary().getAttemptInfo().getFailureRestartCounter());
    assertEquals(
        0L,
        status.getCurrentAttemptSummary().getAttemptInfo().getSchedulingFailureRestartCounter());
  }
}
