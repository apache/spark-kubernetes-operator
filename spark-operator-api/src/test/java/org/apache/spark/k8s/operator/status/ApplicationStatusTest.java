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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.spec.ResourceRetainPolicy;
import org.apache.spark.k8s.operator.spec.RestartConfig;
import org.apache.spark.k8s.operator.spec.RestartPolicy;

class ApplicationStatusTest {

  @Test
  void testInitStatus() {
    ApplicationStatus applicationStatus = new ApplicationStatus();
    Assertions.assertEquals(Submitted, applicationStatus.currentState.currentStateSummary);
    Assertions.assertEquals(1, applicationStatus.getStateTransitionHistory().size());
    Assertions.assertEquals(
        applicationStatus.currentState, applicationStatus.getStateTransitionHistory().get(0L));
  }

  @Test
  void testAppendNewState() {
    ApplicationStatus applicationStatus = new ApplicationStatus();
    ApplicationState newState = new ApplicationState(ApplicationStateSummary.RunningHealthy, "foo");
    ApplicationStatus newStatus = applicationStatus.appendNewState(newState);
    Assertions.assertEquals(2, newStatus.getStateTransitionHistory().size());
    Assertions.assertEquals(newState, newStatus.getStateTransitionHistory().get(1L));
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
    Assertions.assertEquals(
        ApplicationStateSummary.ResourceReleased,
        updatedStatusReleaseResource.getCurrentState().getCurrentStateSummary());
    Assertions.assertTrue(
        updatedStatusReleaseResource.getCurrentState().getMessage().contains(messageOverride));
    Assertions.assertEquals(
        0L, updatedStatusReleaseResource.getCurrentAttemptSummary().getAttemptInfo().getId());

    ApplicationStatus updatedStatusRetainResource =
        status.terminateOrRestart(
            noRetryConfig, ResourceRetainPolicy.Always, messageOverride, false);
    Assertions.assertEquals(
        ApplicationStateSummary.TerminatedWithoutReleaseResources,
        updatedStatusRetainResource.getCurrentState().getCurrentStateSummary());
    Assertions.assertTrue(
        updatedStatusRetainResource.getCurrentState().getMessage().contains(messageOverride));
    Assertions.assertEquals(
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
    Assertions.assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        restartReleaseResource.getCurrentState().getCurrentStateSummary());
    Assertions.assertTrue(
        restartReleaseResource.getCurrentState().getMessage().contains(messageOverride));
    Assertions.assertEquals(
        1L, restartReleaseResource.getCurrentAttemptSummary().getAttemptInfo().getId());

    ApplicationStatus restartRetainResource =
        status.terminateOrRestart(
            alwaysRetryConfig, ResourceRetainPolicy.Always, messageOverride, false);
    Assertions.assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        restartRetainResource.getCurrentState().getCurrentStateSummary());
    Assertions.assertTrue(
        restartRetainResource.getCurrentState().getMessage().contains(messageOverride));
    Assertions.assertEquals(
        1L, restartRetainResource.getCurrentAttemptSummary().getAttemptInfo().getId());

    // trim state history for new restart
    ApplicationStatus restartTrimHistory =
        status.terminateOrRestart(
            alwaysRetryConfig, ResourceRetainPolicy.Never, messageOverride, true);
    Assertions.assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        restartTrimHistory.getCurrentState().getCurrentStateSummary());
    Assertions.assertTrue(
        restartTrimHistory.getCurrentState().getMessage().contains(messageOverride));
    Assertions.assertEquals(
        1L, restartTrimHistory.getCurrentAttemptSummary().getAttemptInfo().getId());
    Assertions.assertEquals(1L, restartTrimHistory.getStateTransitionHistory().size());
    Assertions.assertNotNull(
        restartTrimHistory.getPreviousAttemptSummary().getStateTransitionHistory());
    Assertions.assertEquals(
        status.getStateTransitionHistory(),
        restartTrimHistory.getPreviousAttemptSummary().getStateTransitionHistory());

    ApplicationStatus restartRetainResourceTrimHistory =
        status.terminateOrRestart(
            alwaysRetryConfig, ResourceRetainPolicy.Always, messageOverride, true);
    Assertions.assertEquals(
        ApplicationStateSummary.ScheduledToRestart,
        restartRetainResourceTrimHistory.getCurrentState().getCurrentStateSummary());
    Assertions.assertTrue(
        restartRetainResourceTrimHistory.getCurrentState().getMessage().contains(messageOverride));
    Assertions.assertEquals(
        1L, restartRetainResourceTrimHistory.getCurrentAttemptSummary().getAttemptInfo().getId());
    Assertions.assertNotNull(
        restartRetainResourceTrimHistory.getPreviousAttemptSummary().getStateTransitionHistory());
    Assertions.assertEquals(
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
    Assertions.assertEquals(
        ApplicationStateSummary.ResourceReleased,
        maxRestartExceededReleaseResource.getCurrentState().getCurrentStateSummary());
    Assertions.assertTrue(
        maxRestartExceededReleaseResource.getCurrentState().getMessage().contains(messageOverride));
    Assertions.assertEquals(
        1L, maxRestartExceededReleaseResource.getCurrentAttemptSummary().getAttemptInfo().getId());

    ApplicationStatus maxRestartExceededRetainResource =
        restartFailed.terminateOrRestart(
            alwaysRetryConfig, ResourceRetainPolicy.Always, messageOverride, false);
    Assertions.assertEquals(
        ApplicationStateSummary.TerminatedWithoutReleaseResources,
        maxRestartExceededRetainResource.getCurrentState().getCurrentStateSummary());
    Assertions.assertTrue(
        maxRestartExceededRetainResource.getCurrentState().getMessage().contains(messageOverride));
    Assertions.assertEquals(
        1L, maxRestartExceededRetainResource.getCurrentAttemptSummary().getAttemptInfo().getId());
  }
}
