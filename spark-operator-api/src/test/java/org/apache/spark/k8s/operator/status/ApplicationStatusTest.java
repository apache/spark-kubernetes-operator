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

import static org.apache.spark.k8s.operator.status.ApplicationStateSummary.SUBMITTED;
import static org.apache.spark.k8s.operator.status.ApplicationStateSummary.SUCCEEDED;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.spec.ResourceRetainPolicy;
import org.apache.spark.k8s.operator.spec.RestartConfig;
import org.apache.spark.k8s.operator.spec.RestartPolicy;

class ApplicationStatusTest {

  @Test
  void testInitStatus() {
    ApplicationStatus applicationStatus = new ApplicationStatus();
    Assertions.assertEquals(SUBMITTED, applicationStatus.currentState.currentStateSummary);
    Assertions.assertEquals(1, applicationStatus.stateTransitionHistory.size());
    Assertions.assertEquals(
        applicationStatus.currentState, applicationStatus.stateTransitionHistory.get(0L));
  }

  @Test
  void testAppendNewState() {
    ApplicationStatus applicationStatus = new ApplicationStatus();
    ApplicationState newState =
        new ApplicationState(ApplicationStateSummary.RUNNING_HEALTHY, "foo");
    ApplicationStatus newStatus = applicationStatus.appendNewState(newState);
    Assertions.assertEquals(2, newStatus.stateTransitionHistory.size());
    Assertions.assertEquals(newState, newStatus.stateTransitionHistory.get(1L));
  }

  @Test
  void testTerminateOrRestart() {
    RestartConfig noRetryConfig = new RestartConfig();
    RestartConfig alwaysRetryConfig = new RestartConfig();
    noRetryConfig.setRestartPolicy(RestartPolicy.Never);
    alwaysRetryConfig.setRestartPolicy(RestartPolicy.Always);
    alwaysRetryConfig.setMaxRestartAttempts(1L);
    String messageOverride = "foo";

    // without retry
    ApplicationStatus status1 =
        new ApplicationStatus().appendNewState(new ApplicationState(SUCCEEDED, "bar"));
    ApplicationStatus updatedStatus11 =
        status1.terminateOrRestart(
            noRetryConfig, ResourceRetainPolicy.Never, messageOverride, false);
    Assertions.assertEquals(
        ApplicationStateSummary.RESOURCE_RELEASED,
        updatedStatus11.getCurrentState().getCurrentStateSummary());
    Assertions.assertTrue(updatedStatus11.getCurrentState().getMessage().contains(messageOverride));
    Assertions.assertEquals(0L, updatedStatus11.currentAttemptSummary.attemptInfo.id);

    ApplicationStatus updatedStatus12 =
        status1.terminateOrRestart(
            noRetryConfig, ResourceRetainPolicy.Always, messageOverride, false);
    Assertions.assertEquals(
        ApplicationStateSummary.TERMINATED_WITHOUT_RELEASE_RESOURCES,
        updatedStatus12.getCurrentState().getCurrentStateSummary());
    Assertions.assertTrue(updatedStatus12.getCurrentState().getMessage().contains(messageOverride));
    Assertions.assertEquals(0L, updatedStatus12.currentAttemptSummary.attemptInfo.id);

    // retry policy set
    ApplicationStatus updatedStatus13 =
        status1.terminateOrRestart(
            alwaysRetryConfig, ResourceRetainPolicy.Never, messageOverride, false);
    Assertions.assertEquals(
        ApplicationStateSummary.SCHEDULED_TO_RESTART,
        updatedStatus13.getCurrentState().getCurrentStateSummary());
    Assertions.assertTrue(updatedStatus13.getCurrentState().getMessage().contains(messageOverride));
    Assertions.assertEquals(1L, updatedStatus13.currentAttemptSummary.attemptInfo.id);

    ApplicationStatus updatedStatus14 =
        status1.terminateOrRestart(
            alwaysRetryConfig, ResourceRetainPolicy.Always, messageOverride, false);
    Assertions.assertEquals(
        ApplicationStateSummary.SCHEDULED_TO_RESTART,
        updatedStatus14.getCurrentState().getCurrentStateSummary());
    Assertions.assertTrue(updatedStatus14.getCurrentState().getMessage().contains(messageOverride));
    Assertions.assertEquals(1L, updatedStatus14.currentAttemptSummary.attemptInfo.id);

    // trim state history for new restart
    ApplicationStatus updatedStatus15 =
        status1.terminateOrRestart(
            alwaysRetryConfig, ResourceRetainPolicy.Never, messageOverride, true);
    Assertions.assertEquals(
        ApplicationStateSummary.SCHEDULED_TO_RESTART,
        updatedStatus15.getCurrentState().getCurrentStateSummary());
    Assertions.assertTrue(updatedStatus13.getCurrentState().getMessage().contains(messageOverride));
    Assertions.assertEquals(1L, updatedStatus15.currentAttemptSummary.attemptInfo.id);
    Assertions.assertEquals(1L, updatedStatus15.stateTransitionHistory.size());
    Assertions.assertNotNull(updatedStatus15.previousAttemptSummary.stateTransitionHistory);
    Assertions.assertEquals(
        status1.getStateTransitionHistory(),
        updatedStatus15.previousAttemptSummary.stateTransitionHistory);

    ApplicationStatus updatedStatus16 =
        status1.terminateOrRestart(
            alwaysRetryConfig, ResourceRetainPolicy.Always, messageOverride, true);
    Assertions.assertEquals(
        ApplicationStateSummary.SCHEDULED_TO_RESTART,
        updatedStatus16.getCurrentState().getCurrentStateSummary());
    Assertions.assertTrue(updatedStatus16.getCurrentState().getMessage().contains(messageOverride));
    Assertions.assertEquals(1L, updatedStatus16.currentAttemptSummary.attemptInfo.id);
    Assertions.assertNotNull(updatedStatus16.previousAttemptSummary.stateTransitionHistory);
    Assertions.assertEquals(
        status1.getStateTransitionHistory(),
        updatedStatus16.previousAttemptSummary.stateTransitionHistory);

    // retry policy set but max retry attempt reached
    alwaysRetryConfig.setMaxRestartAttempts(1L);
    ApplicationStatus status2 =
        updatedStatus14.appendNewState(new ApplicationState(ApplicationStateSummary.FAILED, "bar"));
    ApplicationStatus updatedStatus21 =
        status2.terminateOrRestart(
            alwaysRetryConfig, ResourceRetainPolicy.Never, messageOverride, false);
    Assertions.assertEquals(
        ApplicationStateSummary.RESOURCE_RELEASED,
        updatedStatus21.getCurrentState().getCurrentStateSummary());
    Assertions.assertTrue(updatedStatus21.getCurrentState().getMessage().contains(messageOverride));
    Assertions.assertEquals(1L, updatedStatus21.currentAttemptSummary.attemptInfo.id);

    ApplicationStatus updatedStatus22 =
        status2.terminateOrRestart(
            alwaysRetryConfig, ResourceRetainPolicy.Always, messageOverride, false);
    Assertions.assertEquals(
        ApplicationStateSummary.TERMINATED_WITHOUT_RELEASE_RESOURCES,
        updatedStatus22.getCurrentState().getCurrentStateSummary());
    Assertions.assertTrue(updatedStatus22.getCurrentState().getMessage().contains(messageOverride));
    Assertions.assertEquals(1L, updatedStatus22.currentAttemptSummary.attemptInfo.id);
  }
}
