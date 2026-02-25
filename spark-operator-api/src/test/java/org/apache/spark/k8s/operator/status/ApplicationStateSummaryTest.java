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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class ApplicationStateSummaryTest {

  @Test
  void testIsInitializing() {
    assertTrue(ApplicationStateSummary.Submitted.isInitializing());
    assertTrue(ApplicationStateSummary.ScheduledToRestart.isInitializing());

    assertFalse(ApplicationStateSummary.DriverRequested.isInitializing());
    assertFalse(ApplicationStateSummary.RunningHealthy.isInitializing());
    assertFalse(ApplicationStateSummary.RunningWithPartialCapacity.isInitializing());
    assertFalse(ApplicationStateSummary.Failed.isInitializing());
  }

  @Test
  void testIsStarting() {
    // States after ScheduledToRestart but before RunningHealthy
    assertTrue(ApplicationStateSummary.DriverRequested.isStarting());
    assertTrue(ApplicationStateSummary.DriverStarted.isStarting());
    assertTrue(ApplicationStateSummary.DriverReady.isStarting());
    assertTrue(ApplicationStateSummary.InitializedBelowThresholdExecutors.isStarting());

    // States at or after RunningHealthy are not starting
    assertFalse(ApplicationStateSummary.RunningHealthy.isStarting());
    assertFalse(ApplicationStateSummary.RunningWithPartialCapacity.isStarting());
    assertFalse(ApplicationStateSummary.RunningWithBelowThresholdExecutors.isStarting());
    assertFalse(ApplicationStateSummary.Failed.isStarting());

    // States before ScheduledToRestart are not starting
    assertFalse(ApplicationStateSummary.Submitted.isStarting());
    assertFalse(ApplicationStateSummary.ScheduledToRestart.isStarting());
  }

  @Test
  void testIsStopping() {
    // States after RunningWithBelowThresholdExecutors but not terminated
    assertTrue(ApplicationStateSummary.DriverStartTimedOut.isStopping());
    assertTrue(ApplicationStateSummary.ExecutorsStartTimedOut.isStopping());
    assertTrue(ApplicationStateSummary.DriverReadyTimedOut.isStopping());
    assertTrue(ApplicationStateSummary.Succeeded.isStopping());
    assertTrue(ApplicationStateSummary.Failed.isStopping());
    assertTrue(ApplicationStateSummary.SchedulingFailure.isStopping());
    assertTrue(ApplicationStateSummary.DriverEvicted.isStopping());

    // Running states are not stopping
    assertFalse(ApplicationStateSummary.RunningHealthy.isStopping());
    assertFalse(ApplicationStateSummary.RunningWithPartialCapacity.isStopping());
    assertFalse(ApplicationStateSummary.RunningWithBelowThresholdExecutors.isStopping());

    // Terminated states are not considered "stopping"
    assertFalse(ApplicationStateSummary.ResourceReleased.isStopping());
    assertFalse(ApplicationStateSummary.TerminatedWithoutReleaseResources.isStopping());

    // Earlier states are not stopping
    assertFalse(ApplicationStateSummary.Submitted.isStopping());
    assertFalse(ApplicationStateSummary.DriverRequested.isStopping());
  }

  @Test
  void testIsTerminated() {
    assertTrue(ApplicationStateSummary.ResourceReleased.isTerminated());
    assertTrue(ApplicationStateSummary.TerminatedWithoutReleaseResources.isTerminated());

    assertFalse(ApplicationStateSummary.Submitted.isTerminated());
    assertFalse(ApplicationStateSummary.RunningHealthy.isTerminated());
    assertFalse(ApplicationStateSummary.RunningWithPartialCapacity.isTerminated());
    assertFalse(ApplicationStateSummary.Failed.isTerminated());
    assertFalse(ApplicationStateSummary.Succeeded.isTerminated());
  }

  @Test
  void testIsFailure() {
    assertTrue(ApplicationStateSummary.DriverStartTimedOut.isFailure());
    assertTrue(ApplicationStateSummary.ExecutorsStartTimedOut.isFailure());
    assertTrue(ApplicationStateSummary.SchedulingFailure.isFailure());
    assertTrue(ApplicationStateSummary.DriverEvicted.isFailure());
    assertTrue(ApplicationStateSummary.Failed.isFailure());
    assertTrue(ApplicationStateSummary.DriverReadyTimedOut.isFailure());

    assertFalse(ApplicationStateSummary.Submitted.isFailure());
    assertFalse(ApplicationStateSummary.RunningHealthy.isFailure());
    assertFalse(ApplicationStateSummary.RunningWithPartialCapacity.isFailure());
    assertFalse(ApplicationStateSummary.RunningWithBelowThresholdExecutors.isFailure());
    assertFalse(ApplicationStateSummary.Succeeded.isFailure());
    assertFalse(ApplicationStateSummary.ResourceReleased.isFailure());
  }

  @Test
  void testIsInfrastructureFailure() {
    assertTrue(ApplicationStateSummary.DriverStartTimedOut.isInfrastructureFailure());
    assertTrue(ApplicationStateSummary.ExecutorsStartTimedOut.isInfrastructureFailure());
    assertTrue(ApplicationStateSummary.SchedulingFailure.isInfrastructureFailure());

    assertFalse(ApplicationStateSummary.Failed.isInfrastructureFailure());
    assertFalse(ApplicationStateSummary.DriverEvicted.isInfrastructureFailure());
    assertFalse(ApplicationStateSummary.RunningHealthy.isInfrastructureFailure());
    assertFalse(ApplicationStateSummary.RunningWithPartialCapacity.isInfrastructureFailure());
  }

  @Test
  void testRunningWithPartialCapacityOrdinalPosition() {
    // Verify RunningWithPartialCapacity is positioned correctly
    // It should be after RunningHealthy and before RunningWithBelowThresholdExecutors
    assertTrue(
        ApplicationStateSummary.RunningWithPartialCapacity.ordinal()
            > ApplicationStateSummary.RunningHealthy.ordinal());
    assertTrue(
        ApplicationStateSummary.RunningWithPartialCapacity.ordinal()
            < ApplicationStateSummary.RunningWithBelowThresholdExecutors.ordinal());

    // Verify it's in the right category for helper methods
    assertFalse(ApplicationStateSummary.RunningWithPartialCapacity.isStarting());
    assertFalse(ApplicationStateSummary.RunningWithPartialCapacity.isStopping());
    assertFalse(ApplicationStateSummary.RunningWithPartialCapacity.isTerminated());
    assertFalse(ApplicationStateSummary.RunningWithPartialCapacity.isFailure());
    assertFalse(ApplicationStateSummary.RunningWithPartialCapacity.isInfrastructureFailure());
  }

  @Test
  void testRunningStatesOrder() {
    // Verify the correct ordinal order for running states
    assertTrue(
        ApplicationStateSummary.InitializedBelowThresholdExecutors.ordinal()
            < ApplicationStateSummary.RunningHealthy.ordinal());
    assertTrue(
        ApplicationStateSummary.RunningHealthy.ordinal()
            < ApplicationStateSummary.RunningWithPartialCapacity.ordinal());
    assertTrue(
        ApplicationStateSummary.RunningWithPartialCapacity.ordinal()
            < ApplicationStateSummary.RunningWithBelowThresholdExecutors.ordinal());
    assertTrue(
        ApplicationStateSummary.RunningWithBelowThresholdExecutors.ordinal()
            < ApplicationStateSummary.DriverStartTimedOut.ordinal());
  }
}
