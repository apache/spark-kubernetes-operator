/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.kubernetes.operator.status;

import static org.apache.spark.kubernetes.operator.status.ApplicationStateSummary.SUBMITTED;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
}
