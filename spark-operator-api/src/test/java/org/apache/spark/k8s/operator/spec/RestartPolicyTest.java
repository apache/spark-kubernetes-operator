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

package org.apache.spark.k8s.operator.spec;

import static org.apache.spark.k8s.operator.spec.RestartPolicy.OnFailure;
import static org.apache.spark.k8s.operator.spec.RestartPolicy.OnInfrastructureFailure;
import static org.apache.spark.k8s.operator.spec.RestartPolicy.attemptRestartOnState;
import static org.apache.spark.k8s.operator.status.ApplicationStateSummary.DriverEvicted;
import static org.apache.spark.k8s.operator.status.ApplicationStateSummary.DriverReadyTimedOut;
import static org.apache.spark.k8s.operator.status.ApplicationStateSummary.DriverStartTimedOut;
import static org.apache.spark.k8s.operator.status.ApplicationStateSummary.ExecutorsStartTimedOut;
import static org.apache.spark.k8s.operator.status.ApplicationStateSummary.Failed;
import static org.apache.spark.k8s.operator.status.ApplicationStateSummary.Succeeded;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.status.ApplicationStateSummary;

class RestartPolicyTest {

  @Test
  void testAttemptRestartOnState() {
    for (ApplicationStateSummary stateSummary : ApplicationStateSummary.values()) {
      assertTrue(attemptRestartOnState(RestartPolicy.Always, stateSummary));
      assertFalse(attemptRestartOnState(RestartPolicy.Never, stateSummary));
      if (!stateSummary.isStopping()) {
        assertFalse(attemptRestartOnState(OnFailure, stateSummary));
        assertFalse(attemptRestartOnState(OnInfrastructureFailure, stateSummary));
      }
    }
    assertFalse(attemptRestartOnState(OnFailure, Succeeded));
    assertFalse(attemptRestartOnState(OnInfrastructureFailure, Succeeded));
    assertTrue(attemptRestartOnState(OnFailure, Failed));
    assertFalse(attemptRestartOnState(OnInfrastructureFailure, Failed));
    assertTrue(attemptRestartOnState(OnFailure, DriverStartTimedOut));
    assertTrue(attemptRestartOnState(OnInfrastructureFailure, DriverStartTimedOut));
    assertTrue(attemptRestartOnState(OnFailure, DriverReadyTimedOut));
    assertFalse(attemptRestartOnState(OnInfrastructureFailure, DriverReadyTimedOut));
    assertTrue(attemptRestartOnState(OnFailure, DriverEvicted));
    assertFalse(attemptRestartOnState(OnInfrastructureFailure, DriverEvicted));
    assertTrue(attemptRestartOnState(OnFailure, ExecutorsStartTimedOut));
    assertTrue(attemptRestartOnState(OnInfrastructureFailure, ExecutorsStartTimedOut));
  }
}
