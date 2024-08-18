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

import static org.apache.spark.k8s.operator.spec.RestartPolicy.Always;
import static org.apache.spark.k8s.operator.spec.RestartPolicy.Never;
import static org.apache.spark.k8s.operator.spec.RestartPolicy.OnFailure;
import static org.apache.spark.k8s.operator.spec.RestartPolicy.OnInfrastructureFailure;
import static org.apache.spark.k8s.operator.spec.RestartPolicy.attemptRestartOnState;
import static org.apache.spark.k8s.operator.status.ApplicationStateSummary.DriverEvicted;
import static org.apache.spark.k8s.operator.status.ApplicationStateSummary.DriverReadyTimedOut;
import static org.apache.spark.k8s.operator.status.ApplicationStateSummary.DriverStartTimedOut;
import static org.apache.spark.k8s.operator.status.ApplicationStateSummary.ExecutorsStartTimedOut;
import static org.apache.spark.k8s.operator.status.ApplicationStateSummary.Failed;
import static org.apache.spark.k8s.operator.status.ApplicationStateSummary.SchedulingFailure;
import static org.apache.spark.k8s.operator.status.ApplicationStateSummary.Succeeded;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.status.ApplicationStateSummary;

class RestartPolicyTest {

  @Test
  void testAlways() {
    for (ApplicationStateSummary stateSummary : ApplicationStateSummary.values()) {
      assertTrue(attemptRestartOnState(Always, stateSummary));
    }
  }

  @Test
  void testNever() {
    for (ApplicationStateSummary stateSummary : ApplicationStateSummary.values()) {
      assertFalse(attemptRestartOnState(Never, stateSummary));
    }
  }

  @Test
  void testOnFailure() {
    for (ApplicationStateSummary stateSummary : ApplicationStateSummary.values()) {
      if (!stateSummary.isStopping()) {
        assertFalse(attemptRestartOnState(OnFailure, stateSummary));
      }
    }
    assertFalse(attemptRestartOnState(OnFailure, Succeeded));
    assertTrue(attemptRestartOnState(OnFailure, Failed));
    assertTrue(attemptRestartOnState(OnFailure, DriverStartTimedOut));
    assertTrue(attemptRestartOnState(OnFailure, DriverReadyTimedOut));
    assertTrue(attemptRestartOnState(OnFailure, DriverEvicted));
    assertTrue(attemptRestartOnState(OnFailure, ExecutorsStartTimedOut));
  }

  @Test
  void testOnInfrastructureFailure() {
    assertTrue(attemptRestartOnState(OnInfrastructureFailure, DriverStartTimedOut));
    assertTrue(attemptRestartOnState(OnInfrastructureFailure, ExecutorsStartTimedOut));
    assertTrue(attemptRestartOnState(OnInfrastructureFailure, SchedulingFailure));

    for (ApplicationStateSummary stateSummary : ApplicationStateSummary.values()) {
      if (!stateSummary.isStopping()) {
        assertFalse(attemptRestartOnState(OnInfrastructureFailure, stateSummary));
      }
    }
    assertFalse(attemptRestartOnState(OnInfrastructureFailure, Succeeded));
    assertFalse(attemptRestartOnState(OnInfrastructureFailure, Failed));
    assertFalse(attemptRestartOnState(OnInfrastructureFailure, DriverReadyTimedOut));
    assertFalse(attemptRestartOnState(OnInfrastructureFailure, DriverEvicted));
  }
}
