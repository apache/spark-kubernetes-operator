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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.status.ApplicationStateSummary;

class RestartPolicyTest {

  @Test
  void testAttemptRestartOnState() {
    for (ApplicationStateSummary stateSummary : ApplicationStateSummary.values()) {
      Assertions.assertTrue(
          RestartPolicy.attemptRestartOnState(RestartPolicy.Always, stateSummary));
      Assertions.assertFalse(
          RestartPolicy.attemptRestartOnState(RestartPolicy.Never, stateSummary));
      if (!stateSummary.isStopping()) {
        Assertions.assertFalse(
            RestartPolicy.attemptRestartOnState(RestartPolicy.OnFailure, stateSummary));
        Assertions.assertFalse(
            RestartPolicy.attemptRestartOnState(
                RestartPolicy.OnInfrastructureFailure, stateSummary));
      }
    }
    Assertions.assertFalse(
        RestartPolicy.attemptRestartOnState(
            RestartPolicy.OnFailure, ApplicationStateSummary.SUCCEEDED));
    Assertions.assertFalse(
        RestartPolicy.attemptRestartOnState(
            RestartPolicy.OnInfrastructureFailure, ApplicationStateSummary.SUCCEEDED));
    Assertions.assertTrue(
        RestartPolicy.attemptRestartOnState(
            RestartPolicy.OnFailure, ApplicationStateSummary.FAILED));
    Assertions.assertFalse(
        RestartPolicy.attemptRestartOnState(
            RestartPolicy.OnInfrastructureFailure, ApplicationStateSummary.FAILED));
    Assertions.assertTrue(
        RestartPolicy.attemptRestartOnState(
            RestartPolicy.OnFailure, ApplicationStateSummary.DRIVER_START_TIMED_OUT));
    Assertions.assertTrue(
        RestartPolicy.attemptRestartOnState(
            RestartPolicy.OnInfrastructureFailure, ApplicationStateSummary.DRIVER_START_TIMED_OUT));
    Assertions.assertTrue(
        RestartPolicy.attemptRestartOnState(
            RestartPolicy.OnFailure, ApplicationStateSummary.DRIVER_READY_TIMED_OUT));
    Assertions.assertFalse(
        RestartPolicy.attemptRestartOnState(
            RestartPolicy.OnInfrastructureFailure, ApplicationStateSummary.DRIVER_READY_TIMED_OUT));
    Assertions.assertTrue(
        RestartPolicy.attemptRestartOnState(
            RestartPolicy.OnFailure, ApplicationStateSummary.DRIVER_EVICTED));
    Assertions.assertFalse(
        RestartPolicy.attemptRestartOnState(
            RestartPolicy.OnInfrastructureFailure, ApplicationStateSummary.DRIVER_EVICTED));
    Assertions.assertTrue(
        RestartPolicy.attemptRestartOnState(
            RestartPolicy.OnFailure, ApplicationStateSummary.EXECUTORS_LAUNCH_TIMED_OUT));
    Assertions.assertTrue(
        RestartPolicy.attemptRestartOnState(
            RestartPolicy.OnInfrastructureFailure,
            ApplicationStateSummary.EXECUTORS_LAUNCH_TIMED_OUT));
  }
}
