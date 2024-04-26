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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.status.ApplicationStateSummary;

class RestartPolicyTest {

  @Test
  void testAttemptRestartOnState() {
    for (ApplicationStateSummary stateSummary : ApplicationStateSummary.values()) {
      Assertions.assertTrue(attemptRestartOnState(RestartPolicy.Always, stateSummary));
      Assertions.assertFalse(attemptRestartOnState(RestartPolicy.Never, stateSummary));
      if (!stateSummary.isStopping()) {
        Assertions.assertFalse(attemptRestartOnState(OnFailure, stateSummary));
        Assertions.assertFalse(attemptRestartOnState(OnInfrastructureFailure, stateSummary));
      }
    }
    Assertions.assertFalse(attemptRestartOnState(OnFailure, Succeeded));
    Assertions.assertFalse(attemptRestartOnState(OnInfrastructureFailure, Succeeded));
    Assertions.assertTrue(attemptRestartOnState(OnFailure, Failed));
    Assertions.assertFalse(attemptRestartOnState(OnInfrastructureFailure, Failed));
    Assertions.assertTrue(attemptRestartOnState(OnFailure, DriverStartTimedOut));
    Assertions.assertTrue(attemptRestartOnState(OnInfrastructureFailure, DriverStartTimedOut));
    Assertions.assertTrue(attemptRestartOnState(OnFailure, DriverReadyTimedOut));
    Assertions.assertFalse(attemptRestartOnState(OnInfrastructureFailure, DriverReadyTimedOut));
    Assertions.assertTrue(attemptRestartOnState(OnFailure, DriverEvicted));
    Assertions.assertFalse(attemptRestartOnState(OnInfrastructureFailure, DriverEvicted));
    Assertions.assertTrue(attemptRestartOnState(OnFailure, ExecutorsStartTimedOut));
    Assertions.assertTrue(attemptRestartOnState(OnInfrastructureFailure, ExecutorsStartTimedOut));
  }
}
