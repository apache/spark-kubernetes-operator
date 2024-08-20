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

import static org.apache.spark.k8s.operator.status.ClusterStateSummary.Submitted;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ClusterStatusTest {
  @Test
  void testInitStatus() {
    ClusterStatus status = new ClusterStatus();
    assertEquals(Submitted, status.currentState.currentStateSummary);
    assertEquals(1, status.getStateTransitionHistory().size());
    assertEquals(status.currentState, status.getStateTransitionHistory().get(0L));
  }

  @Test
  void testAppendNewState() {
    ClusterStatus status = new ClusterStatus();
    ClusterState newState = new ClusterState(ClusterStateSummary.RunningHealthy, "foo");
    ClusterStatus newStatus = status.appendNewState(newState);
    assertEquals(2, newStatus.getStateTransitionHistory().size());
    assertEquals(newState, newStatus.getStateTransitionHistory().get(1L));
  }
}
