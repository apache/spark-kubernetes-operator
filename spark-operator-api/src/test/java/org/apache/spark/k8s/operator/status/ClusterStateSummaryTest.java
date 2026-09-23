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

class ClusterStateSummaryTest {

  @Test
  void testSuspendedIsNeitherStartingNorTerminated() {
    // A suspended cluster is reconciled again to be resumed, so it must not be terminated, and it
    // must not be mistaken for a starting cluster by the ordinal comparison of isStarting
    assertFalse(ClusterStateSummary.Suspended.isInitializing());
    assertFalse(ClusterStateSummary.Suspended.isStarting());
    assertFalse(ClusterStateSummary.Suspended.isTerminated());
    assertFalse(ClusterStateSummary.Suspended.isFailure());
    assertFalse(ClusterStateSummary.Suspended.isInfrastructureFailure());
  }

  @Test
  void testIsStarting() {
    assertTrue(ClusterStateSummary.Submitted.isStarting());
    assertTrue(ClusterStateSummary.SchedulingFailure.isStarting());

    assertFalse(ClusterStateSummary.RunningHealthy.isStarting());
    assertFalse(ClusterStateSummary.Failed.isStarting());
    assertFalse(ClusterStateSummary.ResourceReleased.isStarting());
  }
}
