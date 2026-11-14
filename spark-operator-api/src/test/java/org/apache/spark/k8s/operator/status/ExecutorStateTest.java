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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.EnumSet;

import org.junit.jupiter.api.Test;

class ExecutorStateTest {

  @Test
  void testValuesAndTheirOrder() {
    assertArrayEquals(
        new ExecutorState[] {
          ExecutorState.PENDING,
          ExecutorState.RUNNING,
          ExecutorState.COMPLETED,
          ExecutorState.FAILED,
          ExecutorState.UNKNOWN
        },
        ExecutorState.values());
  }

  @Test
  void testOnlyCompletedAndFailedAreTerminated() {
    EnumSet<ExecutorState> terminated = EnumSet.noneOf(ExecutorState.class);
    for (ExecutorState state : ExecutorState.values()) {
      if (state.isTerminated()) {
        terminated.add(state);
      }
    }
    assertEquals(EnumSet.of(ExecutorState.COMPLETED, ExecutorState.FAILED), terminated);
  }
}
