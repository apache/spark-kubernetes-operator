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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.status.ApplicationStateSummary;

class RestartConfigTest {

  @Test
  void effectiveRestartBackoffMillisUsesDefaultWhenFailureOverrideIsNull() {
    RestartConfig config =
        RestartConfig.builder()
            .restartBackoffMillis(30000L)
            .restartBackoffMillisForFailure(null) // No override
            .build();

    assertEquals(
        30000L, config.getEffectiveRestartBackoffMillis(ApplicationStateSummary.Failed));
    assertEquals(
        30000L, config.getEffectiveRestartBackoffMillis(ApplicationStateSummary.Succeeded));
  }

  @Test
  void effectiveRestartBackoffMillisUsesFailureOverrideWhenSet() {
    RestartConfig config =
        RestartConfig.builder()
            .restartBackoffMillis(10000L) // 10 seconds default
            .restartBackoffMillisForFailure(60000L) // 1 minute for failures
            .build();

    assertEquals(
        60000L, config.getEffectiveRestartBackoffMillis(ApplicationStateSummary.Failed));
    assertEquals(
        10000L, config.getEffectiveRestartBackoffMillis(ApplicationStateSummary.Succeeded));
  }

  @Test
  void effectiveRestartBackoffMillisHandlesZeroFailureOverride() {
    RestartConfig config =
        RestartConfig.builder()
            .restartBackoffMillis(30000L)
            .restartBackoffMillisForFailure(0L) // No delay for failures
            .build();

    assertEquals(
        0L, config.getEffectiveRestartBackoffMillis(ApplicationStateSummary.Failed));
    assertEquals(
        30000L, config.getEffectiveRestartBackoffMillis(ApplicationStateSummary.Succeeded));
  }
}
