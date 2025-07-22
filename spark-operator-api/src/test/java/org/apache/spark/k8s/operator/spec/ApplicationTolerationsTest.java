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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ApplicationTolerationsTest {
  private final ApplicationTolerations withRetainDurationOnly =
      ApplicationTolerations.builder().resourceRetainDurationMillis(10L).build();
  private final ApplicationTolerations withTTLOnly =
      ApplicationTolerations.builder().ttlAfterStopMillis(10L).build();
  private final ApplicationTolerations withNeitherRetainDurationNorTtl =
      ApplicationTolerations.builder().build();
  private final ApplicationTolerations withRetainDurationGreaterThanTtl =
      ApplicationTolerations.builder()
          .resourceRetainDurationMillis(20L)
          .ttlAfterStopMillis(10L)
          .build();
  private final ApplicationTolerations withRetainDurationShorterThanTtl =
      ApplicationTolerations.builder()
          .resourceRetainDurationMillis(10L)
          .ttlAfterStopMillis(20L)
          .build();

  @Test
  void computeEffectiveRetainDurationMillis() {
    assertEquals(10L, withRetainDurationOnly.computeEffectiveRetainDurationMillis());
    assertEquals(10L, withTTLOnly.computeEffectiveRetainDurationMillis());
    assertEquals(-1, withNeitherRetainDurationNorTtl.computeEffectiveRetainDurationMillis());
    assertEquals(10L, withRetainDurationGreaterThanTtl.computeEffectiveRetainDurationMillis());
    assertEquals(10L, withRetainDurationShorterThanTtl.computeEffectiveRetainDurationMillis());
  }

  @Test
  void isRetainDurationEnabled() {
    assertTrue(withRetainDurationOnly.isRetainDurationEnabled());
    assertTrue(withTTLOnly.isRetainDurationEnabled());
    assertFalse(withNeitherRetainDurationNorTtl.isRetainDurationEnabled());
    assertTrue(withRetainDurationGreaterThanTtl.isRetainDurationEnabled());
    assertTrue(withRetainDurationShorterThanTtl.isRetainDurationEnabled());
  }

  @Test
  void isTTLEnabled() {
    assertFalse(withRetainDurationOnly.isTTLEnabled());
    assertTrue(withTTLOnly.isTTLEnabled());
    assertFalse(withNeitherRetainDurationNorTtl.isTTLEnabled());
    assertTrue(withRetainDurationGreaterThanTtl.isTTLEnabled());
    assertTrue(withRetainDurationShorterThanTtl.isTTLEnabled());
  }
}
