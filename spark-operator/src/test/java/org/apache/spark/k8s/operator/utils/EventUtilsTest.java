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

package org.apache.spark.k8s.operator.utils;

import static org.apache.spark.k8s.operator.config.SparkOperatorConf.KUBERNETES_EVENTS_ENABLED;
import static org.apache.spark.k8s.operator.utils.TestUtils.setConfigKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import io.javaoperatorsdk.operator.api.event.ResourceEventRecorder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class EventUtilsTest {

  private final ResourceEventRecorder recorder = mock(ResourceEventRecorder.class);

  private final AtomicInteger supplierCalls = new AtomicInteger();

  private final Supplier<ResourceEventRecorder> recorderSupplier =
      () -> {
        supplierCalls.incrementAndGet();
        return recorder;
      };

  @AfterEach
  void restoreEventsDisabled() {
    // The option defaults to false. setConfigKey mutates the shared ConfigOption, so every test
    // that enables events has to put it back or it leaks into the rest of the JVM.
    setConfigKey(KUBERNETES_EVENTS_ENABLED, false);
  }

  @Test
  void warnDoesNothingWhenDisabled() {
    // Left at the default of false, so nothing should be published.
    EventUtils.warn(recorderSupplier, EventUtils.REASON_RECONCILE_ERROR, "driver pod rejected");

    verifyNoInteractions(recorder);
    // The recorder is resolved lazily, so a disabled operator never even asks for one.
    assertThat(supplierCalls).hasValue(0);
  }

  @Test
  void warnSwallowsFailureFromRecorder() {
    setConfigKey(KUBERNETES_EVENTS_ENABLED, true);
    doThrow(new IllegalStateException("event write rejected"))
        .when(recorder)
        .warn("ReconcileError", "boom");

    // Publishing is best effort, a failed event must never surface to the reconciler.
    assertThatCode(
            () -> EventUtils.warn(recorderSupplier, EventUtils.REASON_RECONCILE_ERROR, "boom"))
        .doesNotThrowAnyException();
  }

  @Test
  void describeAppendsInnermostCause() {
    Throwable root = new IllegalArgumentException("quota exceeded");
    Throwable middle = new IllegalStateException("submission rejected", root);
    Throwable top = new RuntimeException("reconcile failed", middle);

    // The middle link is dropped, the innermost cause carries the actionable detail.
    assertThat(EventUtils.describe(top))
        .isEqualTo("RuntimeException: reconcile failed, caused by: "
            + "IllegalArgumentException: quota exceeded");
  }

  @Test
  void describeStopsAtMaxCauseDepth() {
    // 15 links, deeper than the 10 the walk is allowed to follow.
    Throwable cause = new IllegalStateException("cause-15");
    for (int i = 14; i >= 1; i--) {
      cause = new IllegalStateException("cause-" + i, cause);
    }
    Throwable top = new RuntimeException("top", cause);

    // The walk reports the deepest link it reached rather than giving up entirely.
    assertThat(EventUtils.describe(top))
        .isEqualTo("RuntimeException: top, caused by: IllegalStateException: cause-10");
  }

  @Test
  void describeTerminatesOnCyclicCauseChain() {
    Throwable first = new RuntimeException("first");
    Throwable second = new IllegalStateException("second", first);
    // first -> second -> first, a chain a naive walk would follow forever.
    first.initCause(second);

    assertThat(EventUtils.describe(first))
        .isEqualTo("RuntimeException: first, caused by: IllegalStateException: second");
  }
}
