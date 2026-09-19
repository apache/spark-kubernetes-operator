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

import static org.apache.spark.k8s.operator.utils.EventUtils.MAX_MESSAGE_LENGTH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Set;

import io.javaoperatorsdk.operator.api.event.EventRecord;
import io.javaoperatorsdk.operator.api.event.EventType;
import io.javaoperatorsdk.operator.api.event.ResourceEventRecorder;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ClusterStateSummary;

class EventUtilsTest {

  private final ResourceEventRecorder recorder = mock(ResourceEventRecorder.class);

  private EventRecord recordWarning(String reason, String message) {
    EventUtils.warn(recorder, reason, message);
    ArgumentCaptor<EventRecord> captor = ArgumentCaptor.forClass(EventRecord.class);
    verify(recorder).record(captor.capture());
    return captor.getValue();
  }

  @Test
  void warnPublishesWarningWithReasonAndMessage() {
    EventRecord event = recordWarning(EventUtils.REASON_RECONCILE_ERROR, "driver pod rejected");

    assertThat(event.type()).isEqualTo(EventType.WARNING);
    assertThat(event.reason()).isEqualTo(EventUtils.REASON_RECONCILE_ERROR);
    assertThat(event.message()).isEqualTo("driver pod rejected");
  }

  @Test
  void warnKeysTheEventOnReasonSoRepeatsAggregate() {
    // The key, not the message, is what DefaultEventRecorder digests into the Event name. Keying
    // on the reason is what makes a repeated failure bump the count on one Event instead of
    // creating a new one every time the message varies.
    EventRecord event = recordWarning(EventUtils.REASON_STATUS_UPDATE_FAILED, "attempt 3 of 15");

    assertThat(event.key()).contains(EventUtils.REASON_STATUS_UPDATE_FAILED);
  }

  @Test
  void normalPublishesNormalWithReasonAndMessage() {
    EventUtils.normal(recorder, EventUtils.REASON_KUEUE_ADMITTED, "Kueue admitted Workload app-1");

    ArgumentCaptor<EventRecord> captor = ArgumentCaptor.forClass(EventRecord.class);
    verify(recorder).record(captor.capture());
    EventRecord event = captor.getValue();
    assertThat(event.type()).isEqualTo(EventType.NORMAL);
    assertThat(event.reason()).isEqualTo(EventUtils.REASON_KUEUE_ADMITTED);
    assertThat(event.message()).isEqualTo("Kueue admitted Workload app-1");
    assertThat(event.key()).contains(EventUtils.REASON_KUEUE_ADMITTED);
  }

  @Test
  void recordPublishesTheGivenType() {
    EventUtils.record(recorder, EventType.NORMAL, "DriverRequested", "driver requested");

    ArgumentCaptor<EventRecord> captor = ArgumentCaptor.forClass(EventRecord.class);
    verify(recorder).record(captor.capture());
    EventRecord event = captor.getValue();
    assertThat(event.type()).isEqualTo(EventType.NORMAL);
    assertThat(event.reason()).isEqualTo("DriverRequested");
    assertThat(event.message()).isEqualTo("driver requested");
    assertThat(event.key()).contains("DriverRequested");
  }

  @Test
  void eventTypeOfApplicationStates() {
    Set<ApplicationStateSummary> expectedWarnings =
        Set.of(
            ApplicationStateSummary.SchedulingFailure,
            ApplicationStateSummary.Failed,
            ApplicationStateSummary.DriverEvicted,
            ApplicationStateSummary.DriverStartTimedOut,
            ApplicationStateSummary.DriverReadyTimedOut,
            ApplicationStateSummary.ExecutorsStartTimedOut,
            ApplicationStateSummary.RunningWithBelowThresholdExecutors,
            ApplicationStateSummary.TerminatedWithoutReleaseResources);
    for (ApplicationStateSummary summary : ApplicationStateSummary.values()) {
      EventType expected =
          expectedWarnings.contains(summary) ? EventType.WARNING : EventType.NORMAL;
      assertThat(EventUtils.eventTypeOf(summary)).as(summary.name()).isEqualTo(expected);
    }
  }

  @Test
  void eventTypeOfClusterStates() {
    Set<ClusterStateSummary> expectedWarnings =
        Set.of(ClusterStateSummary.SchedulingFailure, ClusterStateSummary.Failed);
    for (ClusterStateSummary summary : ClusterStateSummary.values()) {
      EventType expected =
          expectedWarnings.contains(summary) ? EventType.WARNING : EventType.NORMAL;
      assertThat(EventUtils.eventTypeOf(summary)).as(summary.name()).isEqualTo(expected);
    }
  }

  @Test
  void warnTruncatesAnOverLongMessage() {
    EventRecord event = recordWarning(EventUtils.REASON_CLEANUP_ERROR, "x".repeat(5000));

    assertThat(event.message()).hasSize(MAX_MESSAGE_LENGTH).endsWith("...");
  }

  @Test
  void truncateKeepsMessagesWithinTheLimitUntouched() {
    String atLimit = "y".repeat(MAX_MESSAGE_LENGTH);

    assertThat(EventUtils.truncate("short")).isEqualTo("short");
    assertThat(EventUtils.truncate(atLimit)).isEqualTo(atLimit);
  }

  @Test
  void truncateCapsTotalLengthIncludingTheEllipsis() {
    // The ellipsis replaces the dropped characters rather than being appended past the limit, so
    // the result never exceeds what the caller was promised.
    String truncated = EventUtils.truncate("z".repeat(MAX_MESSAGE_LENGTH + 1));

    assertThat(truncated).hasSize(MAX_MESSAGE_LENGTH);
    assertThat(truncated).isEqualTo("z".repeat(MAX_MESSAGE_LENGTH - 3) + "...");
  }

  @Test
  void describeAppendsInnermostCause() {
    Throwable root = new IllegalArgumentException("quota exceeded");
    Throwable middle = new IllegalStateException("submission rejected", root);
    Throwable top = new RuntimeException("reconcile failed", middle);

    // The middle link is dropped, the innermost cause carries the actionable detail.
    assertThat(EventUtils.describe(top))
        .isEqualTo(
            "RuntimeException: reconcile failed, caused by: "
                + "IllegalArgumentException: quota exceeded");
  }

  @Test
  void describeHandlesNullAndCauseless() {
    assertThat(EventUtils.describe(null)).isEmpty();
    assertThat(EventUtils.describe(new IllegalStateException("no cause")))
        .isEqualTo("IllegalStateException: no cause");
  }

  @Test
  void describeFallsBackToTypeWhenMessageIsBlank() {
    assertThat(EventUtils.describe(new IllegalStateException())).isEqualTo("IllegalStateException");
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
