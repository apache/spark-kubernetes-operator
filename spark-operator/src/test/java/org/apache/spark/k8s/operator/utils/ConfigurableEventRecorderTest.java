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
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.KUBERNETES_EVENTS_EXCLUDED_REASONS;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.KUBERNETES_EVENTS_MIN_INTERVAL_SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.javaoperatorsdk.operator.api.event.EventRecord;
import io.javaoperatorsdk.operator.api.event.EventRecorder;
import io.javaoperatorsdk.operator.api.event.EventType;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.config.SparkOperatorConfManager;

class ConfigurableEventRecorderTest {

  private final EventRecorder delegate = mock(EventRecorder.class);
  private final Context<?> context = mock(Context.class);
  private final ConfigurableEventRecorder recorder = new ConfigurableEventRecorder(delegate);
  private final AtomicLong nanoTime = new AtomicLong();
  private final ConfigurableEventRecorder timedRecorder =
      new ConfigurableEventRecorder(delegate, nanoTime::get);

  private void setEventsEnabled(boolean enabled) {
    SparkOperatorConfManager.INSTANCE.refresh(
        Map.of(KUBERNETES_EVENTS_ENABLED.getKey(), String.valueOf(enabled)));
  }

  private void setExcludedReasons(String reasons) {
    SparkOperatorConfManager.INSTANCE.refresh(
        Map.of(
            KUBERNETES_EVENTS_ENABLED.getKey(),
            "true",
            KUBERNETES_EVENTS_EXCLUDED_REASONS.getKey(),
            reasons));
  }

  private void setMinIntervalSeconds(long seconds) {
    SparkOperatorConfManager.INSTANCE.refresh(
        Map.of(
            KUBERNETES_EVENTS_ENABLED.getKey(),
            "true",
            KUBERNETES_EVENTS_MIN_INTERVAL_SECONDS.getKey(),
            String.valueOf(seconds)));
  }

  private void elapseSeconds(long seconds) {
    nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(seconds));
  }

  /** Returns a context whose primary resource carries the given uid. */
  private static Context<?> contextOf(String uid) {
    SparkApplication app = new SparkApplication();
    app.setMetadata(new ObjectMetaBuilder().withUid(uid).build());
    Context<?> context = mock(Context.class);
    doReturn(app).when(context).getPrimaryResource();
    return context;
  }

  @AfterEach
  void resetConf() {
    SparkOperatorConfManager.INSTANCE.refresh(Map.of());
  }

  @Test
  void dropsEventsWhileDisabled() {
    // The option defaults to false, so an operator that never opts in writes nothing.
    recorder.record(EventRecord.warning("ReconcileError", "boom"), context);

    verifyNoInteractions(delegate);
  }

  @Test
  void forwardsEventsWhileEnabled() {
    setEventsEnabled(true);
    EventRecord event = EventRecord.warning("ReconcileError", "boom");

    recorder.record(event, context);

    verify(delegate).record(event, context);
  }

  @Test
  void boundRecorderIsGatedOnTheFlagRatherThanOnBindingTime() {
    // Bound while disabled, then enabled mid reconciliation: the dynamic override has to take
    // effect, which it only does if the flag is read per event and not when forContext is called.
    var bound = recorder.forContext(context);
    bound.warn("ReconcileError", "dropped");
    verifyNoInteractions(delegate);

    setEventsEnabled(true);
    bound.warn("ReconcileError", "published");

    ArgumentCaptor<EventRecord> captor = ArgumentCaptor.forClass(EventRecord.class);
    verify(delegate).record(captor.capture(), eq(context));
    assertThat(captor.getValue().message()).isEqualTo("published");
  }

  @Test
  void boundRecorderPassesThroughTypeAndReason() {
    setEventsEnabled(true);
    var bound = recorder.forContext(context);

    bound.normal("DriverRequested", "requested");

    ArgumentCaptor<EventRecord> captor = ArgumentCaptor.forClass(EventRecord.class);
    verify(delegate).record(captor.capture(), eq(context));
    assertThat(captor.getValue().type()).isEqualTo(EventType.NORMAL);
    assertThat(captor.getValue().reason()).isEqualTo("DriverRequested");
  }

  @Test
  void treatsAMalformedOverrideAsDisabled() {
    // The option is a boxed Boolean, so a malformed override can resolve to null. Publishing an
    // event must never be the thing that throws out of a reconciliation.
    SparkOperatorConfManager.INSTANCE.refresh(
        Map.of(KUBERNETES_EVENTS_ENABLED.getKey(), "null"));

    recorder.record(EventRecord.warning("ReconcileError", "boom"), context);

    verifyNoInteractions(delegate);
  }

  @Test
  void dropsEventsWithExcludedReasons() {
    setExcludedReasons("RunningHealthy,RunningWithPartialCapacity");

    recorder.record(EventRecord.normal("RunningHealthy", "healthy"), context);
    recorder.forContext(context).normal("RunningWithPartialCapacity", "partial");

    verifyNoInteractions(delegate);
  }

  @Test
  void forwardsEventsWithNonExcludedReasons() {
    setExcludedReasons("RunningHealthy");
    EventRecord event = EventRecord.warning("Failed", "boom");

    recorder.record(event, context);

    verify(delegate).record(event, context);
  }

  @Test
  void excludedReasonsIgnoreWhitespaceAndEmptyEntries() {
    setExcludedReasons(" RunningHealthy , ,, DriverReady ,");

    recorder.record(EventRecord.normal("RunningHealthy", "dropped"), context);
    recorder.record(EventRecord.normal("DriverReady", "dropped"), context);
    verifyNoInteractions(delegate);

    EventRecord event = EventRecord.normal("DriverStarted", "published");
    recorder.record(event, context);
    verify(delegate).record(event, context);
  }

  @Test
  void excludedReasonsAreCaseSensitive() {
    setExcludedReasons("runninghealthy");
    EventRecord event = EventRecord.normal("RunningHealthy", "published");

    recorder.record(event, context);

    verify(delegate).record(event, context);
  }

  @Test
  void forwardsEventsOfAllReasonsWhenNoReasonIsExcluded() {
    setExcludedReasons("");
    EventRecord normal = EventRecord.normal("RunningHealthy", "healthy");
    EventRecord warning = EventRecord.warning("ReconcileError", "boom");

    recorder.record(normal, context);
    recorder.record(warning, context);

    verify(delegate).record(normal, context);
    verify(delegate).record(warning, context);
  }

  @Test
  void excludedReasonsTakeEffectWithinARunningReconciliation() {
    setEventsEnabled(true);
    var bound = recorder.forContext(context);
    bound.normal("RunningHealthy", "published");

    setExcludedReasons("RunningHealthy");
    bound.normal("RunningHealthy", "dropped");

    ArgumentCaptor<EventRecord> captor = ArgumentCaptor.forClass(EventRecord.class);
    verify(delegate).record(captor.capture(), eq(context));
    assertThat(captor.getValue().message()).isEqualTo("published");
  }

  @Test
  void dropsEventsWithReasonsMatchingARegex() {
    setExcludedReasons("Running.*, Driver(Started|Ready)");

    recorder.record(EventRecord.normal("RunningHealthy", "dropped"), context);
    recorder.record(EventRecord.normal("RunningWithPartialCapacity", "dropped"), context);
    recorder.record(EventRecord.normal("DriverReady", "dropped"), context);
    verifyNoInteractions(delegate);

    EventRecord event = EventRecord.normal("DriverRequested", "published");
    recorder.record(event, context);
    verify(delegate).record(event, context);
  }

  @Test
  void excludedReasonRegexMustMatchTheWholeReason() {
    setExcludedReasons("Running");
    EventRecord event = EventRecord.normal("RunningHealthy", "published");

    recorder.record(event, context);

    verify(delegate).record(event, context);
  }

  @Test
  void invalidExcludedReasonRegexOnlyMatchesLiterally() {
    // A malformed pattern must not throw out of a reconciliation.
    setExcludedReasons("Running[, Failed");
    EventRecord event = EventRecord.normal("RunningHealthy", "published");

    recorder.record(event, context);
    recorder.record(EventRecord.warning("Failed", "dropped"), context);
    recorder.record(EventRecord.normal("Running[", "dropped"), context);

    verify(delegate).record(event, context);
    verifyNoMoreInteractions(delegate);
  }

  @Test
  void asteriskIsAnInvalidRegexRatherThanAWildcard() {
    // Unlike the watched-namespaces option, '*' has no special meaning here. It is an invalid
    // regex, so it only matches literally, and '.*' is the way to exclude every reason.
    setExcludedReasons("*");
    EventRecord event = EventRecord.normal("RunningHealthy", "published");
    recorder.record(event, context);
    verify(delegate).record(event, context);

    setExcludedReasons(".*");
    recorder.record(EventRecord.normal("RunningHealthy", "dropped"), context);
    verifyNoMoreInteractions(delegate);
  }

  @Test
  void publishesTheFirstEventOfAReasonRightAway() {
    // A rare event such as a failure must never be delayed by the limit.
    setMinIntervalSeconds(300L);
    Context<?> context = contextOf("uid-1");
    EventRecord event = EventRecord.warning("Failed", "boom");

    timedRecorder.record(event, context);

    verify(delegate).record(event, context);
  }

  @Test
  void dropsAnEventRepeatedWithinTheMinInterval() {
    setMinIntervalSeconds(300L);
    Context<?> context = contextOf("uid-1");
    EventRecord first = EventRecord.normal("KueueAdmissionPending", "queued");

    timedRecorder.record(first, context);
    elapseSeconds(120L);
    timedRecorder.record(EventRecord.normal("KueueAdmissionPending", "queued"), context);
    elapseSeconds(120L);
    timedRecorder.record(EventRecord.normal("KueueAdmissionPending", "queued"), context);

    verify(delegate).record(first, context);
    verifyNoMoreInteractions(delegate);
  }

  @Test
  void keepsRepublishingAtTheMinIntervalWhileTheEventRepeats() {
    // The window is fixed rather than sliding: a dropped repeat must not push the deadline out,
    // or an event repeated more often than the interval, as KueueAdmissionPending is at the 120
    // second reconcile interval, would be published once and never again and would then expire
    // from the API server. This also pins the effective period at 360 rather than 300 seconds,
    // since a repeat is only published when a reconciliation emits one.
    setMinIntervalSeconds(300L);
    Context<?> context = contextOf("uid-1");
    EventRecord first = EventRecord.normal("KueueAdmissionPending", "queued");
    EventRecord republished = EventRecord.normal("KueueAdmissionPending", "queued");

    timedRecorder.record(first, context);
    elapseSeconds(120L);
    timedRecorder.record(EventRecord.normal("KueueAdmissionPending", "queued"), context);
    elapseSeconds(120L);
    timedRecorder.record(EventRecord.normal("KueueAdmissionPending", "queued"), context);
    elapseSeconds(120L);
    timedRecorder.record(republished, context);

    verify(delegate).record(first, context);
    verify(delegate).record(republished, context);
    verifyNoMoreInteractions(delegate);
  }

  @Test
  void publishesAgainOnceTheMinIntervalElapsed() {
    // The second record of uid-2 is what makes this exercise the interval check rather than the
    // sweep: it sweeps at 300 seconds while the uid-1 entry is still young, so the sweep is no
    // longer due at 320 seconds and the entry is still there when the interval check runs.
    setMinIntervalSeconds(300L);
    Context<?> first = contextOf("uid-1");
    Context<?> second = contextOf("uid-2");
    EventRecord firstEvent = EventRecord.normal("KueueAdmissionPending", "queued");
    EventRecord sweepTrigger = EventRecord.normal("KueueAdmissionPending", "queued");
    EventRecord republished = EventRecord.normal("KueueAdmissionPending", "queued");

    elapseSeconds(10L);
    timedRecorder.record(firstEvent, first);
    elapseSeconds(290L);
    timedRecorder.record(sweepTrigger, second);
    elapseSeconds(20L);
    timedRecorder.record(republished, first);

    verify(delegate).record(firstEvent, first);
    verify(delegate).record(sweepTrigger, second);
    verify(delegate).record(republished, first);
  }

  @Test
  void publishesARepeatWhoseMessageChanged() {
    // The sink rewrites the message of the existing Event on every repeat, so a repeat that says
    // something new is not a repeat as far as the user is concerned. This is what keeps the cause
    // of a failure, and the SuspendHeld message that retracts a stale KueueAdmissionPending,
    // reaching the user rather than being swallowed for a whole interval.
    setMinIntervalSeconds(300L);
    Context<?> context = contextOf("uid-1");
    EventRecord first = EventRecord.warning("ReconcileError", "boom: Forbidden");
    EventRecord changed = EventRecord.warning("ReconcileError", "boom: ImagePullBackOff");

    timedRecorder.record(first, context);
    elapseSeconds(60L);
    timedRecorder.record(changed, context);

    verify(delegate).record(first, context);
    verify(delegate).record(changed, context);
  }

  @Test
  void limitsEachReasonIndependently() {
    setMinIntervalSeconds(300L);
    Context<?> context = contextOf("uid-1");
    EventRecord pending = EventRecord.normal("KueueAdmissionPending", "queued");
    EventRecord failed = EventRecord.warning("Failed", "boom");

    timedRecorder.record(pending, context);
    timedRecorder.record(failed, context);
    timedRecorder.record(EventRecord.warning("Failed", "boom"), context);

    verify(delegate).record(pending, context);
    verify(delegate).record(failed, context);
    verifyNoMoreInteractions(delegate);
  }

  @Test
  void limitsEachResourceIndependently() {
    // The uid rather than the name identifies the resource, so that a reused name starts over.
    setMinIntervalSeconds(300L);
    Context<?> first = contextOf("uid-1");
    Context<?> second = contextOf("uid-2");
    EventRecord firstEvent = EventRecord.normal("SuspendHeld", "held");
    EventRecord secondEvent = EventRecord.normal("SuspendHeld", "held");

    timedRecorder.record(firstEvent, first);
    timedRecorder.record(secondEvent, second);
    timedRecorder.record(EventRecord.normal("SuspendHeld", "held"), first);

    verify(delegate).record(firstEvent, first);
    verify(delegate).record(secondEvent, second);
    verifyNoMoreInteractions(delegate);
  }

  @Test
  void publishesEveryRepeatWhenTheResourceCannotBeIdentified() {
    // Without a uid the event cannot be attributed, so it is published rather than limited under
    // a key shared with every other unidentified resource.
    setMinIntervalSeconds(300L);
    EventRecord first = EventRecord.warning("ReconcileError", "boom");
    EventRecord second = EventRecord.warning("ReconcileError", "boom");

    timedRecorder.record(first, context);
    timedRecorder.record(second, context);

    verify(delegate).record(first, context);
    verify(delegate).record(second, context);
  }

  @Test
  void excludedReasonsDoNotConsumeTheMinInterval() {
    // The exclusion is checked first, so a reason that is excluded and then allowed again is
    // published right away rather than waiting out an interval it never used.
    SparkOperatorConfManager.INSTANCE.refresh(
        Map.of(
            KUBERNETES_EVENTS_ENABLED.getKey(),
            "true",
            KUBERNETES_EVENTS_MIN_INTERVAL_SECONDS.getKey(),
            "300",
            KUBERNETES_EVENTS_EXCLUDED_REASONS.getKey(),
            "RunningHealthy"));
    Context<?> context = contextOf("uid-1");
    timedRecorder.record(EventRecord.normal("RunningHealthy", "healthy"), context);
    verifyNoInteractions(delegate);

    setMinIntervalSeconds(300L);
    EventRecord event = EventRecord.normal("RunningHealthy", "healthy");
    timedRecorder.record(event, context);

    verify(delegate).record(event, context);
  }

  @Test
  void publishesEveryRepeatWhenTheMinIntervalIsNotPositive() {
    Context<?> context = contextOf("uid-1");
    EventRecord first = EventRecord.normal("KueueAdmissionPending", "queued");
    EventRecord second = EventRecord.normal("KueueAdmissionPending", "queued");
    EventRecord third = EventRecord.normal("KueueAdmissionPending", "queued");

    setMinIntervalSeconds(0L);
    timedRecorder.record(first, context);
    timedRecorder.record(second, context);
    setMinIntervalSeconds(-1L);
    timedRecorder.record(third, context);

    verify(delegate).record(first, context);
    verify(delegate).record(second, context);
    verify(delegate).record(third, context);
  }

  @Test
  void keepsSweepingAfterAHugeMinIntervalIsLowered() {
    // The sweep is due on the time elapsed since the last sweep, not on a deadline computed from
    // the interval in force when it last ran. A deadline would make a lowered interval wait out
    // the old one, and TimeUnit#toNanos saturates, so an interval this large would overflow such
    // a deadline and leave the map unswept for the life of the process.
    setMinIntervalSeconds(9223372037L);
    timedRecorder.record(
        EventRecord.normal("KueueAdmissionPending", "queued"), contextOf("uid-1"));
    assertThat(timedRecorder.trackedEventCount()).isEqualTo(1);

    setMinIntervalSeconds(60L);
    elapseSeconds(120L);
    timedRecorder.record(
        EventRecord.normal("KueueAdmissionPending", "queued"), contextOf("uid-2"));

    assertThat(timedRecorder.trackedEventCount()).isEqualTo(1);
  }

  @Test
  void discardsTheIntervalStateWhileEventsAreDisabled() {
    // Nothing sweeps the map while no event is published, so it must not be left resident.
    setMinIntervalSeconds(300L);
    timedRecorder.record(
        EventRecord.normal("KueueAdmissionPending", "queued"), contextOf("uid-1"));
    assertThat(timedRecorder.trackedEventCount()).isEqualTo(1);

    setEventsEnabled(false);
    timedRecorder.record(
        EventRecord.normal("KueueAdmissionPending", "queued"), contextOf("uid-1"));

    assertThat(timedRecorder.trackedEventCount()).isZero();
  }

  @Test
  void discardsTheIntervalStateWhileTheMinIntervalIsNotPositive() {
    setMinIntervalSeconds(300L);
    timedRecorder.record(
        EventRecord.normal("KueueAdmissionPending", "queued"), contextOf("uid-1"));
    assertThat(timedRecorder.trackedEventCount()).isEqualTo(1);

    setMinIntervalSeconds(0L);
    timedRecorder.record(
        EventRecord.normal("KueueAdmissionPending", "queued"), contextOf("uid-1"));

    assertThat(timedRecorder.trackedEventCount()).isZero();
  }
}
