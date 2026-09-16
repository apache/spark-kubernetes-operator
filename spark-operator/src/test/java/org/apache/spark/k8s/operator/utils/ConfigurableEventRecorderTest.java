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
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.Map;

import io.javaoperatorsdk.operator.api.event.EventRecord;
import io.javaoperatorsdk.operator.api.event.EventRecorder;
import io.javaoperatorsdk.operator.api.event.EventType;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.apache.spark.k8s.operator.config.SparkOperatorConfManager;

class ConfigurableEventRecorderTest {

  private final EventRecorder delegate = mock(EventRecorder.class);
  private final Context<?> context = mock(Context.class);
  private final ConfigurableEventRecorder recorder = new ConfigurableEventRecorder(delegate);

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
}
