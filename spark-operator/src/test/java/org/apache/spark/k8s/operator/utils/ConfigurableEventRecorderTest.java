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
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

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
}
