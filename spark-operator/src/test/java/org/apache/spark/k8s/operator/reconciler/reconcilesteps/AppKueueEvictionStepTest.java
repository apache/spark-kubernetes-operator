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

package org.apache.spark.k8s.operator.reconciler.reconcilesteps;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.Condition;
import io.fabric8.kubernetes.api.model.ConditionBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.javaoperatorsdk.operator.api.event.EventRecord;
import io.javaoperatorsdk.operator.api.event.EventType;
import io.javaoperatorsdk.operator.api.event.ResourceEventRecorder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.kueue.v1beta2.Workload;
import org.apache.spark.k8s.operator.kueue.v1beta2.WorkloadSpec;
import org.apache.spark.k8s.operator.kueue.v1beta2.WorkloadStatus;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.utils.EventUtils;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;

class AppKueueEvictionStepTest {
  private final SparkAppContext context = mock(SparkAppContext.class);
  private final SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
  private final ResourceEventRecorder eventRecorder = mock(ResourceEventRecorder.class);

  @BeforeEach
  void setUp() {
    when(context.getEventRecorder()).thenReturn(eventRecorder);
  }

  @Test
  void workloadWhichIsMissingOrAdmittedIsNotReported() {
    AppKueueEvictionStep step = new AppKueueEvictionStep();
    when(context.getCachedKueueWorkload()).thenReturn(Optional.empty());
    Assertions.assertEquals(ReconcileProgress.proceed(), step.reconcile(context, recorder));

    when(context.getCachedKueueWorkload()).thenReturn(Optional.of(workload(true, admitted())));
    Assertions.assertEquals(ReconcileProgress.proceed(), step.reconcile(context, recorder));

    verifyNoInteractions(eventRecorder, recorder);
  }

  @ParameterizedTest
  @ValueSource(strings = {"Preempted", "PodsReadyTimeout", "ClusterQueueStopped"})
  void evictionIsReportedAndIgnored(String reason) {
    when(context.getCachedKueueWorkload())
        .thenReturn(Optional.of(workload(true, admitted(), evicted(reason))));

    // The driver is still observed, e.g. until it completes
    Assertions.assertEquals(
        ReconcileProgress.proceed(), new AppKueueEvictionStep().reconcile(context, recorder));

    String message = captureMessages(1).get(0);
    Assertions.assertTrue(
        message.contains("(" + reason + ": " + reason + " by the test)"), message);
    // Kueue keeps counting the quota of an evicted Workload until the driver and executors are
    // released
    Assertions.assertTrue(message.contains("holding its quota"), message);
    Assertions.assertTrue(message.contains("Set spec.suspend to true"), message);
    verifyNoInteractions(recorder);
  }

  @Test
  void deactivationIsReportedAndIgnored() {
    when(context.getCachedKueueWorkload())
        // Before Kueue marks the deactivated Workload as evicted
        .thenReturn(Optional.of(workload(false, admitted())))
        .thenReturn(Optional.of(workload(false, admitted(), evicted("Deactivated"))))
        // Reactivated, while Kueue keeps the Evicted condition
        .thenReturn(Optional.of(workload(true, admitted(), evicted("Deactivated"))));
    AppKueueEvictionStep step = new AppKueueEvictionStep();
    for (int i = 0; i < 3; i++) {
      Assertions.assertEquals(ReconcileProgress.proceed(), step.reconcile(context, recorder));
    }

    List<String> messages = captureMessages(3);
    // Kueue stops counting the quota of a deactivated Workload
    Assertions.assertTrue(messages.get(0).contains("is deactivated, "));
    Assertions.assertTrue(messages.get(0).contains("no longer counts"));
    Assertions.assertTrue(
        messages.get(1).contains("is deactivated (Deactivated: Deactivated by the test)"));
    Assertions.assertTrue(messages.get(1).contains("no longer counts"));
    // And counts it again once it is reactivated
    Assertions.assertTrue(messages.get(2).contains("holding its quota"));
    verifyNoInteractions(recorder);
  }

  /** Returns the messages of the published events, which are all KueueEvictionIgnored warnings. */
  private List<String> captureMessages(int count) {
    ArgumentCaptor<EventRecord> captor = ArgumentCaptor.forClass(EventRecord.class);
    verify(eventRecorder, times(count)).record(captor.capture());
    for (EventRecord event : captor.getAllValues()) {
      Assertions.assertEquals(EventType.WARNING, event.type());
      Assertions.assertEquals(EventUtils.REASON_KUEUE_EVICTION_IGNORED, event.reason());
    }
    return captor.getAllValues().stream().map(EventRecord::message).toList();
  }

  private static Workload workload(boolean active, Condition... conditions) {
    Workload workload = new Workload();
    workload.setMetadata(new ObjectMetaBuilder().withName("sparkapplication-app1").build());
    workload.setSpec(WorkloadSpec.builder().active(active).build());
    workload.setStatus(WorkloadStatus.builder().conditions(List.of(conditions)).build());
    return workload;
  }

  private static Condition admitted() {
    return new ConditionBuilder().withType("Admitted").withStatus("True").build();
  }

  private static Condition evicted(String reason) {
    return new ConditionBuilder()
        .withType("Evicted")
        .withStatus("True")
        .withReason(reason)
        .withMessage(reason + " by the test")
        .build();
  }
}
