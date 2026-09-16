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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.assertArg;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.List;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.javaoperatorsdk.operator.api.event.EventRecord;
import io.javaoperatorsdk.operator.api.event.EventType;
import io.javaoperatorsdk.operator.api.event.ResourceEventRecorder;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.context.BaseContext;
import org.apache.spark.k8s.operator.listeners.SparkAppStatusListener;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;

@EnableKubernetesMockClient
@SuppressFBWarnings(
    value = {"UWF_UNWRITTEN_FIELD", "NP_UNWRITTEN_FIELD"},
    justification = "Unwritten fields are covered by Kubernetes mock client")
class StatusRecorderTest {

  static final String DEFAULT_NS = "default";
  KubernetesMockServer server;
  KubernetesClient client;

  SparkAppStatusListener mockStatusListener = mock(SparkAppStatusListener.class);

  ResourceEventRecorder mockEventRecorder = mock(ResourceEventRecorder.class);

  StatusRecorder<ApplicationStatus, SparkApplication, SparkAppStatusListener> statusRecorder =
      new StatusRecorder<>(
          List.of(mockStatusListener), ApplicationStatus.class, SparkApplication.class);

  @Test
  void refreshesResourceVersionOn409Conflict() {
    var testResource = getSparkApplication("1");
    var resourceV2 = getSparkApplication("2");
    var resourceV3 = getSparkApplication("3");

    var context = contextFor(testResource);
    var basePath =
        "/apis/spark.apache.org/v1/namespaces/"
            + DEFAULT_NS
            + "/sparkapplications/"
            + testResource.getMetadata().getName();
    var statusPath = basePath + "/status";
    // First status update returns 409 Conflict
    server.expect().withPath(statusPath).andReturn(409, null).once();
    // After 409, GET latest resource with resourceVersion "2"
    server.expect().withPath(basePath).andReturn(200, resourceV2).once();
    // Second status update succeeds with refreshed resourceVersion
    server.expect().withPath(statusPath).andReturn(200, resourceV3).once();

    statusRecorder.persistStatus(context, new ApplicationStatus());

    verify(mockStatusListener, times(1))
        .listenStatus(
            assertArg(a -> assertThat(a.getMetadata().getResourceVersion()).isEqualTo("3")),
            any(),
            any());
  }

  @Test
  void retriesFailedStatusPatches() {
    var testResource = getSparkApplication("1");
    var resourceV2 = getSparkApplication("2");
    var resourceV3 = getSparkApplication("3");

    var context = contextFor(testResource);
    var path =
        "/apis/spark.apache.org/v1/namespaces/"
            + DEFAULT_NS
            + "/sparkapplications/"
            + testResource.getMetadata().getName()
            + "/status";
    server.expect().withPath(path).andReturn(500, null).once();
    server.expect().withPath(path).andReturn(200, resourceV2).once();
    // this should be not called, thus updated resource should have resourceVersion 2
    server.expect().withPath(path).andReturn(200, resourceV3).once();

    statusRecorder.persistStatus(context, new ApplicationStatus());

    verify(mockStatusListener, times(1))
        .listenStatus(
            assertArg(a -> assertThat(a.getMetadata().getResourceVersion()).isEqualTo("2")),
            any(),
            any());
  }

  @Test
  void publishesAnEventWhenTheResourceEntersAFailureState() {
    var testResource = getSparkApplication("1");
    var context = contextFor(testResource);
    expectStatusPatch(testResource, getSparkApplication("2"));

    // The failure users actually hit: AppInitStep catches the rejected driver pod and records
    // SchedulingFailure rather than throwing, so no reconciler error hook ever sees it.
    statusRecorder.persistStatus(
        context,
        new ApplicationStatus()
            .appendNewState(
                new ApplicationState(
                    ApplicationStateSummary.SchedulingFailure, "exceeded quota for pods")));

    var event = captureRecordedEvent();
    assertThat(event.type()).isEqualTo(EventType.WARNING);
    assertThat(event.reason()).isEqualTo(ApplicationStateSummary.SchedulingFailure.name());
    assertThat(event.message()).isEqualTo("exceeded quota for pods");
    // Keyed on the state so an app that keeps re-entering it aggregates onto one Event.
    assertThat(event.key()).contains(ApplicationStateSummary.SchedulingFailure.name());
  }

  @Test
  void publishesANormalEventForANonFailureTransition() {
    var testResource = getSparkApplication("1");
    var context = contextFor(testResource);
    expectStatusPatch(testResource, getSparkApplication("2"));

    statusRecorder.persistStatus(
        context,
        new ApplicationStatus()
            .appendNewState(
                new ApplicationState(ApplicationStateSummary.DriverRequested, "driver requested")));

    var event = captureRecordedEvent();
    assertThat(event.type()).isEqualTo(EventType.NORMAL);
    assertThat(event.reason()).isEqualTo(ApplicationStateSummary.DriverRequested.name());
    assertThat(event.message()).isEqualTo("driver requested");
    assertThat(event.key()).contains(ApplicationStateSummary.DriverRequested.name());
  }

  @Test
  void publishesAWarningEventForANonFailureWarningState() {
    var testResource = getSparkApplication("1");
    var context = contextFor(testResource);
    expectStatusPatch(testResource, getSparkApplication("2"));

    statusRecorder.persistStatus(
        context,
        new ApplicationStatus()
            .appendNewState(
                new ApplicationState(
                    ApplicationStateSummary.RunningWithBelowThresholdExecutors, "lost executors")));

    var event = captureRecordedEvent();
    assertThat(event.type()).isEqualTo(EventType.WARNING);
    assertThat(event.reason())
        .isEqualTo(ApplicationStateSummary.RunningWithBelowThresholdExecutors.name());
  }

  @Test
  void publishesEveryTransition() {
    var testResource = getSparkApplication("1");
    var context = contextFor(testResource);
    expectStatusPatch(testResource, getSparkApplication("2"));

    var requested =
        new ApplicationStatus()
            .appendNewState(
                new ApplicationState(ApplicationStateSummary.DriverRequested, "requested"));
    statusRecorder.persistStatus(context, requested);
    statusRecorder.persistStatus(
        context,
        requested.appendNewState(
            new ApplicationState(ApplicationStateSummary.DriverStarted, "started")));

    ArgumentCaptor<EventRecord> captor = ArgumentCaptor.forClass(EventRecord.class);
    verify(mockEventRecorder, times(2)).record(captor.capture());
    assertThat(captor.getAllValues())
        .extracting(EventRecord::reason)
        .containsExactly(
            ApplicationStateSummary.DriverRequested.name(),
            ApplicationStateSummary.DriverStarted.name());
  }

  @Test
  void publishesEveryTransitionCarriedByASinglePatch() {
    var testResource = getSparkApplication("1");
    var context = contextFor(testResource);
    expectStatusPatch(testResource, getSparkApplication("2"));

    var requested =
        new ApplicationStatus()
            .appendNewState(
                new ApplicationState(ApplicationStateSummary.DriverRequested, "requested"));
    statusRecorder.persistStatus(context, requested);
    // A driver pod observed as both started and ready appends two states before one patch.
    statusRecorder.persistStatus(
        context,
        requested
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverStarted, "started"))
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverReady, "ready")));

    ArgumentCaptor<EventRecord> captor = ArgumentCaptor.forClass(EventRecord.class);
    verify(mockEventRecorder, times(3)).record(captor.capture());
    assertThat(captor.getAllValues())
        .extracting(EventRecord::reason)
        .containsExactly(
            ApplicationStateSummary.DriverRequested.name(),
            ApplicationStateSummary.DriverStarted.name(),
            ApplicationStateSummary.DriverReady.name());
  }

  @Test
  void publishesOnceForRepeatedStatesInASinglePatch() {
    var testResource = getSparkApplication("1");
    var context = contextFor(testResource);
    expectStatusPatch(testResource, getSparkApplication("2"));

    var started =
        new ApplicationStatus()
            .appendNewState(
                new ApplicationState(ApplicationStateSummary.DriverStarted, "started"));
    statusRecorder.persistStatus(context, started);
    // Both driver observers report the same failed driver pod, appending Failed twice.
    statusRecorder.persistStatus(
        context,
        started
            .appendNewState(new ApplicationState(ApplicationStateSummary.Failed, "driver failed"))
            .appendNewState(new ApplicationState(ApplicationStateSummary.Failed, "driver failed")));

    ArgumentCaptor<EventRecord> captor = ArgumentCaptor.forClass(EventRecord.class);
    verify(mockEventRecorder, times(2)).record(captor.capture());
    assertThat(captor.getAllValues())
        .extracting(EventRecord::reason)
        .containsExactly(
            ApplicationStateSummary.DriverStarted.name(), ApplicationStateSummary.Failed.name());
  }

  @Test
  void publishesNoEventWhenTheCurrentStateDidNotChange() {
    var testResource = getSparkApplication("1");
    var context = contextFor(testResource);
    expectStatusPatch(testResource, getSparkApplication("2"));

    var running =
        new ApplicationStatus()
            .appendNewState(
                new ApplicationState(ApplicationStateSummary.RunningHealthy, "running"));
    statusRecorder.persistStatus(context, running);
    // The status changes and is patched, but the current state is the same one, so the resource
    // has not transitioned again and must not be reported again.
    var sameStateNewAttempt =
        new ApplicationStatus(
            running.getCurrentState(),
            running.getStateTransitionHistory(),
            running.getCurrentAttemptSummary(),
            running.getCurrentAttemptSummary());
    statusRecorder.persistStatus(context, sameStateNewAttempt);

    verify(mockStatusListener, times(2)).listenStatus(any(), any(), any());
    verify(mockEventRecorder, times(1)).record(any(EventRecord.class));
  }

  @Test
  void publishesNoEventWhenTheStatusDidNotChange() {
    var testResource = getSparkApplication("1");
    var context = contextFor(testResource);
    var failed =
        new ApplicationStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.Failed, "driver failed"));
    expectStatusPatch(testResource, getSparkApplication("2"));

    statusRecorder.persistStatus(context, failed);
    // Second call with the very same status: patchAndStatusWithVersionLocked short circuits, so
    // the resource has not transitioned again and must not be reported again.
    statusRecorder.persistStatus(context, failed);

    verify(mockEventRecorder, times(1)).record(any(EventRecord.class));
  }

  @Test
  void publishesAnEventWhenTheStatusPatchIsRejected() {
    var testResource = getSparkApplication("1");
    var context = contextFor(testResource);
    // 403 is a decision by a reachable API server, so reporting it costs a healthy request.
    server.expect().withPath(statusPathOf(testResource)).andReturn(403, null).always();

    assertThat(statusRecorder.persistStatus(context, new ApplicationStatus())).isFalse();

    var event = captureRecordedEvent();
    assertThat(event.reason()).isEqualTo(EventUtils.REASON_STATUS_UPDATE_FAILED);
    assertThat(event.message()).contains("the reported status may be stale");
  }

  @Test
  void publishesNoEventWhenTheApiServerIsUnreachable() {
    var testResource = getSparkApplication("1");
    var context = contextFor(testResource);
    // 503 means the control plane is already degraded. Writing an event costs two more requests
    // against it, so the observability path must not pile on.
    server.expect().withPath(statusPathOf(testResource)).andReturn(503, null).always();

    assertThat(statusRecorder.persistStatus(context, new ApplicationStatus())).isFalse();

    verifyNoInteractions(mockEventRecorder);
  }

  private BaseContext<SparkApplication> contextFor(SparkApplication resource) {
    BaseContext<SparkApplication> context = mock(BaseContext.class);
    when(context.getResource()).thenReturn(resource);
    when(context.getClient()).thenReturn(client);
    when(context.getEventRecorder()).thenReturn(mockEventRecorder);
    return context;
  }

  private void expectStatusPatch(SparkApplication resource, SparkApplication updated) {
    server.expect().withPath(statusPathOf(resource)).andReturn(200, updated).always();
  }

  private static String statusPathOf(SparkApplication resource) {
    return "/apis/spark.apache.org/v1/namespaces/"
        + DEFAULT_NS
        + "/sparkapplications/"
        + resource.getMetadata().getName()
        + "/status";
  }

  private EventRecord captureRecordedEvent() {
    ArgumentCaptor<EventRecord> captor = ArgumentCaptor.forClass(EventRecord.class);
    verify(mockEventRecorder).record(captor.capture());
    return captor.getValue();
  }

  private static SparkApplication getSparkApplication(String resourceVersion) {
    var updated = TestUtils.createMockApp(DEFAULT_NS);
    updated.getMetadata().setResourceVersion(resourceVersion);
    return updated;
  }
}
