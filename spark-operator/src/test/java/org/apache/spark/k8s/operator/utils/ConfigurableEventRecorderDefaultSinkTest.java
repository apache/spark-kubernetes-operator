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

import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.apache.spark.k8s.operator.Constants.HTTP_TOO_MANY_REQUESTS;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.KUBERNETES_EVENTS_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.Map;

import io.fabric8.kubernetes.api.model.Event;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.javaoperatorsdk.operator.api.config.ConfigurationService;
import io.javaoperatorsdk.operator.api.config.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.event.EventRecord;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.config.SparkOperatorConfManager;

/**
 * Tests {@link ConfigurableEventRecorder#withDefaultSink} against a mock API server. It is
 * separate from {@link ConfigurableEventRecorderTest}, since the mock server extension starts a
 * server for every test of the class which it annotates.
 */
@EnableKubernetesMockClient(crud = true)
class ConfigurableEventRecorderDefaultSinkTest {

  private KubernetesMockServer server;
  private KubernetesClient client;

  @AfterEach
  void resetConf() {
    SparkOperatorConfManager.INSTANCE.refresh(Map.of());
  }

  @ParameterizedTest
  @ValueSource(ints = {HTTP_TOO_MANY_REQUESTS, HTTP_UNAVAILABLE})
  void dropsAFailedWriteRatherThanRetryingIt(int code) {
    // Only the first create fails, so a retry would have created the Event.
    SparkOperatorConfManager.INSTANCE.refresh(Map.of(KUBERNETES_EVENTS_ENABLED.getKey(), "true"));
    ConfigurableEventRecorder recorder = ConfigurableEventRecorder.withDefaultSink(client);
    Context<?> context = appContext();
    server.expect().post().withPath("/api/v1/namespaces/ns-1/events").andReturn(code, null).once();

    recorder.record(EventRecord.warning("ReconcileError", "dropped"), context);
    assertThat(client.v1().events().inNamespace("ns-1").list().getItems()).isEmpty();

    // The next write goes through, so the first one was dropped only for the injected failure.
    recorder.record(EventRecord.warning("ReconcileError", "published"), context);
    assertThat(client.v1().events().inNamespace("ns-1").list().getItems())
        .extracting(Event::getMessage)
        .containsExactly("published");
  }

  /** Returns a context from which the JOSDK recorder can build an Event about app-1 in ns-1. */
  private static Context<?> appContext() {
    SparkApplication app = new SparkApplication();
    app.setMetadata(
        new ObjectMetaBuilder().withName("app-1").withNamespace("ns-1").withUid("uid-1").build());
    ControllerConfiguration<?> configuration = mock(ControllerConfiguration.class);
    doReturn(mock(ConfigurationService.class)).when(configuration).getConfigurationService();
    Context<?> context = mock(Context.class);
    doReturn(app).when(context).getPrimaryResource();
    doReturn(configuration).when(context).getControllerConfiguration();
    return context;
  }
}
