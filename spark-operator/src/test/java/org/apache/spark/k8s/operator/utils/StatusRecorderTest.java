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
import static org.mockito.Mockito.when;

import java.util.List;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.context.BaseContext;
import org.apache.spark.k8s.operator.listeners.SparkAppStatusListener;
import org.apache.spark.k8s.operator.status.ApplicationStatus;

@EnableKubernetesMockClient
@SuppressFBWarnings(
    value = {"UWF_UNWRITTEN_FIELD", "NP_UNWRITTEN_FIELD"},
    justification = "Unwritten fields are covered by Kubernetes mock client")
class StatusRecorderTest {

  public static final String DEFAULT_NS = "default";
  KubernetesMockServer server;
  KubernetesClient client;

  SparkAppStatusListener mockStatusListener = mock(SparkAppStatusListener.class);

  StatusRecorder<ApplicationStatus, SparkApplication, SparkAppStatusListener> statusRecorder =
      new StatusRecorder<>(
          List.of(mockStatusListener), ApplicationStatus.class, SparkApplication.class);

  @Test
  void retriesFailedStatusPatches() {
    var testResource = getSparkApplication("1");
    var resourceV2 = getSparkApplication("2");
    var resourceV3 = getSparkApplication("3");

    BaseContext<SparkApplication> context = mock(BaseContext.class);
    when(context.getResource()).thenReturn(testResource);
    when(context.getClient()).thenReturn(client);
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

  private static @NotNull SparkApplication getSparkApplication(String resourceVersion) {
    var updated = TestUtils.createMockApp(DEFAULT_NS);
    updated.getMetadata().setResourceVersion(resourceVersion);
    return updated;
  }
}
