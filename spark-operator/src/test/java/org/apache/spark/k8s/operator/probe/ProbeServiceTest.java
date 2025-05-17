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

package org.apache.spark.k8s.operator.probe;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.OPERATOR_PROBE_PORT;
import static org.apache.spark.k8s.operator.probe.ProbeService.HEALTHZ;
import static org.apache.spark.k8s.operator.probe.ProbeService.READYZ;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.RuntimeInfo;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.metrics.healthcheck.SentinelManager;

@SuppressWarnings("PMD.UnitTestShouldIncludeAssert")
@EnableKubernetesMockClient
class ProbeServiceTest {
  @Test
  void testHealthProbeEndpointWithStaticProperties() throws Exception {
    Operator operator = mock(Operator.class);
    RuntimeInfo runtimeInfo = mock(RuntimeInfo.class);
    when(operator.getRuntimeInfo()).thenReturn(runtimeInfo);
    when(runtimeInfo.isStarted()).thenReturn(true).thenReturn(true);
    var sentinelManager = mock(SentinelManager.class);
    when(runtimeInfo.unhealthyInformerWrappingEventSourceHealthIndicator())
        .thenReturn(new HashMap<>());
    when(sentinelManager.allSentinelsAreHealthy()).thenReturn(true);
    ProbeService probeService =
        new ProbeService(
            Collections.singletonList(operator), Collections.singletonList(sentinelManager), null);
    probeService.start();
    hitHealthyEndpoint();
    probeService.stop();
  }

  @Test
  void testHealthProbeEndpointWithDynamicProperties() throws Exception {
    Operator operator = mock(Operator.class);
    Operator operator1 = mock(Operator.class);
    RuntimeInfo runtimeInfo = mock(RuntimeInfo.class);
    RuntimeInfo runtimeInfo1 = mock(RuntimeInfo.class);
    when(operator.getRuntimeInfo()).thenReturn(runtimeInfo);
    when(operator1.getRuntimeInfo()).thenReturn(runtimeInfo1);

    when(runtimeInfo.isStarted()).thenReturn(true).thenReturn(true);
    when(runtimeInfo1.isStarted()).thenReturn(true).thenReturn(true);

    var sentinelManager = mock(SentinelManager.class);
    when(runtimeInfo.unhealthyInformerWrappingEventSourceHealthIndicator())
        .thenReturn(new HashMap<>());
    when(runtimeInfo1.unhealthyInformerWrappingEventSourceHealthIndicator())
        .thenReturn(new HashMap<>());
    when(sentinelManager.allSentinelsAreHealthy()).thenReturn(true);
    ProbeService probeService =
        new ProbeService(
            List.of(operator, operator1), Collections.singletonList(sentinelManager), null);
    probeService.start();
    hitHealthyEndpoint();
    probeService.stop();
  }

  @Test
  void testReadinessProbeEndpointWithDynamicProperties() throws Exception {
    Operator operator = mock(Operator.class);
    Operator operator1 = mock(Operator.class);
    RuntimeInfo runtimeInfo = mock(RuntimeInfo.class);
    RuntimeInfo runtimeInfo1 = mock(RuntimeInfo.class);
    when(operator.getRuntimeInfo()).thenReturn(runtimeInfo);
    when(operator1.getRuntimeInfo()).thenReturn(runtimeInfo1);

    when(runtimeInfo.isStarted()).thenReturn(true).thenReturn(true);
    when(runtimeInfo1.isStarted()).thenReturn(true).thenReturn(true);

    var sentinelManager = mock(SentinelManager.class);
    KubernetesClient client = mock(KubernetesClient.class);
    when(runtimeInfo.unhealthyInformerWrappingEventSourceHealthIndicator())
        .thenReturn(new HashMap<>());
    when(runtimeInfo1.unhealthyInformerWrappingEventSourceHealthIndicator())
        .thenReturn(new HashMap<>());
    when(operator1.getKubernetesClient()).thenReturn(client);
    ProbeService probeService =
        new ProbeService(
            List.of(operator, operator1), Collections.singletonList(sentinelManager), null);
    probeService.start();
    hitStartedUpEndpoint();
    probeService.stop();
  }

  private void hitHealthyEndpoint() throws IOException, MalformedURLException {
    URL u = new URL("http://localhost:" + OPERATOR_PROBE_PORT.getValue() + HEALTHZ);
    HttpURLConnection connection = (HttpURLConnection) u.openConnection();
    connection.setConnectTimeout(100000);
    connection.connect();
    assertEquals(connection.getResponseCode(), HTTP_OK, "Health Probe should return HTTP_OK");
  }

  private void hitStartedUpEndpoint() throws IOException, MalformedURLException {
    URL u = new URL("http://localhost:" + OPERATOR_PROBE_PORT.getValue() + READYZ);
    HttpURLConnection connection = (HttpURLConnection) u.openConnection();
    connection.setConnectTimeout(100000);
    connection.connect();
    assertEquals(connection.getResponseCode(), HTTP_OK, "operators are not ready");
  }
}
