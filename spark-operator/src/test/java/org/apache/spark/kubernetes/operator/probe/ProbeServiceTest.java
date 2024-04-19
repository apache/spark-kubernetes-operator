/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.spark.kubernetes.operator.probe;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.RuntimeInfo;
import org.junit.jupiter.api.Test;

import org.apache.spark.kubernetes.operator.health.SentinelManager;

import static org.apache.spark.kubernetes.operator.config.SparkOperatorConf.OperatorProbePort;
import static org.apache.spark.kubernetes.operator.probe.ProbeService.HEALTHZ;
import static org.apache.spark.kubernetes.operator.probe.ProbeService.READYZ;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("PMD.JUnitTestsShouldIncludeAssert")
@EnableKubernetesMockClient
class ProbeServiceTest {
  @Test
  void testHealthProbeEndpointWithStaticProperties() throws Exception {
    Operator operator = mock(Operator.class);
    RuntimeInfo runtimeInfo = mock(RuntimeInfo.class);
    when(operator.getRuntimeInfo()).thenReturn(runtimeInfo);
    when(runtimeInfo.isStarted()).thenReturn(true).thenReturn(true);
    SentinelManager sentinelManager = mock(SentinelManager.class);
    when(runtimeInfo.unhealthyInformerWrappingEventSourceHealthIndicator()).thenReturn(
        new HashMap<>());
    when(sentinelManager.allSentinelsAreHealthy()).thenReturn(true);
    ProbeService probeService = new ProbeService(Arrays.asList(operator), sentinelManager);
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

    SentinelManager sentinelManager = mock(SentinelManager.class);
    when(runtimeInfo.unhealthyInformerWrappingEventSourceHealthIndicator()).thenReturn(
        new HashMap<>());
    when(runtimeInfo1.unhealthyInformerWrappingEventSourceHealthIndicator()).thenReturn(
        new HashMap<>());
    when(sentinelManager.allSentinelsAreHealthy()).thenReturn(true);
    ProbeService probeService =
        new ProbeService(Arrays.asList(operator, operator1), sentinelManager);
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

    SentinelManager sentinelManager = mock(SentinelManager.class);
    KubernetesClient client = mock(KubernetesClient.class);
    when(runtimeInfo.unhealthyInformerWrappingEventSourceHealthIndicator()).thenReturn(
        new HashMap<>());
    when(runtimeInfo1.unhealthyInformerWrappingEventSourceHealthIndicator()).thenReturn(
        new HashMap<>());
    when(operator1.getKubernetesClient()).thenReturn(client);
    ProbeService probeService =
        new ProbeService(Arrays.asList(operator, operator1), sentinelManager);
    probeService.start();
    hitStartedUpEndpoint();
    probeService.stop();
  }

  private void hitHealthyEndpoint() throws Exception {
    URL u = new URL("http://localhost:" + OperatorProbePort.getValue() + HEALTHZ);
    HttpURLConnection connection = (HttpURLConnection) u.openConnection();
    connection.setConnectTimeout(100000);
    connection.connect();
    assertEquals(connection.getResponseCode(), 200, "Health Probe should return 200");
  }

  private void hitStartedUpEndpoint() throws Exception {
    URL u = new URL("http://localhost:" + OperatorProbePort.getValue() + READYZ);
    HttpURLConnection connection = (HttpURLConnection) u.openConnection();
    connection.setConnectTimeout(100000);
    connection.connect();
    assertEquals(connection.getResponseCode(), 200, "operators are not ready");
  }
}
