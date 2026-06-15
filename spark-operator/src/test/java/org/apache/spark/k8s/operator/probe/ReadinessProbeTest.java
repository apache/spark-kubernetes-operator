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

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.OutputStream;

import com.sun.net.httpserver.HttpExchange;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.RuntimeInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.apache.spark.k8s.operator.config.DynamicConfigMonitor;
import org.apache.spark.k8s.operator.utils.ProbeUtil;

class ReadinessProbeTest {
  HttpExchange httpExchange;

  @BeforeEach
  void beforeEach() {
    OutputStream outputStream = mock(OutputStream.class);
    httpExchange = mock(HttpExchange.class);
    when(httpExchange.getResponseBody()).thenReturn(outputStream);
  }

  @Test
  void testHandleSucceed() throws IOException {
    Operator operator = mock(Operator.class);
    RuntimeInfo runtimeInfo = mock(RuntimeInfo.class);
    when(operator.getRuntimeInfo()).thenReturn(runtimeInfo);
    when(runtimeInfo.isStarted()).thenReturn(true);
    ReadinessProbe readinessProbe = new ReadinessProbe(operator, null);
    try (var mockedStatic = Mockito.mockStatic(ProbeUtil.class)) {
      mockedStatic.when(() -> ProbeUtil.isOperatorStarted(operator)).thenReturn(true);
      readinessProbe.handle(httpExchange);
      mockedStatic.verify(() -> ProbeUtil.sendMessage(httpExchange, HTTP_OK, "started"));
    }
  }

  @Test
  void testHandleSucceedWithRunningDynamicConfigMonitor() throws IOException {
    Operator operator = mock(Operator.class);
    DynamicConfigMonitor dynamicConfigMonitor = mock(DynamicConfigMonitor.class);
    when(dynamicConfigMonitor.isRunning()).thenReturn(true);
    ReadinessProbe readinessProbe = new ReadinessProbe(operator, dynamicConfigMonitor);
    try (var mockedStatic = Mockito.mockStatic(ProbeUtil.class)) {
      mockedStatic.when(() -> ProbeUtil.isOperatorStarted(operator)).thenReturn(true);
      readinessProbe.handle(httpExchange);
      mockedStatic.verify(() -> ProbeUtil.sendMessage(httpExchange, HTTP_OK, "started"));
    }
  }

  @Test
  void testHandleFailsWhenDynamicConfigMonitorNotRunning() throws IOException {
    Operator operator = mock(Operator.class);
    DynamicConfigMonitor dynamicConfigMonitor = mock(DynamicConfigMonitor.class);
    when(dynamicConfigMonitor.isRunning()).thenReturn(false);
    ReadinessProbe readinessProbe = new ReadinessProbe(operator, dynamicConfigMonitor);
    try (var mockedStatic = Mockito.mockStatic(ProbeUtil.class)) {
      mockedStatic.when(() -> ProbeUtil.isOperatorStarted(operator)).thenReturn(true);
      readinessProbe.handle(httpExchange);
      mockedStatic.verify(
          () ->
              ProbeUtil.sendMessage(
                  httpExchange, HTTP_BAD_REQUEST, "dynamic config monitor is not running yet"));
    }
  }
}
