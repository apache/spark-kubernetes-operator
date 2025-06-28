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

package org.apache.spark.k8s.operator.metrics;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.spark.k8s.operator.utils.ProbeUtil.sendMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.codahale.metrics.MetricRegistry;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.metrics.sink.PrometheusServlet;

/** Serves as simple Prometheus sink (pull model), presenting metrics snapshot as HttpHandler. */
@Slf4j
public class PrometheusPullModelHandler extends PrometheusServlet implements HttpHandler {
  private static final String EMPTY_RECORD_VALUE = "[]";

  public PrometheusPullModelHandler(Properties properties, MetricRegistry registry) {
    super(properties, registry);
  }

  @Override
  public void start() {
    log.info("PrometheusPullModelHandler started");
  }

  @Override
  public void stop() {
    log.info("PrometheusPullModelHandler stopped");
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    HttpServletRequest httpServletRequest = null;
    String value = getMetricsSnapshot(httpServletRequest);
    sendMessage(
        exchange,
        HTTP_OK,
        String.join("\n", filterNonEmptyRecords(value)),
        Map.of("Content-Type", Collections.singletonList("text/plain;version=0.0.4")));
  }

  protected List<String> filterNonEmptyRecords(String metricsSnapshot) {
    // filter out empty records which could cause Prometheus invalid syntax exception, e.g:
    // metrics_jvm_threadStates_deadlocks_Number{type="gauges"} []
    // metrics_jvm_threadStates_deadlocks_Value{type="gauges"} []
    String[] records = metricsSnapshot.split("\n");
    List<String> filteredRecords = new ArrayList<>();
    for (String record : records) {
      String[] keyValuePair = record.split(" ");
      if (EMPTY_RECORD_VALUE.equals(keyValuePair[1])) {
        continue;
      }
      filteredRecords.add(record);
    }
    return filteredRecords;
  }
}
