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

import static org.apache.spark.k8s.operator.config.SparkOperatorConf.OPERATOR_METRICS_PORT;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executor;

import com.sun.net.httpserver.HttpServer;
import lombok.extern.slf4j.Slf4j;

/** Start Http service at endpoint /prometheus, exposing operator metrics */
@Slf4j
public class MetricsService {
  HttpServer server;
  final MetricsSystem metricsSystem;

  public MetricsService(MetricsSystem metricsSystem, Executor executor) {
    this.metricsSystem = metricsSystem;
    try {
      server = HttpServer.create(new InetSocketAddress(OPERATOR_METRICS_PORT.getValue()), 0);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create Metrics Server", e);
    }
    server.setExecutor(executor);
  }

  public void start() {
    log.info("Starting Metrics Service for Prometheus ...");
    server.createContext("/prometheus", metricsSystem.getPrometheusPullModelHandler());
    server.start();
  }

  public void stop() {
    log.info("Metrics Service stopped");
    server.stop(0);
  }
}
