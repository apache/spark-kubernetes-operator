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

package org.apache.spark.kubernetes.operator.metrics;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;

import com.sun.net.httpserver.HttpServer;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.kubernetes.operator.metrics.sink.PrometheusPullModelSink;
import org.apache.spark.metrics.sink.Sink;

import static org.apache.spark.kubernetes.operator.config.SparkOperatorConf.OperatorMetricsPort;

@Slf4j
public class MetricsService {
  HttpServer server;
  MetricsSystem metricsSystem;

  public MetricsService(MetricsSystem metricsSystem) {
    this.metricsSystem = metricsSystem;
    try {
      server = HttpServer.create(new InetSocketAddress(OperatorMetricsPort.getValue()), 0);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create Metrics Server", e);
    }
    server.setExecutor(null);
  }

  public void start() {
    log.info("Metrics Service started");
    List<Sink> sinks = metricsSystem.getSinks();
    Optional<Sink> instanceOptional =
        sinks.stream().filter(x -> x instanceof PrometheusPullModelSink).findAny();
    instanceOptional.ifPresent(sink ->
        server.createContext("/prometheus", (PrometheusPullModelSink) sink));
    server.start();
  }

  public void stop() {
    log.info("Metrics Service stopped");
    server.stop(0);
  }
}
