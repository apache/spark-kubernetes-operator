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

import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.metrics.sink.MockSink;
import org.apache.spark.metrics.sink.Sink;
import org.apache.spark.metrics.source.Source;

class MetricsSystemTest {
  @Test
  void testMetricsSystemWithResourcesAdd() {
    MetricsSystem metricsSystem = new MetricsSystem();
    Set<Source> sourcesList = metricsSystem.getSources();
    Set<Sink> sinks = metricsSystem.getSinks();
    metricsSystem.start();
    Assertions.assertEquals(1, sourcesList.size());
    // By default, only prometheus sink is enabled
    Assertions.assertEquals(1, sinks.size());
    Assertions.assertFalse(metricsSystem.getRegistry().getMetrics().isEmpty());
    metricsSystem.stop();
    Assertions.assertTrue(metricsSystem.getRegistry().getMetrics().isEmpty());
  }

  @Test
  void testMetricsSystemWithCustomizedSink() {
    Properties properties = new Properties();
    properties.put("sink.mocksink.class", "org.apache.spark.k8s.operator.metrics.sink.MockSink");
    properties.put("sink.mocksink.period", "10");
    MetricsSystem metricsSystem = new MetricsSystem(properties);
    metricsSystem.start();
    Assertions.assertEquals(2, metricsSystem.getSinks().size());
    Optional<Sink> mockSinkOptional =
        metricsSystem.getSinks().stream().filter(sink -> sink instanceof MockSink).findFirst();
    Assertions.assertTrue(mockSinkOptional.isPresent());
    Sink mockSink = mockSinkOptional.get();
    metricsSystem.stop();
    MockSink sink = (MockSink) mockSink;
    Assertions.assertEquals(sink.getPollPeriod(), 10);
    Assertions.assertEquals(sink.getTimeUnit(), TimeUnit.SECONDS);
  }

  @Test
  void testMetricsSystemWithTwoSinkConfigurations() {
    Properties properties = new Properties();
    properties.put("sink.mocksink.class", "org.apache.spark.k8s.operator.metrics.sink.MockSink");
    properties.put("sink.mocksink.period", "10");
    properties.put("sink.console.class", "org.apache.spark.metrics.sink.ConsoleSink");
    MetricsSystem metricsSystem = new MetricsSystem(properties);
    metricsSystem.start();
    Assertions.assertEquals(3, metricsSystem.getSinks().size());
  }
}
