/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.kubernetes.operator.metrics;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.kubernetes.operator.metrics.sink.MockSink;
import org.apache.spark.metrics.sink.Sink;
import org.apache.spark.metrics.source.Source;

class MetricsSystemTest {
  @Test
  void testMetricsSystemWithResourcesAdd() {
    MetricsSystem metricsSystem = new MetricsSystem();
    List<Source> sourcesList = metricsSystem.getSources();
    List<Sink> sinks = metricsSystem.getSinks();
    metricsSystem.start();
    Assertions.assertEquals(1, sourcesList.size());
    // Default no sink added
    Assertions.assertEquals(0, sinks.size());
    Assertions.assertFalse(metricsSystem.getRegistry().getMetrics().isEmpty());
    metricsSystem.stop();
    Assertions.assertTrue(metricsSystem.getRegistry().getMetrics().isEmpty());
  }

  @Test
  void testMetricsSystemWithCustomizedSink() {
    Properties properties = new Properties();
    properties.put("sink.mocksink.class",
        "org.apache.spark.kubernetes.operator.metrics.sink.MockSink");
    properties.put("sink.mocksink.period", "10");
    MetricsSystem metricsSystem = new MetricsSystem(properties);
    metricsSystem.start();
    Sink mockSink = metricsSystem.getSinks().get(0);
    metricsSystem.stop();
    MockSink sink = (MockSink) mockSink;
    Assertions.assertEquals(sink.getPollPeriod(), 10);
    Assertions.assertEquals(sink.getTimeUnit(), TimeUnit.SECONDS);
  }

  @Test
  void testMetricsSystemWithTwoSinkConfigurations() {
    Properties properties = new Properties();
    properties.put("sink.mocksink.class",
        "org.apache.spark.kubernetes.operator.metrics.sink.MockSink");
    properties.put("sink.mocksink.period", "10");
    properties.put("sink.console.class", "org.apache.spark.metrics.sink.ConsoleSink");
    MetricsSystem metricsSystem = new MetricsSystem(properties);
    metricsSystem.start();
    Assertions.assertEquals(2, metricsSystem.getSinks().size());
  }
}
