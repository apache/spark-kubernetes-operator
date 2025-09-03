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

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Properties;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.junit.jupiter.api.Test;

class PrometheusPullModelHandlerTest {

  @Test
  void testFormatMetricsSnapshotIncludesGauge() throws Exception {
    MetricRegistry registry = new MetricRegistry();
    registry.register("foo_gauge", (Gauge<Integer>) () -> 42);

    PrometheusPullModelHandler handler = new PrometheusPullModelHandler(new Properties(), registry);

    String output = handler.formatMetricsSnapshot();
    assertTrue(output.contains("# TYPE foo_gauge gauge"));
    assertTrue(output.contains("foo_gauge 42"));
  }

  @Test
  void testFormatMetricsSnapshotIncludesCounter() throws Exception {
    MetricRegistry registry = new MetricRegistry();
    Counter counter = registry.counter("foo_counter");
    counter.inc(5);

    PrometheusPullModelHandler handler = new PrometheusPullModelHandler(new Properties(), registry);

    String output = handler.formatMetricsSnapshot();
    assertTrue(output.contains("# TYPE foo_counter_total counter"));
    assertTrue(output.contains("foo_counter_total 5"));
  }

  @Test
  void testFormatMetricsSnapshotIncludesHistogram() throws Exception {
    MetricRegistry registry = new MetricRegistry();
    Histogram histogram = registry.histogram("foo_histogram");
    histogram.update(100);
    histogram.update(200);

    PrometheusPullModelHandler handler = new PrometheusPullModelHandler(new Properties(), registry);

    String output = handler.formatMetricsSnapshot();

    assertTrue(output.contains("# TYPE foo_histogram summary"));
    assertTrue(output.contains("foo_histogram_count 2"));
    assertTrue(output.contains("foo_histogram_sum"));
  }

  @Test
  void testFormatMetricsSnapshotIncludesHistogramWithNanos() throws Exception {
    MetricRegistry registry = new MetricRegistry();
    Histogram histogram = registry.histogram("foo_nanos_histogram");
    histogram.update(563682);
    histogram.update(716252);
    histogram.update(292098);

    PrometheusPullModelHandler handler = new PrometheusPullModelHandler(new Properties(), registry);

    String output = handler.formatMetricsSnapshot();

    assertTrue(output.contains("# TYPE foo_seconds_histogram summary"));
    assertTrue(output.contains("foo_seconds_histogram_count 3"));
    assertTrue(output.contains("foo_seconds_histogram_sum 0.001572032"));
  }

  @Test
  void testFormatMetricsSnapshotIncludesMeter() throws Exception {
    MetricRegistry registry = new MetricRegistry();
    Meter meter = registry.meter("foo_meter");
    meter.mark(3);

    PrometheusPullModelHandler handler = new PrometheusPullModelHandler(new Properties(), registry);

    String output = handler.formatMetricsSnapshot();
    assertTrue(output.contains("# TYPE foo_meter_total counter"));
    assertTrue(output.contains("foo_meter_total 3"));
    assertTrue(output.contains("foo_meter_m1_rate"));
  }

  @Test
  void testFormatMetricsSnapshotIncludesTimer() throws Exception {
    MetricRegistry registry = new MetricRegistry();
    Timer timer = registry.timer("foo_timer");

    timer.update(Duration.of(500, ChronoUnit.MILLIS));
    timer.update(Duration.of(1000, ChronoUnit.MILLIS));
    PrometheusPullModelHandler handler = new PrometheusPullModelHandler(new Properties(), registry);

    String output = handler.formatMetricsSnapshot();
    assertTrue(output.contains("# TYPE foo_timer_duration_seconds summary"));
    assertTrue(output.contains("foo_timer_duration_seconds_count 2"));
    assertTrue(output.contains("foo_timer_duration_seconds_sum 1.5"));
    assertTrue(output.contains("foo_timer_m1_rate"));
  }

  @Test
  void testFormatMetricsSnapshotIncludesEmpty() throws Exception {
    MetricRegistry registry = new MetricRegistry();
    registry.register("foo_gauge", (Gauge<Objects>) () -> null);
    PrometheusPullModelHandler handler = new PrometheusPullModelHandler(new Properties(), registry);

    String output = handler.formatMetricsSnapshot();
    assertFalse(output.contains("null"));
  }
}
