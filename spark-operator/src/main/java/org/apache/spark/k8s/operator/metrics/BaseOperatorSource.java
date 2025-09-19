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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import lombok.RequiredArgsConstructor;

/** Base class for operator metric sources. */
@RequiredArgsConstructor
public class BaseOperatorSource {
  protected final MetricRegistry metricRegistry;
  protected final Map<String, Histogram> histograms = new ConcurrentHashMap<>();
  protected final Map<String, Counter> counters = new ConcurrentHashMap<>();
  protected final Map<String, Gauge<?>> gauges = new ConcurrentHashMap<>();
  protected final Map<String, Timer> timers = new ConcurrentHashMap<>();

  /**
   * Retrieves or creates a Histogram metric.
   *
   * @param metricNamePrefix The prefix for the metric name.
   * @param names Additional names to form the full metric name.
   * @return A Histogram instance.
   */
  protected Histogram getHistogram(String metricNamePrefix, String... names) {
    Histogram histogram;
    String metricName = MetricRegistry.name(metricNamePrefix, names).toLowerCase();
    if (histograms.containsKey(metricName)) {
      histogram = histograms.get(metricName);
    } else {
      histogram = metricRegistry.histogram(metricName);
      histograms.put(metricName, histogram);
    }
    return histogram;
  }

  /**
   * Retrieves or creates a Counter metric.
   *
   * @param metricNamePrefix The prefix for the metric name.
   * @param names Additional names to form the full metric name.
   * @return A Counter instance.
   */
  protected Counter getCounter(String metricNamePrefix, String... names) {
    Counter counter;
    String metricName = MetricRegistry.name(metricNamePrefix, names).toLowerCase();
    if (counters.containsKey(metricName)) {
      counter = counters.get(metricName);
    } else {
      counter = metricRegistry.counter(metricName);
      counters.put(metricName, counter);
    }
    return counter;
  }

  /**
   * Retrieves or creates a Gauge metric.
   *
   * @param defaultGauge The default gauge to use if one doesn't exist.
   * @param metricNamePrefix The prefix for the metric name.
   * @param names Additional names to form the full metric name.
   * @return A Gauge instance.
   */
  protected Gauge<?> getGauge(Gauge<?> defaultGauge, String metricNamePrefix, String... names) {
    Gauge<?> gauge;
    String metricName = MetricRegistry.name(metricNamePrefix, names).toLowerCase();
    if (gauges.containsKey(metricName)) {
      gauge = gauges.get(metricName);
    } else {
      gauge = defaultGauge;
      gauges.put(metricName, defaultGauge);
    }
    return gauge;
  }

  /**
   * Retrieves or creates a Timer metric.
   *
   * @param metricNamePrefix The prefix for the metric name.
   * @param names Additional names to form the full metric name.
   * @return A Timer instance.
   */
  protected Timer getTimer(String metricNamePrefix, String... names) {
    Timer timer;
    String metricName = MetricRegistry.name(metricNamePrefix, names).toLowerCase();
    if (timers.containsKey(metricName)) {
      timer = timers.get(metricName);
    } else {
      timer = metricRegistry.timer(metricName);
      timers.put(metricName, timer);
    }
    return timer;
  }

  /**
   * Returns the simple class name as the metric name prefix.
   *
   * @param klass The class to get the simple name from.
   * @return The simple class name.
   */
  protected String getMetricNamePrefix(Class<?> klass) {
    return klass.getSimpleName();
  }
}
