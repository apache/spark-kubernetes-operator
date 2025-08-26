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

@RequiredArgsConstructor
public class BaseOperatorSource {
  protected final MetricRegistry metricRegistry;
  protected final Map<String, Histogram> histograms = new ConcurrentHashMap<>();
  protected final Map<String, Counter> counters = new ConcurrentHashMap<>();
  protected final Map<String, Gauge<?>> gauges = new ConcurrentHashMap<>();
  protected final Map<String, Timer> timers = new ConcurrentHashMap<>();

  protected Histogram getHistogram(String metricName) {
    Histogram histogram;
    if (histograms.containsKey(metricName)) {
      histogram = histograms.get(metricName);
    } else {
      histogram = metricRegistry.histogram(metricName);
      histograms.put(metricName, histogram);
    }
    return histogram;
  }

  protected Counter getCounter(String metricName) {
    Counter counter;
    if (counters.containsKey(metricName)) {
      counter = counters.get(metricName);
    } else {
      counter = metricRegistry.counter(metricName);
      counters.put(metricName, counter);
    }
    return counter;
  }

  protected Gauge<?> getGauge(Gauge<?> defaultGauge, String metricName) {
    Gauge<?> gauge;
    if (gauges.containsKey(metricName)) {
      gauge = gauges.get(metricName);
    } else {
      gauge = defaultGauge;
      gauges.put(metricName, defaultGauge);
    }
    return gauge;
  }

  protected Timer getTimer(String metricName) {
    Timer timer;
    if (timers.containsKey(metricName)) {
      timer = timers.get(metricName);
    } else {
      timer = metricRegistry.timer(metricName);
      timers.put(metricName, timer);
    }
    return timer;
  }

  public String getMetricName(Class<?> klass, String... names) {
    return MetricRegistry.name(klass.getSimpleName(), names).toLowerCase();
  }
}
