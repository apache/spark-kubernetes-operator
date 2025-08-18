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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import jakarta.servlet.http.HttpServletRequest;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import org.apache.spark.k8s.operator.config.SparkOperatorConf;
import org.apache.spark.metrics.sink.PrometheusServlet;

/** Serves as simple Prometheus sink (pull model), presenting metrics snapshot as HttpHandler. */
@Slf4j
public class PrometheusPullModelHandler extends PrometheusServlet implements HttpHandler {
  private static final String EMPTY_RECORD_VALUE = "[]";
  @Getter private final MetricRegistry registry;
  @Getter private final boolean enablePrometheusTextBasedFormat;
  @Getter private final boolean enableSanitizePrometheusMetricsName;

  public PrometheusPullModelHandler(Properties properties, MetricRegistry registry) {
    super(properties, registry);
    this.registry = registry;
    this.enablePrometheusTextBasedFormat =
        SparkOperatorConf.EnablePrometheusTextBasedFormat.getValue();
    this.enableSanitizePrometheusMetricsName =
        SparkOperatorConf.EnableSanitizePrometheusMetricsName.getValue();
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
    if (SparkOperatorConf.EnablePrometheusTextBasedFormat.getValue()) {
      sendMessage(
          exchange,
          HTTP_OK,
          formatMetricsSnapshot(),
          Map.of("Content-Type", Collections.singletonList("text/plain;version=0.0.4")));
    } else {
      HttpServletRequest httpServletRequest = null;
      String value = getMetricsSnapshot(httpServletRequest);
      sendMessage(
          exchange,
          HTTP_OK,
          String.join("\n", filterNonEmptyRecords(value)),
          Map.of("Content-Type", Collections.singletonList("text/plain;version=0.0.4")));
    }
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

  protected String formatMetricsSnapshot() {
    Map<String, Gauge> gauges = registry.getGauges();
    Map<String, Counter> counters = registry.getCounters();
    Map<String, Histogram> histograms = registry.getHistograms();
    Map<String, Meter> meters = registry.getMeters();
    Map<String, Timer> timers = registry.getTimers();

    StringBuilder stringBuilder = new StringBuilder();

    for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
      appendIfNotEmpty(stringBuilder, formatGauge(entry.getKey(), entry.getValue()));
    }

    // Counters
    for (Map.Entry<String, Counter> entry : counters.entrySet()) {
      String name = sanitize(entry.getKey()) + "_total";
      Counter counter = entry.getValue();
      appendIfNotEmpty(stringBuilder, formatCounter(name, counter));
    }

    // Histograms
    for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
      appendIfNotEmpty(stringBuilder, formatHistogram(entry.getKey(), entry.getValue()));
    }

    // Meters
    for (Map.Entry<String, Meter> entry : meters.entrySet()) {
      appendIfNotEmpty(stringBuilder, formatMeter(entry.getKey(), entry.getValue()));
    }

    // Timers (Meter + Histogram in nanoseconds)
    for (Map.Entry<String, Timer> entry : timers.entrySet()) {
      appendIfNotEmpty(stringBuilder, formatTimer(entry.getKey(), entry.getValue()));
    }
    return stringBuilder.toString();
  }

  protected void appendIfNotEmpty(StringBuilder stringBuilder, String value) {
    if (StringUtils.isNotEmpty(value)) {
      stringBuilder.append(value);
    }
  }

  protected String formatGauge(String name, Gauge gauge) {
    if (gauge != null
        && gauge.getValue() != null
        && !EMPTY_RECORD_VALUE.equals(gauge.getValue())
        && gauge.getValue() instanceof Number) {
      String formattedName = sanitize(name);
      return "# HELP "
          + formattedName
          + " Gauge metric\n"
          + "# TYPE "
          + formattedName
          + " gauge\n"
          + sanitize(formattedName)
          + ' '
          + gauge.getValue()
          + "\n\n";
    }
    return null;
  }

  protected String formatCounter(String name, Counter counter) {
    if (counter != null) {
      return "# HELP "
          + name
          + " Counter metric\n"
          + "# TYPE "
          + name
          + " counter\n"
          + name
          + " "
          + counter.getCount()
          + "\n\n";
    }
    return null;
  }

  protected String formatHistogram(String name, Histogram histogram) {
    if (histogram != null && histogram.getSnapshot() != null) {
      StringBuilder stringBuilder = new StringBuilder(300);
      String baseName = sanitize(name);
      Snapshot snap = histogram.getSnapshot();
      long count = histogram.getCount();
      stringBuilder
          .append("# HELP ")
          .append(baseName)
          .append(" Histogram metric\n# TYPE ")
          .append(baseName)
          .append(" histogram\n");
      boolean isNanosHistogram = baseName.contains("nanos");
      if (isNanosHistogram) {
        baseName = nanosMetricsNameToSeconds(baseName);
      }
      appendBucket(
          stringBuilder,
          baseName,
          "le=\"0.5\"",
          isNanosHistogram ? nanosToSeconds(snap.getMedian()) : snap.getMean());
      appendBucket(
          stringBuilder,
          baseName,
          "le=\"0.75\"",
          isNanosHistogram ? nanosToSeconds(snap.get75thPercentile()) : snap.get75thPercentile());
      appendBucket(
          stringBuilder,
          baseName,
          "le=\"0.95\"",
          isNanosHistogram ? nanosToSeconds(snap.get95thPercentile()) : snap.get95thPercentile());
      appendBucket(
          stringBuilder,
          baseName,
          "le=\"0.98\"",
          isNanosHistogram ? nanosToSeconds(snap.get98thPercentile()) : snap.get98thPercentile());
      appendBucket(
          stringBuilder,
          baseName,
          "le=\"0.99\"",
          isNanosHistogram ? nanosToSeconds(snap.get99thPercentile()) : snap.get99thPercentile());
      double sum =
          isNanosHistogram ? nanosToSeconds(snap.getMean() * count) : snap.getMean() * count;
      stringBuilder
          .append(baseName)
          .append("_count ")
          .append(count)
          .append('\n')
          .append(baseName)
          .append("_sum ")
          .append(sum)
          .append("\n\n");
      return stringBuilder.toString();
    }
    return null;
  }

  protected String formatMeter(String name, Meter meter) {
    if (meter != null) {
      StringBuilder stringBuilder = new StringBuilder(200);
      String baseName = sanitize(name);
      stringBuilder
          .append("# HELP ")
          .append(baseName)
          .append("_total Meter count\n# TYPE ")
          .append(baseName)
          .append("_total counter\n")
          .append(baseName)
          .append("_total ")
          .append(meter.getCount())
          .append("\n\n# TYPE ")
          .append(baseName)
          .append("_rate gauge\n")
          .append(baseName)
          .append("_rate{interval=\"1m\"} ")
          .append(meter.getOneMinuteRate())
          .append('\n')
          .append(baseName)
          .append("_rate{interval=\"5m\"} ")
          .append(meter.getFiveMinuteRate())
          .append('\n')
          .append(baseName)
          .append("_rate{interval=\"15m\"} ")
          .append(meter.getFifteenMinuteRate())
          .append("\n\n");
      return stringBuilder.toString();
    }
    return null;
  }

  protected String formatTimer(String name, Timer timer) {
    if (timer != null && timer.getSnapshot() != null) {
      StringBuilder stringBuilder = new StringBuilder(300);
      String baseName = sanitize(name);
      Snapshot snap = timer.getSnapshot();
      long count = timer.getCount();
      stringBuilder
          .append("# HELP ")
          .append(baseName)
          .append("_duration_seconds Timer histogram\n# TYPE ")
          .append(baseName)
          .append("_duration_seconds histogram\n");
      appendBucket(
          stringBuilder,
          baseName + "_duration_seconds",
          "le=\"0.5\"",
          nanosToSeconds(snap.getMedian()));
      appendBucket(
          stringBuilder,
          baseName + "_duration_seconds",
          "le=\"0.75\"",
          nanosToSeconds(snap.get75thPercentile()));
      appendBucket(
          stringBuilder,
          baseName + "_duration_seconds",
          "le=\"0.95\"",
          nanosToSeconds(snap.get95thPercentile()));
      appendBucket(
          stringBuilder,
          baseName + "_duration_seconds",
          "le=\"0.98\"",
          nanosToSeconds(snap.get98thPercentile()));
      appendBucket(
          stringBuilder,
          baseName + "_duration_seconds",
          "le=\"0.99\"",
          nanosToSeconds(snap.get99thPercentile()));
      stringBuilder
          .append(baseName)
          .append("_duration_seconds_count ")
          .append(count)
          .append('\n')
          .append(baseName)
          .append("_duration_seconds_sum ")
          .append(nanosToSeconds(snap.getMean() * count))
          .append("\n\n# TYPE ")
          .append(baseName)
          .append("_calls_total counter\n")
          .append(baseName)
          .append("_calls_total ")
          .append(count)
          .append("\n\n");
      return stringBuilder.toString();
    }
    return null;
  }

  protected void appendBucket(
      StringBuilder builder, String baseName, String leLabel, double value) {
    builder
        .append(baseName)
        .append("_bucket{")
        .append(leLabel)
        .append("} ")
        .append(value)
        .append('\n');
  }

  protected double nanosToSeconds(double nanos) {
    return nanos / 1_000_000_000.0;
  }

  protected String sanitize(String name) {
    if (enableSanitizePrometheusMetricsName) {
      return name.replaceAll("[^a-zA-Z0-9_:]", "_").toLowerCase();
    }
    return name;
  }

  protected String nanosMetricsNameToSeconds(String name) {
    return name.replaceAll("_nanos", "_seconds");
  }
}
