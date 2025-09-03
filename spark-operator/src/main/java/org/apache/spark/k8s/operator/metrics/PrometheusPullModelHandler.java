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
        SparkOperatorConf.PROMETHEUS_TEXT_BASED_FORMAT_ENABLED.getValue();
    this.enableSanitizePrometheusMetricsName =
        SparkOperatorConf.SANITIZE_PROMETHEUS_METRICS_NAME_ENABLED.getValue();
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
    if (enablePrometheusTextBasedFormat) {
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
          + formattedName
          + ' '
          + gauge.getValue()
          + "\n\n";
    }
    return null;
  }

  protected String formatCounter(String name, Counter counter) {
    if (counter != null) {
      String formattedName = sanitize(name);
      return "# HELP "
          + formattedName
          + " Counter metric\n"
          + "# TYPE "
          + formattedName
          + " counter\n"
          + formattedName
          + " "
          + counter.getCount()
          + "\n\n";
    }
    return null;
  }

  protected String formatHistogram(String name, Histogram histogram) {
    if (histogram != null && histogram.getSnapshot() != null) {
      String baseName = sanitize(name);
      Snapshot snap = histogram.getSnapshot();
      long count = histogram.getCount();
      boolean isNanosHistogram = baseName.contains("nanos");
      if (isNanosHistogram) {
        baseName = nanosMetricsNameToSeconds(baseName);
      }
      double sum =
          isNanosHistogram ? nanosToSeconds(snap.getMean() * count) : snap.getMean() * count;
      return "# HELP "
          + baseName
          + " Histogram metric\n# TYPE "
          + baseName
          + " summary\n"
          + baseName
          + "{quantile=\"0.5\"} "
          + (isNanosHistogram ? nanosToSeconds(snap.getMedian()) : snap.getMean())
          + "\n"
          + baseName
          + "{quantile=\"0.75\"} "
          + (isNanosHistogram ? nanosToSeconds(snap.get75thPercentile()) : snap.get75thPercentile())
          + "\n"
          + baseName
          + "{quantile=\"0.95\"} "
          + (isNanosHistogram ? nanosToSeconds(snap.get95thPercentile()) : snap.get95thPercentile())
          + "\n"
          + baseName
          + "{quantile=\"0.98\"} "
          + (isNanosHistogram ? nanosToSeconds(snap.get98thPercentile()) : snap.get98thPercentile())
          + "\n"
          + baseName
          + "{quantile=\"0.99\"} "
          + (isNanosHistogram ? nanosToSeconds(snap.get99thPercentile()) : snap.get99thPercentile())
          + "\n"
          + baseName
          + "{quantile=\"0.999\"} "
          + (isNanosHistogram
              ? nanosToSeconds(snap.get999thPercentile())
              : snap.get99thPercentile())
          + "\n"
          + baseName
          + "_count "
          + count
          + "\n"
          + baseName
          + "_sum "
          + sum
          + "\n\n";
    }
    return null;
  }

  protected String formatMeter(String name, Meter meter) {
    if (meter != null) {
      String baseName = sanitize(name);
      return "# HELP "
          + baseName
          + "_total Meter count\n# TYPE "
          + baseName
          + "_total counter\n"
          + baseName
          + "_total "
          + meter.getCount()
          + "\n\n# TYPE "
          + baseName
          + "_rate gauge\n"
          + baseName
          + "_m1_rate "
          + meter.getOneMinuteRate()
          + "\n"
          + baseName
          + "_m5_rate "
          + meter.getFiveMinuteRate()
          + "\n"
          + baseName
          + "_m15_rate "
          + meter.getFifteenMinuteRate()
          + "\n\n";
    }
    return null;
  }

  protected String formatTimer(String name, Timer timer) {
    if (timer != null && timer.getSnapshot() != null) {
      String baseName = sanitize(name);
      Snapshot snap = timer.getSnapshot();
      long count = timer.getCount();
      return "# HELP "
          + baseName
          + "_duration_seconds Timer summary\n# TYPE "
          + baseName
          + "_duration_seconds summary\n"
          + "\n"
          + baseName
          + "_duration_seconds"
          + "{quantile=\"0.5\"} "
          + nanosToSeconds(snap.getMedian())
          + "\n"
          + baseName
          + "_duration_seconds"
          + "{quantile=\"0.75\"} "
          + nanosToSeconds(snap.get75thPercentile())
          + "\n"
          + baseName
          + "_duration_seconds"
          + "{quantile=\"0.95\"} "
          + nanosToSeconds(snap.get95thPercentile())
          + "\n"
          + baseName
          + "_duration_seconds"
          + "{quantile=\"0.98\"} "
          + nanosToSeconds(snap.get98thPercentile())
          + "\n"
          + baseName
          + "_duration_seconds"
          + "{quantile=\"0.99\"} "
          + nanosToSeconds(snap.get99thPercentile())
          + "\n"
          + baseName
          + "_duration_seconds"
          + "{quantile=\"0.999\"} "
          + nanosToSeconds(snap.get999thPercentile())
          + "\n"
          + baseName
          + "_duration_seconds_count "
          + count
          + "\n"
          + baseName
          + "_duration_seconds_sum "
          + nanosToSeconds(snap.getMean() * count)
          + "\n\n# TYPE "
          + baseName
          + " gauge\n"
          + baseName
          + "_count "
          + count
          + "\n"
          + baseName
          + "_m1_rate "
          + timer.getOneMinuteRate()
          + "\n"
          + baseName
          + "_m5_rate "
          + timer.getFiveMinuteRate()
          + "\n"
          + baseName
          + "_m15_rate "
          + timer.getFifteenMinuteRate()
          + "\n\n";
    }
    return null;
  }

  protected double nanosToSeconds(double nanos) {
    return nanos / 1_000_000_000.0;
  }

  protected String sanitize(String name) {
    if (enableSanitizePrometheusMetricsName) {
      return name.replaceAll("[^a-zA-Z0-9_]", "_").toLowerCase();
    }
    return name;
  }

  protected String nanosMetricsNameToSeconds(String name) {
    return name.replaceAll("_nanos", "_seconds");
  }
}
