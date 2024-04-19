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

package org.apache.spark.kubernetes.operator.metrics.source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import org.apache.spark.metrics.source.Source;

import static org.apache.spark.kubernetes.operator.config.SparkOperatorConf.KubernetesClientMetricsGroupByResponseCodeGroupEnabled;

@Slf4j
public class KubernetesMetricsInterceptor implements Interceptor, Source {
  MetricRegistry metricRegistry;
  public static final String NAMESPACES = "namespaces";
  public static final String HTTP_REQUEST_GROUP = "http.request";
  public static final String HTTP_REQUEST_FAILED_GROUP = "failed";
  public static final String HTTP_RESPONSE_GROUP = "http.response";
  public static final String HTTP_RESPONSE_1XX = "1xx";
  public static final String HTTP_RESPONSE_2XX = "2xx";
  public static final String HTTP_RESPONSE_3XX = "3xx";
  public static final String HTTP_RESPONSE_4XX = "4xx";
  public static final String HTTP_RESPONSE_5XX = "5xx";
  private final Histogram responseLatency;
  private final Map<Integer, Meter> responseCodeMeters =
      new ConcurrentHashMap<>();
  private final Map<String, Meter> requestMethodCounter = new ConcurrentHashMap<>();
  private final List<Meter> responseCodeGroupMeters = new ArrayList<>(5);
  private final Meter requestFailedRateMeter;
  private final Meter requestRateMeter;
  private final Meter responseRateMeter;
  private final Map<String, Meter> namespacedResourceMethodMeters = new ConcurrentHashMap<>();

  public KubernetesMetricsInterceptor() {
    metricRegistry = new MetricRegistry();

    responseLatency = metricRegistry.histogram(
        MetricRegistry.name(HTTP_RESPONSE_GROUP, "latency", "nanos").toLowerCase());
    requestFailedRateMeter =
        metricRegistry.meter(MetricRegistry.name(HTTP_REQUEST_FAILED_GROUP).toLowerCase());
    requestRateMeter =
        metricRegistry.meter(MetricRegistry.name(HTTP_REQUEST_GROUP).toLowerCase());
    responseRateMeter =
        metricRegistry.meter(MetricRegistry.name(HTTP_RESPONSE_GROUP).toLowerCase());

    if (KubernetesClientMetricsGroupByResponseCodeGroupEnabled.getValue()) {
      responseCodeGroupMeters.add(
          metricRegistry.meter(MetricRegistry.name(HTTP_RESPONSE_1XX).toLowerCase()));
      responseCodeGroupMeters.add(
          metricRegistry.meter(MetricRegistry.name(HTTP_RESPONSE_2XX).toLowerCase()));
      responseCodeGroupMeters.add(
          metricRegistry.meter(MetricRegistry.name(HTTP_RESPONSE_3XX).toLowerCase()));
      responseCodeGroupMeters.add(
          metricRegistry.meter(MetricRegistry.name(HTTP_RESPONSE_4XX).toLowerCase()));
      responseCodeGroupMeters.add(
          metricRegistry.meter(MetricRegistry.name(HTTP_RESPONSE_5XX).toLowerCase()));
    }
  }

  @NotNull
  @Override
  public Response intercept(@NotNull Chain chain) throws IOException {
    Request request = chain.request();
    updateRequestMetrics(request);
    Response response = null;
    final long startTime = System.nanoTime();
    try {
      response = chain.proceed(request);
      return response;
    } finally {
      updateResponseMetrics(response, startTime);
    }
  }

  @Override
  public String sourceName() {
    return "kubernetes.client";
  }

  @Override
  public MetricRegistry metricRegistry() {
    return this.metricRegistry;
  }

  private void updateRequestMetrics(Request request) {
    this.requestRateMeter.mark();
    getMeterByRequestMethod(request.method()).mark();
    Optional<Pair<String, String>> resourceNamePairOptional =
        parseNamespaceScopedResource(request.url().uri().getPath());
    resourceNamePairOptional.ifPresent(pair -> {
          getMeterByRequestMethodAndResourceName(
              pair.getValue(), request.method()).mark();
          getMeterByRequestMethodAndResourceName(
              pair.getKey() + "." + pair.getValue(),
              request.method()).mark();
        }
    );
  }

  private void updateResponseMetrics(Response response, long startTimeNanos) {
    final long latency = System.nanoTime() - startTimeNanos;
    if (response != null) {
      this.responseRateMeter.mark();
      this.responseLatency.update(latency);
      getMeterByResponseCode(response.code()).mark();
      if (KubernetesClientMetricsGroupByResponseCodeGroupEnabled.getValue()) {
        responseCodeGroupMeters.get(response.code() / 100 - 1).mark();
      }
    } else {
      this.requestFailedRateMeter.mark();
    }
  }

  private Meter getMeterByRequestMethod(String method) {
    return requestMethodCounter.computeIfAbsent(
        method,
        key ->
            metricRegistry.meter(
                MetricRegistry.name(HTTP_REQUEST_GROUP, method).toLowerCase()));
  }

  private Meter getMeterByRequestMethodAndResourceName(String resourceName, String method) {
    String metricsName = MetricRegistry.name(resourceName, method);
    return namespacedResourceMethodMeters.computeIfAbsent(
        metricsName,
        key ->
            metricRegistry.meter(metricsName.toLowerCase()));
  }

  private Meter getMeterByResponseCode(int code) {
    return responseCodeMeters.computeIfAbsent(code,
        key -> metricRegistry.meter(
            MetricRegistry.name(HTTP_RESPONSE_GROUP, String.valueOf(code))));
  }

  public Optional<Pair<String, String>> parseNamespaceScopedResource(String path) {
    if (path.contains(NAMESPACES)) {
      var index = path.indexOf(NAMESPACES) + NAMESPACES.length();
      String namespaceAndResources = path.substring(index + 1);
      String[] parts = namespaceAndResources.split("/");
      return Optional.of(Pair.of(parts[0], parts[1]));
    } else {
      return Optional.empty();
    }
  }
}
