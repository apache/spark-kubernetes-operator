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

package org.apache.spark.k8s.operator.metrics.source;

import static org.apache.spark.k8s.operator.config.SparkOperatorConf.KUBERNETES_CLIENT_METRICS_GROUP_BY_RESPONSE_CODE_GROUP_ENABLED;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import io.fabric8.kubernetes.client.http.*;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.metrics.source.Source;
import org.apache.spark.util.Pair;

/** Interceptor for Kubernetes client to collect metrics. */
@Slf4j
public class KubernetesMetricsInterceptor implements Interceptor, Source {
  final MetricRegistry metricRegistry;
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
  private final Map<Integer, Meter> responseCodeMeters = new ConcurrentHashMap<>();
  private final Map<String, Meter> requestMethodCounter = new ConcurrentHashMap<>();
  private final List<Meter> responseCodeGroupMeters = new ArrayList<>(5);
  private final Meter requestFailedRateMeter;
  private final Meter requestRateMeter;
  private final Meter responseRateMeter;
  private final Map<String, Meter> namespacedResourceMethodMeters = new ConcurrentHashMap<>();

  /** Constructs a new KubernetesMetricsInterceptor, initializing metric registries. */
  public KubernetesMetricsInterceptor() {
    metricRegistry = new MetricRegistry();

    responseLatency =
        metricRegistry.histogram(
            MetricRegistry.name(HTTP_RESPONSE_GROUP, "latency", "nanos").toLowerCase());
    requestFailedRateMeter =
        metricRegistry.meter(MetricRegistry.name(HTTP_REQUEST_FAILED_GROUP).toLowerCase());
    requestRateMeter = metricRegistry.meter(MetricRegistry.name(HTTP_REQUEST_GROUP).toLowerCase());
    responseRateMeter =
        metricRegistry.meter(MetricRegistry.name(HTTP_RESPONSE_GROUP).toLowerCase());

    if (KUBERNETES_CLIENT_METRICS_GROUP_BY_RESPONSE_CODE_GROUP_ENABLED.getValue()) {
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

  /**
   * Called before a request to allow for the manipulation of the request
   *
   * @param builder used to modify the request
   * @param request the current request
   */
  @Override
  public void before(BasicBuilder builder, HttpRequest request, RequestTags tags) {
    updateRequestMetrics(request);
  }

  /**
   * Called after a non-WebSocket HTTP response is received. The body might or might not be already
   * consumed.
   *
   * <p>Should be used to analyze response codes and headers, original response shouldn't be
   * altered.
   *
   * @param request the original request sent to the server.
   * @param response the response received from the server.
   */
  @Override
  public void after(
      HttpRequest request,
      HttpResponse<?> response,
      AsyncBody.Consumer<List<ByteBuffer>> consumer) {
    updateResponseMetrics(response, System.nanoTime());
  }

  /**
   * Called after a websocket failure or by default from a normal request.
   *
   * <p>Failure is determined by HTTP status code and will be invoked in addition to {@link
   * Interceptor#after(HttpRequest, HttpResponse, AsyncBody.Consumer)}
   *
   * @param builder used to modify the request
   * @param response the failed response
   * @return true if the builder should be used to execute a new request
   */
  @Override
  public CompletableFuture<Boolean> afterFailure(
      BasicBuilder builder, HttpResponse<?> response, RequestTags tags) {
    requestFailedRateMeter.mark();
    return CompletableFuture.completedFuture(false);
  }

  /**
   * Called after a connection attempt fails.
   *
   * <p>This method will be invoked on each failed connection attempt.
   *
   * @param request the HTTP request.
   * @param failure the Java exception that caused the failure.
   */
  @Override
  public void afterConnectionFailure(HttpRequest request, Throwable failure) {
    requestFailedRateMeter.mark();
  }

  /**
   * Returns the name of this metrics source.
   *
   * @return The source name.
   */
  @Override
  public String sourceName() {
    return "kubernetes.client";
  }

  /**
   * Returns the MetricRegistry used by this interceptor.
   *
   * @return The MetricRegistry instance.
   */
  @Override
  public MetricRegistry metricRegistry() {
    return this.metricRegistry;
  }

  private void updateRequestMetrics(HttpRequest request) {
    this.requestRateMeter.mark();
    getMeterByRequestMethod(request.method()).mark();
    Optional<Pair<String, String>> resourceNamePairOptional =
        parseNamespaceScopedResource(request.uri().getPath());
    resourceNamePairOptional.ifPresent(
        pair -> {
          getMeterByRequestMethodAndResourceName(pair.getRight(), request.method()).mark();
          getMeterByRequestMethodAndResourceName(
                  pair.getLeft() + "." + pair.getRight(), request.method())
              .mark();
        });
  }

  private void updateResponseMetrics(HttpResponse response, long startTimeNanos) {
    Objects.requireNonNull(response);
    final long latency = System.nanoTime() - startTimeNanos;
    responseRateMeter.mark();
    responseLatency.update(latency);
    getMeterByResponseCode(response.code()).mark();
    if (KUBERNETES_CLIENT_METRICS_GROUP_BY_RESPONSE_CODE_GROUP_ENABLED.getValue()) {
      responseCodeGroupMeters.get(response.code() / 100 - 1).mark();
    }
  }

  private Meter getMeterByRequestMethod(String method) {
    return requestMethodCounter.computeIfAbsent(
        method,
        key -> metricRegistry.meter(MetricRegistry.name(HTTP_REQUEST_GROUP, method).toLowerCase()));
  }

  private Meter getMeterByRequestMethodAndResourceName(String resourceName, String method) {
    String metricsName = MetricRegistry.name(resourceName, method);
    return namespacedResourceMethodMeters.computeIfAbsent(
        metricsName, key -> metricRegistry.meter(metricsName.toLowerCase()));
  }

  private Meter getMeterByResponseCode(int code) {
    return responseCodeMeters.computeIfAbsent(
        code,
        key ->
            metricRegistry.meter(MetricRegistry.name(HTTP_RESPONSE_GROUP, String.valueOf(code))));
  }

  /**
   * Parses the given path to extract namespace-scoped resource information.
   *
   * @param path The request path.
   * @return An Optional containing a Pair of namespace and resource name, or empty if not found.
   */
  public Optional<Pair<String, String>> parseNamespaceScopedResource(String path) {
    if (path.contains(NAMESPACES)) {
      int index = path.indexOf(NAMESPACES) + NAMESPACES.length();
      String namespaceAndResources = path.substring(index + 1);
      String[] parts = namespaceAndResources.split("/");
      return Optional.of(Pair.of(parts[0], parts[1]));
    } else {
      return Optional.empty();
    }
  }
}
