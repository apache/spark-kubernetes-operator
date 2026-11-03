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

import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.apache.spark.k8s.operator.utils.TestUtils.histogram;
import static org.apache.spark.k8s.operator.utils.TestUtils.meterCount;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.StatusBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.http.AsyncBody;
import io.fabric8.kubernetes.client.http.HttpLoggingInterceptor;
import io.fabric8.kubernetes.client.http.HttpResponse;
import io.fabric8.kubernetes.client.http.Interceptor;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.Logger;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.client.KubernetesClientFactory;
import org.apache.spark.k8s.operator.metrics.SummingHistogram;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.util.Pair;

@EnableKubernetesMockClient(crud = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KubernetesMetricsInterceptorTest {

  private static final String CONFIG_MAP_PATH =
      "/api/v1/namespaces/spark-system/configmaps/spark-job-operator-configuration";
  private static final String LATENCY = "http.response.latency.nanos";

  private KubernetesMockServer mockServer;
  private KubernetesClient kubernetesClient;

  @AfterEach
  void cleanUp() {
    mockServer.reset();
  }

  @Test
  @Order(1)
  void testMetricsEnabled() {
    KubernetesMetricsInterceptor metricsInterceptor = new KubernetesMetricsInterceptor();
    List<Interceptor> interceptors = List.of(metricsInterceptor);
    try (KubernetesClient client =
        KubernetesClientFactory.buildKubernetesClient(
            interceptors, kubernetesClient.getConfiguration())) {
      SparkApplication sparkApplication = createSparkApplication();
      ConfigMap configMap = createConfigMap();

      Map<String, Metric> metrics = new HashMap<>(metricsInterceptor.metricRegistry().getMetrics());
      Assertions.assertEquals(9, metrics.size());
      Assertions.assertInstanceOf(
          SummingHistogram.class, metrics.get("http.response.latency.nanos"));
      client.resource(sparkApplication).create();
      client.resource(configMap).get();
      Map<String, Metric> metrics2 =
          new HashMap<>(metricsInterceptor.metricRegistry().getMetrics());
      Assertions.assertEquals(17, metrics2.size());
      List<String> expectedMetricsName =
          Arrays.asList(
              "http.response.201",
              "http.request.post",
              "sparkapplications.post",
              "spark-test.sparkapplications.post",
              "spark-test.sparkapplications.post",
              "configmaps.get",
              "spark-system.configmaps.get",
              "2xx",
              "4xx");
      expectedMetricsName.stream()
          .forEach(
              name -> {
                Meter metric = (Meter) metrics2.get(name);
                Assertions.assertEquals(1, metric.getCount());
              });
      Assertions.assertEquals(2, ((Meter) metrics2.get("http.request")).getCount());
      client.resource(sparkApplication).delete();
    }
  }

  @Test
  @Order(2)
  void testWhenKubernetesServerNotWorking() {
    KubernetesMetricsInterceptor metricsInterceptor = new KubernetesMetricsInterceptor();
    List<Interceptor> interceptors = List.of(metricsInterceptor);
    try (KubernetesClient client =
        KubernetesClientFactory.buildKubernetesClient(
            interceptors, kubernetesClient.getConfiguration())) {
      int retry = client.getConfiguration().getRequestRetryBackoffLimit();
      mockServer.shutdown();
      SparkApplication sparkApplication = createSparkApplication();
      assertThrows(
          Exception.class,
          () -> {
            client.resource(sparkApplication).create();
          });

      Map<String, Metric> map = metricsInterceptor.metricRegistry().getMetrics();
      Assertions.assertEquals(12, map.size());
      Meter metric = (Meter) map.get("failed");
      Assertions.assertEquals(metric.getCount(), retry);
      Assertions.assertEquals(((Meter) map.get("http.request")).getCount(), retry + 1);
    }
  }

  @Test
  @Order(3)
  void testParseNamespaceScopedResource() {
    KubernetesMetricsInterceptor metricsInterceptor = new KubernetesMetricsInterceptor();
    Assertions.assertEquals(
        Optional.of(Pair.of("spark-system", "configmaps")),
        metricsInterceptor.parseNamespaceScopedResource(
            "/api/v1/namespaces/spark-system/configmaps/spark-job-operator-configuration"));
    Assertions.assertEquals(
        Optional.of(Pair.of("spark-test", "sparkapplications")),
        metricsInterceptor.parseNamespaceScopedResource(
            "/apis/spark.apache.org/v1/namespaces/spark-test/sparkapplications"));
    for (String path :
        List.of(
            "/api/v1/namespaces",
            "/api/v1/namespaces/spark-system",
            "/apis/rbac.authorization.k8s.io/v1/clusterroles/namespaces-reader")) {
      Assertions.assertEquals(
          Optional.empty(), metricsInterceptor.parseNamespaceScopedResource(path), path);
    }
  }

  @Test
  @Order(4)
  void testResponseCodeGroups() {
    KubernetesMetricsInterceptor metricsInterceptor = new KubernetesMetricsInterceptor();
    for (int code : new int[] {99, 100, 599, 600, 999}) {
      HttpResponse<?> response = mock(HttpResponse.class);
      when(response.code()).thenReturn(code);
      metricsInterceptor.after(null, response, null);
    }
    Map<String, Meter> meters = metricsInterceptor.metricRegistry().getMeters();
    Assertions.assertEquals(5, meters.get("http.response").getCount());
    Assertions.assertEquals(1, meters.get("http.response.999").getCount());
    Assertions.assertEquals(1, meters.get("1xx").getCount());
    Assertions.assertEquals(0, meters.get("2xx").getCount());
    Assertions.assertEquals(0, meters.get("3xx").getCount());
    Assertions.assertEquals(0, meters.get("4xx").getCount());
    Assertions.assertEquals(1, meters.get("5xx").getCount());
  }

  @Test
  @Order(5)
  void testResponseLatency() {
    KubernetesMetricsInterceptor metricsInterceptor = new KubernetesMetricsInterceptor();
    List<Interceptor> interceptors = List.of(metricsInterceptor);
    try (KubernetesClient client =
        KubernetesClientFactory.buildKubernetesClient(
            interceptors, kubernetesClient.getConfiguration())) {
      ConfigMap configMap = createConfigMap();
      mockServer
          .expect()
          .get()
          .delay(50)
          .withPath(CONFIG_MAP_PATH)
          .andReturn(200, configMap)
          .once();
      long startTime = System.nanoTime();
      client.resource(configMap).get();
      long elapsed = System.nanoTime() - startTime;

      Histogram latency = histogram(metricsInterceptor.metricRegistry(), LATENCY);
      Assertions.assertEquals(1, latency.getCount());
      Snapshot snapshot = latency.getSnapshot();
      Assertions.assertTrue(snapshot.getMax() >= 50_000_000L, () -> "max: " + snapshot.getMax());
      Assertions.assertTrue(
          snapshot.getMax() <= elapsed,
          () -> "max: " + snapshot.getMax() + ", elapsed: " + elapsed);
    }
  }

  @Test
  @Order(6)
  void testResponseLatencyOfResentRequest() {
    KubernetesMetricsInterceptor metricsInterceptor = new KubernetesMetricsInterceptor();
    MetricRegistry metrics = metricsInterceptor.metricRegistry();
    List<Interceptor> interceptors = List.of(metricsInterceptor);
    Config config =
        new ConfigBuilder(kubernetesClient.getConfiguration())
            .withOauthTokenProvider(() -> "token")
            .build();
    try (KubernetesClient client =
        KubernetesClientFactory.buildKubernetesClient(interceptors, config)) {
      ConfigMap configMap = createConfigMap();
      mockServer
          .expect()
          .get()
          .delay(200)
          .withPath(CONFIG_MAP_PATH)
          .andReturn(HTTP_UNAUTHORIZED, new StatusBuilder().withCode(HTTP_UNAUTHORIZED).build())
          .once();
      mockServer.expect().get().withPath(CONFIG_MAP_PATH).andReturn(200, configMap).once();
      long startTime = System.nanoTime();
      client.resource(configMap).get();
      long elapsed = System.nanoTime() - startTime;

      Assertions.assertEquals(2, meterCount(metrics, "http.response"));
      Assertions.assertEquals(1, meterCount(metrics, "http.response.401"));
      Assertions.assertEquals(0, meterCount(metrics, "failed"));
      Histogram latency = histogram(metrics, LATENCY);
      Assertions.assertEquals(2, latency.getCount());
      Snapshot snapshot = latency.getSnapshot();
      Assertions.assertTrue(snapshot.getMax() >= 200_000_000L, () -> "max: " + snapshot.getMax());
      Assertions.assertTrue(snapshot.getMin() < 200_000_000L, () -> "min: " + snapshot.getMin());
      long sum = Assertions.assertInstanceOf(SummingHistogram.class, latency).getSum();
      Assertions.assertTrue(sum >= 200_000_000L, () -> "sum: " + sum);
      Assertions.assertTrue(sum <= elapsed, () -> "sum: " + sum + ", elapsed: " + elapsed);
    }
  }

  @Test
  @Order(7)
  void testResponseLatencyOfWebSocketUpgrade() {
    KubernetesMetricsInterceptor metricsInterceptor = new KubernetesMetricsInterceptor();
    MetricRegistry metrics = metricsInterceptor.metricRegistry();
    List<Interceptor> interceptors = List.of(metricsInterceptor);
    try (KubernetesClient client =
            KubernetesClientFactory.buildKubernetesClient(
                interceptors, kubernetesClient.getConfiguration());
        SharedIndexInformer<ConfigMap> informer =
            client.configMaps().inNamespace("spark-system").inform()) {
      await()
          .pollDelay(Duration.ZERO)
          .untilAsserted(
              () -> {
                Assertions.assertEquals(2, meterCount(metrics, "http.response"));
                Assertions.assertEquals(1, meterCount(metrics, "http.response.101"));
                Assertions.assertEquals(1, histogram(metrics, LATENCY).getCount());
              });
      Assertions.assertTrue(informer.isWatching());
    }
  }

  @Test
  @Order(8)
  void testConsumerUnwrapsDelegate() {
    AsyncBody.Consumer<List<ByteBuffer>> delegate = (value, asyncBody) -> {};
    AsyncBody.Consumer<List<ByteBuffer>> consumer =
        new KubernetesMetricsInterceptor().consumer(delegate, null);
    Assertions.assertSame(delegate, consumer.unwrap(delegate.getClass()));
  }

  @Test
  @Order(9)
  void testResponseLatencyWithHttpLogging() {
    Logger logger = mock(Logger.class);
    when(logger.isTraceEnabled()).thenReturn(true);
    KubernetesMetricsInterceptor metricsInterceptor = new KubernetesMetricsInterceptor();
    // fabric8 installs HttpLoggingInterceptor first at the TRACE level, so it is wrapped inside.
    List<Interceptor> interceptors =
        List.of(new HttpLoggingInterceptor(logger), metricsInterceptor);
    try (KubernetesClient client =
        KubernetesClientFactory.buildKubernetesClient(
            interceptors, kubernetesClient.getConfiguration())) {
      ConfigMap configMap = createConfigMap();
      mockServer.expect().get().withPath(CONFIG_MAP_PATH).andReturn(200, configMap).once();
      client.resource(configMap).get();

      Assertions.assertEquals(
          1, histogram(metricsInterceptor.metricRegistry(), LATENCY).getCount());
      // The response body is logged only if HttpLoggingInterceptor finds its own consumer.
      String body = "\"kind\":\"ConfigMap\"";
      verify(logger, timeout(10_000)).trace(argThat((String m) -> m != null && m.contains(body)));
    }
  }

  @Test
  @Order(10)
  void testResponseLatencyWithoutTimedConsumer() {
    KubernetesMetricsInterceptor metricsInterceptor = new KubernetesMetricsInterceptor();
    MetricRegistry metrics = metricsInterceptor.metricRegistry();
    AsyncBody.Consumer<List<ByteBuffer>> consumer = (value, asyncBody) -> {};
    HttpResponse<?> response = mock(HttpResponse.class);
    when(response.code()).thenReturn(200);
    metricsInterceptor.after(null, response, consumer);
    metricsInterceptor.after(null, response, consumer);
    Assertions.assertEquals(2, meterCount(metrics, "http.response"));
    Assertions.assertEquals(0, histogram(metrics, LATENCY).getCount());
  }

  private static SparkApplication createSparkApplication() {
    ObjectMeta meta = new ObjectMeta();
    meta.setName("sample-spark-application");
    meta.setNamespace("spark-test");
    SparkApplication sparkApplication = new SparkApplication();
    sparkApplication.setMetadata(meta);
    ApplicationSpec applicationSpec = new ApplicationSpec();
    applicationSpec.setMainClass("org.apache.spark.examples.SparkPi");
    applicationSpec.setJars("local:///opt/spark/examples/jars/spark-examples.jar");
    applicationSpec.setSparkConf(
        Map.of(
            "spark.executor.instances", "5",
            "spark.kubernetes.container.image", "spark",
            "spark.kubernetes.namespace", "spark-test",
            "spark.kubernetes.authenticate.driver.serviceAccountName", "spark"));
    sparkApplication.setSpec(applicationSpec);
    return sparkApplication;
  }

  private static ConfigMap createConfigMap() {
    ObjectMeta meta = new ObjectMeta();
    meta.setName("spark-job-operator-configuration");
    meta.setNamespace("spark-system");
    ConfigMap configMap = new ConfigMap();
    configMap.setMetadata(meta);
    return configMap;
  }
}
