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
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.http.AsyncBody;
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

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.client.KubernetesClientFactory;
import org.apache.spark.k8s.operator.metrics.SummingHistogram;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;

@EnableKubernetesMockClient(crud = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class KubernetesMetricsInterceptorTest {

  private static final String CONFIG_MAP_PATH =
      "/api/v1/namespaces/spark-system/configmaps/spark-job-operator-configuration";

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
      client.resource(configMap).get();

      Map<String, Metric> map = metricsInterceptor.metricRegistry().getMetrics();
      Histogram latency = (Histogram) map.get("http.response.latency.nanos");
      Assertions.assertEquals(1, latency.getCount());
      Assertions.assertTrue(latency.getSnapshot().getMax() >= 50_000_000L);
      Assertions.assertTrue(latency.getSnapshot().getMax() < 30_000_000_000L);
    }
  }

  @Test
  @Order(4)
  void testResponseLatencyOfResentRequest() {
    KubernetesMetricsInterceptor metricsInterceptor = new KubernetesMetricsInterceptor();
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
          .andReturn(HTTP_UNAUTHORIZED, null)
          .once();
      mockServer.expect().get().withPath(CONFIG_MAP_PATH).andReturn(200, configMap).once();
      client.resource(configMap).get();

      Assertions.assertEquals(2, meterCount(metricsInterceptor, "http.response"));
      Assertions.assertEquals(1, meterCount(metricsInterceptor, "http.response.401"));
      Assertions.assertEquals(0, meterCount(metricsInterceptor, "failed"));
      Map<String, Metric> map = metricsInterceptor.metricRegistry().getMetrics();
      Histogram latency = (Histogram) map.get("http.response.latency.nanos");
      Assertions.assertEquals(2, latency.getCount());
      Assertions.assertTrue(latency.getSnapshot().getMax() >= 200_000_000L);
      Assertions.assertTrue(latency.getSnapshot().getMin() < 200_000_000L);
    }
  }

  @Test
  @Order(5)
  void testResponseLatencyOfWebSocketUpgrade() {
    KubernetesMetricsInterceptor metricsInterceptor = new KubernetesMetricsInterceptor();
    List<Interceptor> interceptors = List.of(metricsInterceptor);
    try (KubernetesClient client =
            KubernetesClientFactory.buildKubernetesClient(
                interceptors, kubernetesClient.getConfiguration());
        SharedIndexInformer<ConfigMap> informer =
            client.configMaps().inNamespace("spark-system").inform()) {
      Map<String, Metric> map = metricsInterceptor.metricRegistry().getMetrics();
      await()
          .untilAsserted(
              () -> {
                Assertions.assertEquals(2, meterCount(metricsInterceptor, "http.response"));
                Assertions.assertEquals(1, meterCount(metricsInterceptor, "http.response.101"));
                Assertions.assertEquals(
                    1, ((Histogram) map.get("http.response.latency.nanos")).getCount());
              });
      Assertions.assertTrue(informer.isWatching());
    }
  }

  @Test
  @Order(6)
  void testConsumerUnwrapsDelegate() {
    AsyncBody.Consumer<List<ByteBuffer>> delegate = (value, asyncBody) -> {};
    AsyncBody.Consumer<List<ByteBuffer>> consumer =
        new KubernetesMetricsInterceptor().consumer(delegate, null);
    Assertions.assertSame(delegate, consumer.unwrap(delegate.getClass()));
  }

  private static long meterCount(KubernetesMetricsInterceptor interceptor, String name) {
    Meter meter = interceptor.metricRegistry().getMeters().get(name);
    Assertions.assertNotNull(meter, name);
    return meter.getCount();
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
