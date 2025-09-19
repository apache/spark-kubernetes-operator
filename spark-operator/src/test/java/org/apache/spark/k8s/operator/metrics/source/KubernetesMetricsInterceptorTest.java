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

import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.http.Interceptor;
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
import org.apache.spark.k8s.operator.spec.ApplicationSpec;

@EnableKubernetesMockClient(crud = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@SuppressFBWarnings(
    value = {"UWF_UNWRITTEN_FIELD", "NP_UNWRITTEN_FIELD"},
    justification = "Unwritten fields are covered by Kubernetes mock client")
class KubernetesMetricsInterceptorTest {

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
    List<Interceptor> interceptors = Collections.singletonList(metricsInterceptor);
    try (KubernetesClient client =
        KubernetesClientFactory.buildKubernetesClient(
            interceptors, kubernetesClient.getConfiguration())) {
      SparkApplication sparkApplication = createSparkApplication();
      ConfigMap configMap = createConfigMap();

      Map<String, Metric> metrics = new HashMap<>(metricsInterceptor.metricRegistry().getMetrics());
      Assertions.assertEquals(9, metrics.size());
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
                Assertions.assertEquals(metric.getCount(), 1);
              });
      Assertions.assertEquals(((Meter) metrics2.get("http.request")).getCount(), 2);
      client.resource(sparkApplication).delete();
    }
  }

  @Test
  @Order(2)
  void testWhenKubernetesServerNotWorking() {
    KubernetesMetricsInterceptor metricsInterceptor = new KubernetesMetricsInterceptor();
    List<Interceptor> interceptors = Collections.singletonList(metricsInterceptor);
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
