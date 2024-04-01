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

import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.apache.spark.kubernetes.operator.SparkApplication;
import org.apache.spark.kubernetes.operator.client.KubernetesClientFactory;
import org.apache.spark.kubernetes.operator.metrics.MetricsSystem;
import org.apache.spark.kubernetes.operator.metrics.MetricsSystemFactory;
import org.apache.spark.kubernetes.operator.spec.ApplicationSpec;
import org.apache.spark.metrics.source.Source;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertThrows;

@EnableKubernetesMockClient(crud = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@SuppressWarnings("PMD")
class KubernetesMetricsInterceptorTest {

    @NotNull
    KubernetesMockServer mockServer;
    @NotNull
    KubernetesClient kubernetesClient;

    @AfterEach
    void cleanUp() {
        mockServer.reset();
    }

    @Test
    @Order(1)
    void testMetricsEnabled() {
        MetricsSystem metricsSystem = MetricsSystemFactory.createMetricsSystem();
        KubernetesClient client = KubernetesClientFactory.buildKubernetesClient(metricsSystem,
                kubernetesClient.getConfiguration());
        var sparkApplication = createSparkApplication();
        var configMap = createConfigMap();
        Source source = metricsSystem.getSources().get(0);
        Map<String, Metric> metrics = new HashMap<>(source.metricRegistry().getMetrics());
        Assertions.assertEquals(9, metrics.size());
        client.resource(sparkApplication).create();
        client.resource(configMap).get();
        Map<String, Metric> metrics2 = new HashMap<>(source.metricRegistry().getMetrics());
        Assertions.assertEquals(17, metrics2.size());
        List<String> expectedMetricsName =
                Arrays.asList("http.response.201", "http.request.post", "sparkapplications.post",
                        "spark-test.sparkapplications.post", "spark-test.sparkapplications.post",
                        "configmaps.get",
                        "spark-system.configmaps.get", "2xx", "4xx");
        expectedMetricsName.stream().forEach(name -> {
            Meter metric = (Meter) metrics2.get(name);
            Assertions.assertEquals(metric.getCount(), 1);
        });
        client.resource(sparkApplication).delete();
    }

    @Test
    @Order(2)
    void testWhenKubernetesServerNotWorking() {
        MetricsSystem metricsSystem = MetricsSystemFactory.createMetricsSystem();
        KubernetesClient client = KubernetesClientFactory.buildKubernetesClient(metricsSystem,
                kubernetesClient.getConfiguration());
        int retry = client.getConfiguration().getRequestRetryBackoffLimit();
        mockServer.shutdown();
        var sparkApplication = createSparkApplication();
        assertThrows(Exception.class, () -> {
            client.resource(sparkApplication).create();
        });
        Source source = metricsSystem.getSources().get(0);
        Map<String, Metric> map = source.metricRegistry().getMetrics();
        Assertions.assertEquals(21, map.size());
        Meter metric = (Meter) map.get("failed");
        Assertions.assertEquals(metric.getCount(), retry + 1);
    }

    private static SparkApplication createSparkApplication() {
        ObjectMeta meta = new ObjectMeta();
        meta.setName("sample-spark-application");
        meta.setNamespace("spark-test");
        var sparkApplication = new SparkApplication();
        sparkApplication.setMetadata(meta);
        ApplicationSpec applicationSpec = new ApplicationSpec();
        applicationSpec.setMainClass("org.apache.spark.examples.SparkPi");
        applicationSpec.setJars("local:///opt/spark/examples/jars/spark-examples.jar");
        applicationSpec.setSparkConf(Map.of(
                "spark.executor.instances", "5",
                "spark.kubernetes.container.image", "spark",
                "spark.kubernetes.namespace", "spark-test",
                "spark.kubernetes.authenticate.driver.serviceAccountName", "spark"
        ));
        sparkApplication.setSpec(applicationSpec);
        return sparkApplication;
    }

    private static ConfigMap createConfigMap() {
        ObjectMeta meta = new ObjectMeta();
        meta.setName("spark-job-operator-configuration");
        meta.setNamespace("spark-system");
        var configMap = new ConfigMap();
        configMap.setMetadata(meta);
        return configMap;
    }
}
