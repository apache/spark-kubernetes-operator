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

package org.apache.spark.k8s.operator.metrics.healthcheck;

import static org.apache.spark.k8s.operator.utils.TestUtils.createMockApp;
import static org.apache.spark.k8s.operator.utils.TestUtils.notTimedOut;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.MockedStatic;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.config.SparkOperatorConf;
import org.apache.spark.k8s.operator.config.SparkOperatorConfManager;
import org.apache.spark.k8s.operator.utils.ReconcilerUtils;
import org.apache.spark.k8s.operator.utils.Utils;

@EnableKubernetesMockClient(crud = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@SuppressFBWarnings(
    value = {"UWF_UNWRITTEN_FIELD", "NP_UNWRITTEN_FIELD", "UUF_UNUSED_FIELD"},
    justification = "Unwritten fields are covered by Kubernetes mock client")
class SentinelManagerTest {
  private static final String DEFAULT = "default";
  private static final String SPARK_DEMO = "spark-demo";
  KubernetesClient kubernetesClient;
  KubernetesMockServer server;
  public static final int SENTINEL_RESOURCE_RECONCILIATION_DELAY_SECONDS = 10;

  @BeforeAll
  static void beforeAll() {
    Map<String, String> overrideValue =
        Map.of(
            SparkOperatorConf.SENTINEL_RESOURCE_RECONCILIATION_DELAY.getKey(),
            Duration.ofSeconds(SENTINEL_RESOURCE_RECONCILIATION_DELAY_SECONDS).toString());
    SparkOperatorConfManager.INSTANCE.refresh(overrideValue);
  }

  @Test
  @Order(1)
  void testIsSentinelResource() {
    SparkApplication sparkApplication = new SparkApplication();
    Map<String, String> lableMap = sparkApplication.getMetadata().getLabels();
    lableMap.put(Constants.LABEL_SENTINEL_RESOURCE, "true");
    Set<String> namespaces = new HashSet<>();
    sparkApplication.getMetadata().setNamespace("spark-test");
    namespaces.add("spark-test");
    try (MockedStatic<Utils> mockUtils = mockStatic(Utils.class)) {
      mockUtils.when(Utils::getWatchedNamespaces).thenReturn(namespaces);
      assertTrue(SentinelManager.isSentinelResource(sparkApplication));
    }
  }

  @Test
  @Order(3)
  void testHandleSentinelResourceReconciliation() throws InterruptedException {
    // Reduce the SENTINEL_RESOURCE_RECONCILIATION_DELAY time to 10
    SparkOperatorConfManager.INSTANCE.refresh(
        Map.of(
            SparkOperatorConf.SENTINEL_RESOURCE_RECONCILIATION_DELAY.getKey(), "10"));

    // Before Spark Reconciler Started
    SparkApplication mockApp = createMockApp(DEFAULT);
    kubernetesClient.resource(ReconcilerUtils.clone(mockApp)).create();
    KubernetesResourceList<SparkApplication> crList =
        kubernetesClient.resources(SparkApplication.class).inNamespace(DEFAULT).list();
    SparkApplication sparkApplication = crList.getItems().get(0);
    Long generation = sparkApplication.getMetadata().getGeneration();
    assertEquals(1L, generation);

    // Spark Reconciler Handle Sentinel Resources at the first time
    var sentinelManager = new SentinelManager<SparkApplication>();
    sentinelManager.handleSentinelResourceReconciliation(sparkApplication, kubernetesClient);
    KubernetesResourceList<SparkApplication> crList2 =
        kubernetesClient.resources(SparkApplication.class).inNamespace(DEFAULT).list();
    SparkApplication sparkApplication2 = crList2.getItems().get(0);
    Map<String, String> sparkConf2 = new HashMap<>(sparkApplication2.getSpec().getSparkConf());
    long generation2 = sparkApplication2.getMetadata().getGeneration();

    assertEquals("1", sparkConf2.get(Constants.SENTINEL_RESOURCE_DUMMY_FIELD));
    assertEquals(2L, generation2);
    var state2 = sentinelManager.getSentinelResources().get(ResourceID.fromResource(mockApp));
    long previousGeneration2 = state2.previousGeneration;
    assertTrue(sentinelManager.allSentinelsAreHealthy());
    assertEquals(1L, previousGeneration2);

    Thread.sleep(Duration.ofSeconds(SENTINEL_RESOURCE_RECONCILIATION_DELAY_SECONDS * 2).toMillis());
    List<SparkApplication> crList3 =
        kubernetesClient.resources(SparkApplication.class).inNamespace(DEFAULT).list().getItems();
    SparkApplication sparkApplication3 = crList3.get(0);
    Map<String, String> sparkConf3 = new HashMap<>(sparkApplication3.getSpec().getSparkConf());
    // Spark Sentinel Applications' s k8s generation should change
    assertNotEquals(sparkApplication3.getMetadata().getGeneration(), generation2);
    // Spark conf SPARK_CONF_SENTINEL_DUMMY_FIELD values should increase
    assertNotEquals(
        sparkConf2.get(Constants.SENTINEL_RESOURCE_DUMMY_FIELD),
        sparkConf3.get(Constants.SENTINEL_RESOURCE_DUMMY_FIELD));
    var state3 = sentinelManager.getSentinelResources().get(ResourceID.fromResource(mockApp));
    assertEquals(state3.previousGeneration, previousGeneration2);
    // Given the 2 * SENTINEL_RESOURCE_RECONCILIATION_DELAY_SECONDS, the reconcile method is
    // not called to handleSentinelResourceReconciliation to update
    assertFalse(sentinelManager.allSentinelsAreHealthy());

    sentinelManager.handleSentinelResourceReconciliation(sparkApplication2, kubernetesClient);
    sentinelManager.handleSentinelResourceReconciliation(sparkApplication2, kubernetesClient);
    boolean isHealthy;
    long currentTimeInMills = System.currentTimeMillis();
    do {
      isHealthy = sentinelManager.allSentinelsAreHealthy();
    } while (!isHealthy
        && notTimedOut(
            currentTimeInMills,
            TimeUnit.MILLISECONDS.convert(
                Duration.ofSeconds(SENTINEL_RESOURCE_RECONCILIATION_DELAY_SECONDS))));
    assertTrue(isHealthy);
    kubernetesClient.resources(SparkApplication.class).inNamespace(DEFAULT).delete();
  }

  @Test
  @Order(2)
  void sentinelManagerShouldReportHealthyWhenWatchedNamespaceIsReduced()
      throws InterruptedException {
    Set<String> namespaces = new HashSet<>();
    namespaces.add(DEFAULT);
    namespaces.add(SPARK_DEMO);

    try (var mockUtils = mockStatic(Utils.class)) {
      mockUtils.when(Utils::getWatchedNamespaces).thenReturn(namespaces);
      var sentinelManager = new SentinelManager<SparkApplication>();
      NonNamespaceOperation<
              SparkApplication,
              KubernetesResourceList<SparkApplication>,
              Resource<SparkApplication>>
          cr1 = kubernetesClient.resources(SparkApplication.class).inNamespace(DEFAULT);
      NonNamespaceOperation<
              SparkApplication,
              KubernetesResourceList<SparkApplication>,
              Resource<SparkApplication>>
          cr2 = kubernetesClient.resources(SparkApplication.class).inNamespace(SPARK_DEMO);

      SparkApplication mockApp1 = createMockApp(DEFAULT);
      SparkApplication mockApp2 = createMockApp(SPARK_DEMO);
      cr1.create(mockApp1);
      cr2.create(mockApp2);

      var crList1 = kubernetesClient.resources(SparkApplication.class).inNamespace(DEFAULT).list();
      var crList2 =
          kubernetesClient.resources(SparkApplication.class).inNamespace(SPARK_DEMO).list();
      SparkApplication sparkApplication1 = crList1.getItems().get(0);
      SparkApplication sparkApplication2 = crList2.getItems().get(0);
      sentinelManager.handleSentinelResourceReconciliation(sparkApplication1, kubernetesClient);
      sentinelManager.handleSentinelResourceReconciliation(sparkApplication2, kubernetesClient);
      assertEquals(
          2,
          sentinelManager.getSentinelResources().size(),
          "Sentinel Manager should watch on resources in two namespaces");
      assertTrue(
          sentinelManager.allSentinelsAreHealthy(), "Sentinel Manager should report healthy");
      namespaces.remove(SPARK_DEMO);
      Thread.sleep(Duration.ofSeconds(SENTINEL_RESOURCE_RECONCILIATION_DELAY_SECONDS).toMillis());
      assertTrue(
          sentinelManager.allSentinelsAreHealthy(),
          "Sentinel Manager should report healthy after one namespace is "
              + "removed from the watch");
      assertEquals(
          1,
          sentinelManager.getSentinelResources().size(),
          "Sentinel Manager should only watch on one namespace");
    }
  }
}
