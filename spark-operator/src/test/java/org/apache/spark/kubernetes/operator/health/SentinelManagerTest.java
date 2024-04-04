/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.spark.kubernetes.operator.health;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.apache.spark.kubernetes.operator.SparkApplication;
import org.apache.spark.kubernetes.operator.config.SparkOperatorConfManager;
import org.apache.spark.kubernetes.operator.reconciler.SparkReconcilerUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.MockedStatic;

import javax.validation.constraints.NotNull;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.kubernetes.operator.Constants.SENTINEL_LABEL;
import static org.apache.spark.kubernetes.operator.Constants.SPARK_CONF_SENTINEL_DUMMY_FIELD;
import static org.apache.spark.kubernetes.operator.config.SparkOperatorConf.SENTINEL_RESOURCE_RECONCILIATION_DELAY;
import static org.apache.spark.kubernetes.operator.utils.TestUtils.createMockDeployment;
import static org.apache.spark.kubernetes.operator.utils.TestUtils.notTimedOut;
import static org.mockito.Mockito.mockStatic;

@EnableKubernetesMockClient(crud = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SentinelManagerTest {
    public static final String DEFAULT = "default";
    public static final String SPARK_DEMO = "spark-demo";
    @NotNull
    KubernetesClient kubernetesClient;
    @NotNull
    KubernetesMockServer server;
    public static final int SENTINEL_RESOURCE_RECONCILIATION_DELAY_SECONDS = 10;

    @BeforeAll
    static void beforeAll() {
        Map<String, String> overrideValue =
                Collections.singletonMap(SENTINEL_RESOURCE_RECONCILIATION_DELAY.getKey(),
                        Duration.ofSeconds(SENTINEL_RESOURCE_RECONCILIATION_DELAY_SECONDS)
                                .toString());
        SparkOperatorConfManager.INSTANCE.refresh(overrideValue);
    }

    @Test
    @Order(1)
    void testIsSentinelResource() {
        SparkApplication sparkApplication = new SparkApplication();
        var lableMap = sparkApplication.getMetadata().getLabels();
        lableMap.put(SENTINEL_LABEL, "true");
        Set<String> namespaces = new HashSet<>();
        sparkApplication.getMetadata().setNamespace("spark-test");
        namespaces.add("spark-test");
        try (MockedStatic<SparkReconcilerUtils> mockUtils =
                     mockStatic(SparkReconcilerUtils.class)) {
            mockUtils.when(SparkReconcilerUtils::getWatchedNamespaces).thenReturn(namespaces);
            Assertions.assertTrue(SentinelManager.isSentinelResource(sparkApplication));
        }
    }

    @Test
    @Order(3)
    void testHandleSentinelResourceReconciliation() throws InterruptedException {
        // Reduce the SENTINEL_RESOURCE_RECONCILIATION_DELAY time to 0
        SparkOperatorConfManager.INSTANCE.refresh(
                Collections.singletonMap(SENTINEL_RESOURCE_RECONCILIATION_DELAY.getKey(), "10"));

        // Before Spark Reconciler Started
        var mockDeployment = createMockDeployment(DEFAULT);
        kubernetesClient.resource(SparkReconcilerUtils.clone(mockDeployment)).create();
        var crList = kubernetesClient.resources(SparkApplication.class).inNamespace(DEFAULT).list();
        var sparkApplication = crList.getItems().get(0);
        var generation = sparkApplication.getMetadata().getGeneration();
        Assertions.assertEquals(generation, 1L);

        // Spark Reconciler Handle Sentinel Resources at the first time
        SentinelManager sentinelManager = new SentinelManager<SparkApplication>();
        sentinelManager.handleSentinelResourceReconciliation(sparkApplication, kubernetesClient);
        var crList2 =
                kubernetesClient.resources(SparkApplication.class).inNamespace(DEFAULT).list();
        var sparkApplication2 = crList2.getItems().get(0);
        var sparkConf2 = new HashMap<>(sparkApplication2.getSpec().getSparkConf());
        var generation2 = sparkApplication2.getMetadata().getGeneration();

        Assertions.assertEquals(sparkConf2.get(SPARK_CONF_SENTINEL_DUMMY_FIELD), "1");
        Assertions.assertEquals(generation2, 2L);
        SentinelManager.SentinelResourceState state2 =
                (SentinelManager.SentinelResourceState) sentinelManager.getSentinelResources()
                        .get(ResourceID.fromResource(mockDeployment));
        var previousGeneration2 = state2.previousGeneration;
        Assertions.assertTrue(sentinelManager.allSentinelsAreHealthy());
        Assertions.assertEquals(previousGeneration2, 1L);

        Thread.sleep(
                Duration.ofSeconds(SENTINEL_RESOURCE_RECONCILIATION_DELAY_SECONDS * 2).toMillis());
        var crList3 = kubernetesClient.resources(SparkApplication.class).inNamespace(DEFAULT).list()
                .getItems();
        var sparkApplication3 = crList3.get(0);
        var sparkConf3 = new HashMap<>(sparkApplication3.getSpec().getSparkConf());
        // Spark Sentinel Applications' s k8s generation should change
        Assertions.assertNotEquals(sparkApplication3.getMetadata().getGeneration(), generation2);
        // Spark conf SPARK_CONF_SENTINEL_DUMMY_FIELD values should increase
        Assertions.assertNotEquals(sparkConf2.get(SPARK_CONF_SENTINEL_DUMMY_FIELD),
                sparkConf3.get(SPARK_CONF_SENTINEL_DUMMY_FIELD));
        SentinelManager.SentinelResourceState state3 =
                (SentinelManager.SentinelResourceState) sentinelManager.getSentinelResources()
                        .get(ResourceID.fromResource(mockDeployment));
        Assertions.assertEquals(state3.previousGeneration, previousGeneration2);
        // Given the 2 * SENTINEL_RESOURCE_RECONCILIATION_DELAY_SECONDS, the reconcile method is
        // not called to handleSentinelResourceReconciliation to update
        Assertions.assertFalse(sentinelManager.allSentinelsAreHealthy());

        sentinelManager.handleSentinelResourceReconciliation(sparkApplication2, kubernetesClient);
        sentinelManager.handleSentinelResourceReconciliation(sparkApplication2, kubernetesClient);
        boolean isHealthy;
        long currentTimeInMills = System.currentTimeMillis();
        do {
            isHealthy = sentinelManager.allSentinelsAreHealthy();
        } while (!isHealthy && notTimedOut(currentTimeInMills, TimeUnit.MILLISECONDS.convert(
                Duration.ofSeconds(SENTINEL_RESOURCE_RECONCILIATION_DELAY_SECONDS))));
        Assertions.assertTrue(isHealthy);
        kubernetesClient.resources(SparkApplication.class).inNamespace(DEFAULT).delete();
    }

    @Test
    @Order(2)
    void sentinelManagerShouldReportHealthyWhenWatchedNamespaceIsReduced()
            throws InterruptedException {
        Set<String> namespaces = new HashSet<>();
        namespaces.add(DEFAULT);
        namespaces.add(SPARK_DEMO);

        try (MockedStatic<SparkReconcilerUtils> mockUtils =
                     mockStatic(SparkReconcilerUtils.class)) {
            mockUtils.when(SparkReconcilerUtils::getWatchedNamespaces).thenReturn(namespaces);
            SentinelManager sentinelManager = new SentinelManager<SparkApplication>();
            NonNamespaceOperation<SparkApplication, KubernetesResourceList<SparkApplication>,
                    Resource<SparkApplication>> cr1 =
                    kubernetesClient.resources(SparkApplication.class).inNamespace(DEFAULT);
            NonNamespaceOperation<SparkApplication, KubernetesResourceList<SparkApplication>,
                    Resource<SparkApplication>> cr2 =
                    kubernetesClient.resources(SparkApplication.class).inNamespace(SPARK_DEMO);

            var mockDeployment1 = createMockDeployment(DEFAULT);
            var mockDeployment2 = createMockDeployment(SPARK_DEMO);
            cr1.create(mockDeployment1);
            cr2.create(mockDeployment2);

            var crList1 =
                    kubernetesClient.resources(SparkApplication.class).inNamespace(DEFAULT).list();
            var crList2 =
                    kubernetesClient.resources(SparkApplication.class)
                            .inNamespace(SPARK_DEMO).list();
            var sparkApplication1 = crList1.getItems().get(0);
            var sparkApplication2 = crList2.getItems().get(0);
            sentinelManager.handleSentinelResourceReconciliation(sparkApplication1,
                    kubernetesClient);
            sentinelManager.handleSentinelResourceReconciliation(sparkApplication2,
                    kubernetesClient);
            Assertions.assertEquals(sentinelManager.getSentinelResources().size(), 2,
                    "Sentinel Manager should watch on resources in two namespaces");
            Assertions.assertTrue(sentinelManager.allSentinelsAreHealthy(),
                    "Sentinel Manager should report healthy");
            namespaces.remove(SPARK_DEMO);
            Thread.sleep(Duration.ofSeconds(SENTINEL_RESOURCE_RECONCILIATION_DELAY_SECONDS)
                    .toMillis());
            Assertions.assertTrue(sentinelManager.allSentinelsAreHealthy(),
                    "Sentinel Manager should report healthy after one namespace is " +
                            "removed from the watch");
            Assertions.assertEquals(sentinelManager.getSentinelResources().size(), 1,
                    "Sentinel Manager should only watch on one namespace");
        }
    }
}
