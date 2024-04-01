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

package org.apache.spark.kubernetes.operator.probe;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.RuntimeInfo;
import io.javaoperatorsdk.operator.api.config.ResourceConfiguration;
import io.javaoperatorsdk.operator.health.InformerHealthIndicator;
import io.javaoperatorsdk.operator.health.InformerWrappingEventSourceHealthIndicator;
import io.javaoperatorsdk.operator.health.Status;
import org.apache.spark.kubernetes.operator.health.SentinelManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@EnableKubernetesMockClient(crud = true)
class HealthProbeTest {
    public static Operator operator;
    public static Operator sparkConfMonitor;
    public static List<Operator> operators;
    @NotNull
    KubernetesClient kubernetesClient;
    private AtomicBoolean isRunning;
    private AtomicBoolean isRunning2;
    private Map<String, Map<String, InformerWrappingEventSourceHealthIndicator>>
            unhealthyEventSources = new HashMap<>();
    private Map<String, Map<String, InformerWrappingEventSourceHealthIndicator>>
            unhealthyEventSources2 = new HashMap<>();

    @BeforeAll
    public static void beforeAll() {
        operator = mock(Operator.class);
        sparkConfMonitor = mock(Operator.class);
        operators = Arrays.asList(operator, sparkConfMonitor);
    }

    @BeforeEach
    public void beforeEach() {
        isRunning = new AtomicBoolean(false);
        isRunning2 = new AtomicBoolean(false);
        var runtimeInfo =
                new RuntimeInfo(new Operator(
                        overrider -> overrider.withKubernetesClient(kubernetesClient))) {
                    @Override
                    public boolean isStarted() {
                        return isRunning.get();
                    }

                    @Override
                    public Map<String, Map<String, InformerWrappingEventSourceHealthIndicator>>
                    unhealthyInformerWrappingEventSourceHealthIndicator() {
                        return unhealthyEventSources;
                    }
                };

        var runtimeInfo2 =
                new RuntimeInfo(new Operator(
                        overrider -> overrider.withKubernetesClient(kubernetesClient))) {
                    @Override
                    public boolean isStarted() {
                        return isRunning2.get();
                    }

                    @Override
                    public Map<String, Map<String, InformerWrappingEventSourceHealthIndicator>>
                    unhealthyInformerWrappingEventSourceHealthIndicator() {
                        return unhealthyEventSources2;
                    }
                };

        when(operator.getRuntimeInfo()).thenReturn(runtimeInfo);
        when(sparkConfMonitor.getRuntimeInfo()).thenReturn(runtimeInfo2);
    }

    @Test
    void testHealthProbeWithInformerHealthWithMultiOperators() {
        var healthyProbe = new HealthProbe(operators);
        isRunning.set(true);
        assertFalse(healthyProbe.isHealthy(),
                "Healthy Probe should fail when the spark conf monitor operator is not running");
        isRunning2.set(true);
        assertTrue(healthyProbe.isHealthy(),
                "Healthy Probe should pass when both operators are running");

        unhealthyEventSources2.put(
                "c1", Map.of("e1", informerHealthIndicator(Map.of("i1", Status.UNHEALTHY))));
        assertFalse(healthyProbe.isHealthy(),
                "Healthy Probe should fail when monitor's informer health is not healthy");
        unhealthyEventSources2.clear();
        assertTrue(healthyProbe.isHealthy(), "Healthy Probe should pass");
    }

    @Test
    void testHealthProbeWithInformerHealthWithSingleOperator() {
        var healthyProbe = new HealthProbe(Arrays.asList(operator));
        assertFalse(healthyProbe.isHealthy(),
                "Health Probe should fail when operator is not running");
        isRunning.set(true);
        unhealthyEventSources.put(
                "c1", Map.of("e1", informerHealthIndicator(Map.of("i1", Status.UNHEALTHY))));
        assertFalse(healthyProbe.isHealthy(),
                "Healthy Probe should fail when informer health is not healthy");
        unhealthyEventSources.clear();
        assertTrue(healthyProbe.isHealthy(), "Healthy Probe should pass");
    }

    @Test
    void testHealthProbeWithSentinelHealthWithMultiOperators() {
        var healthyProbe = new HealthProbe(operators);
        SentinelManager sentinelManager = mock(SentinelManager.class);
        healthyProbe.registerSentinelResourceManager(sentinelManager);
        isRunning.set(true);
        isRunning2.set(true);
        when(sentinelManager.allSentinelsAreHealthy()).thenReturn(false);
        assertFalse(healthyProbe.isHealthy(),
                "Healthy Probe should fail when sentinels report failures");

        when(sentinelManager.allSentinelsAreHealthy()).thenReturn(true);
        assertTrue(healthyProbe.isHealthy(), "Healthy Probe should pass");
    }

    private static InformerWrappingEventSourceHealthIndicator informerHealthIndicator(
            Map<String, Status> informerStatuses) {
        Map<String, InformerHealthIndicator> informers = new HashMap<>();
        informerStatuses.forEach(
                (n, s) ->
                        informers.put(
                                n,
                                new InformerHealthIndicator() {
                                    @Override
                                    public boolean hasSynced() {
                                        return false;
                                    }

                                    @Override
                                    public boolean isWatching() {
                                        return false;
                                    }

                                    @Override
                                    public boolean isRunning() {
                                        return false;
                                    }

                                    @Override
                                    public Status getStatus() {
                                        return s;
                                    }

                                    @Override
                                    public String getTargetNamespace() {
                                        return null;
                                    }
                                }));

        return new InformerWrappingEventSourceHealthIndicator() {
            @Override
            public Map<String, InformerHealthIndicator> informerHealthIndicators() {
                return informers;
            }

            @Override
            public ResourceConfiguration getInformerConfiguration() {
                return null;
            }
        };
    }
}
