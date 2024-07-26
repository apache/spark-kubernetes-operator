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

package org.apache.spark.k8s.operator.probe;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.RuntimeInfo;
import io.javaoperatorsdk.operator.api.config.ResourceConfiguration;
import io.javaoperatorsdk.operator.health.InformerHealthIndicator;
import io.javaoperatorsdk.operator.health.InformerWrappingEventSourceHealthIndicator;
import io.javaoperatorsdk.operator.health.Status;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.metrics.healthcheck.SentinelManager;

@EnableKubernetesMockClient(crud = true)
@SuppressFBWarnings(
    value = {"UWF_UNWRITTEN_FIELD", "NP_UNWRITTEN_FIELD", "UUF_UNUSED_FIELD"},
    justification = "Unwritten fields are covered by Kubernetes mock client")
class HealthProbeTest {
  private static Operator operator;
  private static Operator sparkConfMonitor;
  private static List<Operator> operators;
  private KubernetesClient kubernetesClient;
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
    RuntimeInfo runtimeInfo =
        new RuntimeInfo(
            new Operator(overrider -> overrider.withKubernetesClient(kubernetesClient))) {
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

    RuntimeInfo runtimeInfo2 =
        new RuntimeInfo(
            new Operator(overrider -> overrider.withKubernetesClient(kubernetesClient))) {
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
    HealthProbe healthyProbe = new HealthProbe(operators, Collections.emptyList());
    isRunning.set(true);
    assertFalse(
        healthyProbe.isHealthy(),
        "Healthy Probe should fail when the spark conf monitor operator is not running");
    isRunning2.set(true);
    assertTrue(
        healthyProbe.isHealthy(), "Healthy Probe should pass when both operators are running");

    unhealthyEventSources2.put(
        "c1", Map.of("e1", informerHealthIndicator(Map.of("i1", Status.UNHEALTHY))));
    assertFalse(
        healthyProbe.isHealthy(),
        "Healthy Probe should fail when monitor's informer health is not healthy");
    unhealthyEventSources2.clear();
    assertTrue(healthyProbe.isHealthy(), "Healthy Probe should pass");
  }

  @Test
  void testHealthProbeWithInformerHealthWithSingleOperator() {
    HealthProbe healthyProbe =
        new HealthProbe(Collections.singletonList(operator), Collections.emptyList());
    assertFalse(healthyProbe.isHealthy(), "Health Probe should fail when operator is not running");
    isRunning.set(true);
    unhealthyEventSources.put(
        "c1", Map.of("e1", informerHealthIndicator(Map.of("i1", Status.UNHEALTHY))));
    assertFalse(
        healthyProbe.isHealthy(), "Healthy Probe should fail when informer health is not healthy");
    unhealthyEventSources.clear();
    assertTrue(healthyProbe.isHealthy(), "Healthy Probe should pass");
  }

  @Test
  void testHealthProbeWithSentinelHealthWithMultiOperators() {
    var sentinelManager = mock(SentinelManager.class);
    HealthProbe healthyProbe =
        new HealthProbe(operators, Collections.singletonList(sentinelManager));
    isRunning.set(true);
    isRunning2.set(true);
    when(sentinelManager.allSentinelsAreHealthy()).thenReturn(false);
    assertFalse(
        healthyProbe.isHealthy(), "Healthy Probe should fail when sentinels report failures");

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
