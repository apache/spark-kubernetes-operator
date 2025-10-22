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

package org.apache.spark.k8s.operator;

import static org.apache.spark.k8s.operator.utils.TestUtils.setConfigKey;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.util.function.Consumer;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.RegisteredController;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import org.apache.spark.k8s.operator.client.KubernetesClientFactory;
import org.apache.spark.k8s.operator.config.SparkOperatorConf;
import org.apache.spark.k8s.operator.config.SparkOperatorConfigMapReconciler;
import org.apache.spark.k8s.operator.metrics.MetricsService;
import org.apache.spark.k8s.operator.metrics.MetricsSystem;
import org.apache.spark.k8s.operator.metrics.MetricsSystemFactory;
import org.apache.spark.k8s.operator.metrics.source.KubernetesMetricsInterceptor;
import org.apache.spark.k8s.operator.probe.ProbeService;
import org.apache.spark.k8s.operator.reconciler.SparkAppReconciler;
import org.apache.spark.k8s.operator.reconciler.SparkClusterReconciler;
import org.apache.spark.k8s.operator.utils.Utils;

class SparkOperatorTest {

  @Test
  void testOperatorConstructionWithDynamicConfigEnabled() {
    MetricsSystem mockMetricsSystem = mock(MetricsSystem.class);
    KubernetesClient mockClient = mock(KubernetesClient.class);
    boolean dynamicConfigEnabled = SparkOperatorConf.DYNAMIC_CONFIG_ENABLED.getValue();

    try (MockedStatic<MetricsSystemFactory> mockMetricsSystemFactory =
            mockStatic(MetricsSystemFactory.class);
        MockedStatic<KubernetesClientFactory> mockKubernetesClientFactory =
            mockStatic(KubernetesClientFactory.class);
        MockedStatic<Utils> mockUtils = mockStatic(Utils.class);
        MockedConstruction<Operator> operatorConstruction = mockConstruction(Operator.class);
        MockedConstruction<SparkAppReconciler> sparkAppReconcilerConstruction =
            mockConstruction(SparkAppReconciler.class);
        MockedConstruction<SparkOperatorConfigMapReconciler> configReconcilerConstruction =
            mockConstruction(SparkOperatorConfigMapReconciler.class);
        MockedConstruction<ProbeService> probeServiceConstruction =
            mockConstruction(ProbeService.class);
        MockedConstruction<MetricsService> metricsServiceConstruction =
            mockConstruction(MetricsService.class);
        MockedConstruction<KubernetesMetricsInterceptor> interceptorMockedConstruction =
            mockConstruction(KubernetesMetricsInterceptor.class)) {
      setConfigKey(SparkOperatorConf.DYNAMIC_CONFIG_ENABLED, true);
      mockMetricsSystemFactory
          .when(MetricsSystemFactory::createMetricsSystem)
          .thenReturn(mockMetricsSystem);
      mockKubernetesClientFactory
          .when(() -> KubernetesClientFactory.buildKubernetesClient(any()))
          .thenReturn(mockClient);
      mockUtils.when(Utils::getWatchedNamespaces).thenReturn(Set.of("namespace-1"));

      SparkOperator sparkOperator = new SparkOperator();
      Assertions.assertEquals(1, sparkOperator.registeredSparkControllers.size());
      Assertions.assertEquals(2, operatorConstruction.constructed().size());
      Assertions.assertEquals(1, sparkAppReconcilerConstruction.constructed().size());
      Assertions.assertEquals(1, configReconcilerConstruction.constructed().size());
      Assertions.assertEquals(1, probeServiceConstruction.constructed().size());
      Assertions.assertEquals(1, metricsServiceConstruction.constructed().size());
      Assertions.assertEquals(1, interceptorMockedConstruction.constructed().size());
      verify(mockMetricsSystem).registerSource(interceptorMockedConstruction.constructed().get(0));

      SparkAppReconciler sparkAppReconciler = sparkAppReconcilerConstruction.constructed().get(0);
      Operator sparkAppOperator = operatorConstruction.constructed().get(0);
      verify(sparkAppOperator).register(eq(sparkAppReconciler), any(Consumer.class));
    } finally {
      setConfigKey(SparkOperatorConf.DYNAMIC_CONFIG_ENABLED, dynamicConfigEnabled);
    }
  }

  @Test
  void testOperatorConstructionWithDynamicConfigDisabled() {
    MetricsSystem mockMetricsSystem = mock(MetricsSystem.class);
    KubernetesClient mockClient = mock(KubernetesClient.class);
    boolean dynamicConfigEnabled = SparkOperatorConf.DYNAMIC_CONFIG_ENABLED.getValue();
    try (MockedStatic<MetricsSystemFactory> mockMetricsSystemFactory =
            mockStatic(MetricsSystemFactory.class);
        MockedStatic<KubernetesClientFactory> mockKubernetesClientFactory =
            mockStatic(KubernetesClientFactory.class);
        MockedConstruction<Operator> operatorConstruction = mockConstruction(Operator.class);
        MockedConstruction<SparkAppReconciler> sparkAppReconcilerConstruction =
            mockConstruction(SparkAppReconciler.class);
        MockedConstruction<ProbeService> probeServiceConstruction =
            mockConstruction(ProbeService.class);
        MockedConstruction<MetricsService> metricsServiceConstruction =
            mockConstruction(MetricsService.class);
        MockedConstruction<KubernetesMetricsInterceptor> interceptorMockedConstruction =
            mockConstruction(KubernetesMetricsInterceptor.class)) {
      setConfigKey(SparkOperatorConf.DYNAMIC_CONFIG_ENABLED, false);
      mockMetricsSystemFactory
          .when(MetricsSystemFactory::createMetricsSystem)
          .thenReturn(mockMetricsSystem);
      mockKubernetesClientFactory
          .when(() -> KubernetesClientFactory.buildKubernetesClient(any()))
          .thenReturn(mockClient);
      SparkOperator sparkOperator = new SparkOperator();
      Assertions.assertEquals(1, sparkOperator.registeredSparkControllers.size());
      Assertions.assertEquals(1, operatorConstruction.constructed().size());
      Assertions.assertEquals(1, sparkAppReconcilerConstruction.constructed().size());
      Assertions.assertEquals(1, probeServiceConstruction.constructed().size());
      Assertions.assertEquals(1, metricsServiceConstruction.constructed().size());
      Assertions.assertEquals(1, interceptorMockedConstruction.constructed().size());
      verify(mockMetricsSystem).registerSource(interceptorMockedConstruction.constructed().get(0));
    } finally {
      setConfigKey(SparkOperatorConf.DYNAMIC_CONFIG_ENABLED, dynamicConfigEnabled);
    }
  }

  @SuppressWarnings("PMD.UnusedLocalVariable")
  @Test
  void testUpdateWatchedNamespacesWithDynamicConfigEnabled() {
    MetricsSystem mockMetricsSystem = mock(MetricsSystem.class);
    KubernetesClient mockClient = mock(KubernetesClient.class);
    var registeredController = mock(RegisteredController.class);
    when(registeredController.allowsNamespaceChanges()).thenReturn(true);
    boolean dynamicConfigEnabled = SparkOperatorConf.DYNAMIC_CONFIG_ENABLED.getValue();

    try (MockedStatic<MetricsSystemFactory> mockMetricsSystemFactory =
            mockStatic(MetricsSystemFactory.class);
        MockedStatic<KubernetesClientFactory> mockKubernetesClientFactory =
            mockStatic(KubernetesClientFactory.class);
        MockedStatic<Utils> mockUtils = mockStatic(Utils.class);
        MockedConstruction<Operator> operatorConstruction =
            mockConstruction(
                Operator.class,
                (mock, context) -> {
                  when(mock.register(any(SparkAppReconciler.class), any(Consumer.class)))
                      .thenReturn(registeredController);
                  when(mock.register(any(SparkClusterReconciler.class), any(Consumer.class)))
                      .thenReturn(registeredController);
                });
        MockedConstruction<SparkAppReconciler> sparkAppReconcilerConstruction =
            mockConstruction(SparkAppReconciler.class);
        MockedConstruction<SparkOperatorConfigMapReconciler> configReconcilerConstruction =
            mockConstruction(SparkOperatorConfigMapReconciler.class);
        MockedConstruction<ProbeService> probeServiceConstruction =
            mockConstruction(ProbeService.class);
        MockedConstruction<MetricsService> metricsServiceConstruction =
            mockConstruction(MetricsService.class);
        MockedConstruction<KubernetesMetricsInterceptor> interceptorMockedConstruction =
            mockConstruction(KubernetesMetricsInterceptor.class)) {
      setConfigKey(SparkOperatorConf.DYNAMIC_CONFIG_ENABLED, true);
      mockMetricsSystemFactory
          .when(MetricsSystemFactory::createMetricsSystem)
          .thenReturn(mockMetricsSystem);
      mockKubernetesClientFactory
          .when(() -> KubernetesClientFactory.buildKubernetesClient(any()))
          .thenReturn(mockClient);
      mockUtils.when(Utils::getWatchedNamespaces).thenReturn(Set.of("namespace-1"));
      SparkOperator sparkOperator = new SparkOperator();
      Set<String> updatedNamespaces = Set.of("namespace-1", "namespace-2");
      Assertions.assertTrue(sparkOperator.updateWatchingNamespaces(updatedNamespaces));
      Assertions.assertEquals(updatedNamespaces, sparkOperator.watchedNamespaces);
      verify(registeredController).allowsNamespaceChanges();
      verify(registeredController).changeNamespaces(updatedNamespaces);
      verifyNoMoreInteractions(registeredController);
    } finally {
      setConfigKey(SparkOperatorConf.DYNAMIC_CONFIG_ENABLED, dynamicConfigEnabled);
    }
  }
}
