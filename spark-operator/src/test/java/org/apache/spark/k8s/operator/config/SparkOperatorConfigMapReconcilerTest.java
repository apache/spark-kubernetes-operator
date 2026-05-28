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

package org.apache.spark.k8s.operator.config;

import static org.apache.spark.k8s.operator.config.SparkOperatorConf.DYNAMIC_CONFIG_MAP_NAME;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.RECONCILER_INTERVAL_SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubeapitest.junit.EnableKubeAPIServer;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings(
    value = {"UWF_UNWRITTEN_FIELD"},
    justification = "Unwritten fields are covered by Kubernetes mock client")
@EnableKubeAPIServer
class SparkOperatorConfigMapReconcilerTest {

  static final Long TARGET_RECONCILER_INTERVAL = 60L;

  private static KubernetesClient client;

  Operator operator;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void startController() {
    var reconciler =
        new SparkOperatorConfigMapReconciler(mock(Function.class), mock(Function.class));
    operator = new Operator(o -> o.withKubernetesClient(client)
            .withCloseClientOnStop(false));
    operator.register(reconciler);
    operator.start();
  }

  @AfterEach
  void stopController() {
    operator.stop();
    SparkOperatorConfManager.INSTANCE.refresh(Map.of());
  }

  @Test
  void sanityTest() {
    client.resource(testConfigMap()).create();

    await()
        .untilAsserted(
            () -> {
              assertThat(RECONCILER_INTERVAL_SECONDS.getValue()).isEqualTo(60L);
            });
  }

  @Test
  @SuppressWarnings("unchecked")
  void reconcileSkipsConfigMapWithNonMatchingName() throws Exception {
    Function<Set<String>, Boolean> namespaceUpdater = mock(Function.class);
    Function<Void, Set<String>> watchedNamespacesGetter = mock(Function.class);
    SparkOperatorConfigMapReconciler reconciler =
        new SparkOperatorConfigMapReconciler(namespaceUpdater, watchedNamespacesGetter);

    ConfigMap rogueCm = configMap("rogue-config-map", Map.of("rogue.key", "rogue-value"));

    UpdateControl<ConfigMap> result = reconciler.reconcile(rogueCm, mock(Context.class));

    assertTrue(result.isNoUpdate());
    verify(namespaceUpdater, never()).apply(any());
    verify(watchedNamespacesGetter, never()).apply(any());
    // The rogue config map's data must not have leaked into the override store.
    assertNull(SparkOperatorConfManager.INSTANCE.configOverrides.getProperty("rogue.key"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void reconcileRefreshesConfigForMatchingName() throws Exception {
    Function<Set<String>, Boolean> namespaceUpdater = mock(Function.class);
    Function<Void, Set<String>> watchedNamespacesGetter = mock(Function.class);
    Set<String> watched = Set.of("ns1");
    when(watchedNamespacesGetter.apply(null)).thenReturn(watched);

    SparkOperatorConfigMapReconciler reconciler =
        new SparkOperatorConfigMapReconciler(namespaceUpdater, watchedNamespacesGetter);

    ConfigMap goodCm =
        configMap(
            DYNAMIC_CONFIG_MAP_NAME.getValue(),
            Map.of(RECONCILER_INTERVAL_SECONDS.getKey(), TARGET_RECONCILER_INTERVAL.toString()));

    UpdateControl<ConfigMap> result = reconciler.reconcile(goodCm, mock(Context.class));

    assertTrue(result.isNoUpdate());
    verify(watchedNamespacesGetter, times(1)).apply(null);
    verify(namespaceUpdater, times(1)).apply(watched);
    assertEquals(TARGET_RECONCILER_INTERVAL, RECONCILER_INTERVAL_SECONDS.getValue());
  }

  @Test
  @SuppressWarnings("unchecked")
  void reconcileHonorsCustomConfiguredConfigMapName() throws Exception {
    String customName = "custom-dynamic-config";
    // DYNAMIC_CONFIG_MAP_NAME has enableDynamicOverride=false, so it reads from initialConfig
    // which is populated only at singleton construction time. Inject the override directly so
    // we can exercise the reconciler with a non-default configured name.
    Object prevValue =
        SparkOperatorConfManager.INSTANCE.initialConfig.put(
            DYNAMIC_CONFIG_MAP_NAME.getKey(), customName);
    try {
      assertEquals(customName, DYNAMIC_CONFIG_MAP_NAME.getValue());

      Function<Set<String>, Boolean> namespaceUpdater = mock(Function.class);
      Function<Void, Set<String>> watchedNamespacesGetter = mock(Function.class);
      when(watchedNamespacesGetter.apply(null)).thenReturn(Set.of());

      SparkOperatorConfigMapReconciler reconciler =
          new SparkOperatorConfigMapReconciler(namespaceUpdater, watchedNamespacesGetter);

      // Default-named ConfigMap must be ignored when a custom name is configured.
      ConfigMap defaultNameCm =
          configMap("spark-kubernetes-operator-dynamic-configuration",
              Map.of("rogue.key", "rogue-value"));
      reconciler.reconcile(defaultNameCm, mock(Context.class));
      verify(namespaceUpdater, never()).apply(any());

      // Custom-named ConfigMap must be processed.
      ConfigMap customCm = configMap(customName, Map.of());
      reconciler.reconcile(customCm, mock(Context.class));
      verify(namespaceUpdater, times(1)).apply(any());
    } finally {
      if (prevValue != null) {
        SparkOperatorConfManager.INSTANCE.initialConfig.put(
            DYNAMIC_CONFIG_MAP_NAME.getKey(), prevValue);
      } else {
        SparkOperatorConfManager.INSTANCE.initialConfig.remove(DYNAMIC_CONFIG_MAP_NAME.getKey());
      }
    }
  }

  ConfigMap testConfigMap() {
    return configMap(
        DYNAMIC_CONFIG_MAP_NAME.getValue(),
        Map.of(RECONCILER_INTERVAL_SECONDS.getKey(), TARGET_RECONCILER_INTERVAL.toString()));
  }

  private static ConfigMap configMap(String name, Map<String, String> data) {
    ConfigMap configMap = new ConfigMap();
    configMap.setMetadata(
        new ObjectMetaBuilder().withName(name).withNamespace("default").build());
    configMap.setData(data);
    return configMap;
  }
}
