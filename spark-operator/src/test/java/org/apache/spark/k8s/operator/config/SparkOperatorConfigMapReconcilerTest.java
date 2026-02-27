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

import static org.apache.spark.k8s.operator.config.SparkOperatorConf.RECONCILER_INTERVAL_SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

import java.util.Map;
import java.util.function.Function;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubeapitest.junit.EnableKubeAPIServer;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.Operator;
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
    operator = new Operator(o -> o.withKubernetesClient(client));
    operator.register(reconciler);
    operator.start();
  }

  @AfterEach
  void stopController() {
    operator.stop();
  }

  @Test
  @SuppressWarnings("PMD.UnitTestShouldIncludeAssert")
  void sanityTest() {
    client.resource(testConfigMap()).create();

    await()
        .untilAsserted(
            () -> {
              assertThat(RECONCILER_INTERVAL_SECONDS.getValue()).isEqualTo(60L);
            });
  }

  ConfigMap testConfigMap() {
    ConfigMap configMap = new ConfigMap();
    configMap.setMetadata(
        new ObjectMetaBuilder().withName("spark-conf").withNamespace("default").build());
    configMap.setData(
        Map.of(RECONCILER_INTERVAL_SECONDS.getKey(), TARGET_RECONCILER_INTERVAL.toString()));
    return configMap;
  }
}
