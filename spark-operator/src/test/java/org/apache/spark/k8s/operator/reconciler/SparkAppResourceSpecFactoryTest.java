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

package org.apache.spark.k8s.operator.reconciler;

import static org.apache.spark.k8s.operator.Constants.DRIVER_SPARK_TEMPLATE_FILE_PROP_KEY;
import static org.apache.spark.k8s.operator.Constants.EXECUTOR_SPARK_TEMPLATE_FILE_PROP_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.apache.spark.k8s.operator.SparkAppResourceSpec;
import org.apache.spark.k8s.operator.SparkAppSubmissionWorker;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.kueue.KueuePodSetFlavor;
import org.apache.spark.k8s.operator.spec.BaseApplicationTemplateSpec;
import org.apache.spark.k8s.operator.utils.ModelUtils;

class SparkAppResourceSpecFactoryTest {

  @Test
  void testBuildResourceSpecCoversBasicOverride() {
    SparkApplication app = new SparkApplication();
    app.setMetadata(
        new ObjectMetaBuilder().withNamespace("foo").withName("bar-app").withUid("uid").build());
    KubernetesClient mockClient = mock(KubernetesClient.class);
    Pod mockDriver = mock(Pod.class);
    when(mockDriver.getMetadata()).thenReturn(new ObjectMeta());
    SparkAppResourceSpec mockSpec = mock(SparkAppResourceSpec.class);
    when(mockSpec.getConfiguredPod()).thenReturn(mockDriver);
    ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);
    SparkAppSubmissionWorker mockWorker = mock(SparkAppSubmissionWorker.class);
    when(mockWorker.getResourceSpec(any(), any(), captor.capture())).thenReturn(mockSpec);
    SparkAppResourceSpec spec =
        SparkAppResourceSpecFactory.buildResourceSpec(app, mockClient, mockWorker, Map.of());
    verify(mockWorker).getResourceSpec(eq(app), eq(mockClient), any());
    Map<String, String> props = captor.getValue();
    assertTrue(props.containsKey("spark.kubernetes.namespace"));
    assertEquals("foo", props.get("spark.kubernetes.namespace"));
    ArgumentCaptor<ObjectMeta> metaArgumentCaptor = ArgumentCaptor.forClass(ObjectMeta.class);
    verify(mockDriver).setMetadata(metaArgumentCaptor.capture());
    assertEquals(mockSpec, spec);
    ObjectMeta metaOverride = metaArgumentCaptor.getValue();
    assertEquals(1, metaOverride.getOwnerReferences().size());
    assertEquals("bar-app", metaOverride.getOwnerReferences().get(0).getName());
    assertEquals("uid", metaOverride.getOwnerReferences().get(0).getUid());
    assertEquals(app.getKind(), metaOverride.getOwnerReferences().get(0).getKind());
  }

  @Test
  void testBuildResourceSpecAppliesKueueFlavorsToPodTemplates() throws Exception {
    SparkApplication app = new SparkApplication();
    app.setMetadata(
        new ObjectMetaBuilder().withNamespace("foo").withName("bar-app").withUid("uid").build());
    Toleration userToleration = toleration("user");
    app.getSpec()
        .setExecutorSpec(
            BaseApplicationTemplateSpec.builder()
                .podTemplateSpec(
                    new PodTemplateSpecBuilder()
                        .withNewSpec()
                        .withNodeSelector(Map.of("zone", "a"))
                        .withTolerations(userToleration)
                        .endSpec()
                        .build())
                .build());
    String originalSpec = ModelUtils.objectMapper.writeValueAsString(app.getSpec());
    KubernetesClient mockClient = mock(KubernetesClient.class);
    SparkAppResourceSpec mockSpec = mock(SparkAppResourceSpec.class);
    Pod driver = new PodBuilder().withNewMetadata().endMetadata().withNewSpec().endSpec().build();
    when(mockSpec.getConfiguredPod()).thenReturn(driver);
    Map<String, String> templateFiles = new HashMap<>();
    Map<String, Pod> templates = new HashMap<>();
    SparkAppSubmissionWorker mockWorker = mock(SparkAppSubmissionWorker.class);
    when(mockWorker.getResourceSpec(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Map<String, String> confOverrides = invocation.getArgument(2);
              for (String key :
                  List.of(
                      DRIVER_SPARK_TEMPLATE_FILE_PROP_KEY, EXECUTOR_SPARK_TEMPLATE_FILE_PROP_KEY)) {
                String path = confOverrides.get(key);
                templateFiles.put(key, path);
                templates.put(
                    key,
                    ModelUtils.objectMapper.readValue(Files.readString(Path.of(path)), Pod.class));
              }
              return mockSpec;
            });
    Toleration spot = toleration("spot");
    Toleration gpu = toleration("gpu");

    SparkAppResourceSpecFactory.buildResourceSpec(
        app,
        mockClient,
        mockWorker,
        Map.of(
            "driver", new KueuePodSetFlavor(Map.of("pool", "cpu"), List.of(spot)),
            "executor", new KueuePodSetFlavor(Map.of("pool", "gpu"), List.of(gpu))));

    // A driver without the pod template gets a new one
    Pod driverTemplate = templates.get(DRIVER_SPARK_TEMPLATE_FILE_PROP_KEY);
    assertEquals(Map.of("pool", "cpu"), driverTemplate.getSpec().getNodeSelector());
    assertEquals(List.of(spot), driverTemplate.getSpec().getTolerations());
    // The flavor is merged into the executor pod template of the spec
    Pod executorTemplate = templates.get(EXECUTOR_SPARK_TEMPLATE_FILE_PROP_KEY);
    assertEquals(Map.of("zone", "a", "pool", "gpu"), executorTemplate.getSpec().getNodeSelector());
    assertEquals(List.of(userToleration, gpu), executorTemplate.getSpec().getTolerations());
    // The temp files are cleaned up, and the SparkApplication is not modified
    templateFiles.values().forEach(path -> assertFalse(Files.exists(Path.of(path)), path));
    assertEquals(originalSpec, ModelUtils.objectMapper.writeValueAsString(app.getSpec()));
  }

  private static Toleration toleration(String key) {
    return new Toleration("NoSchedule", key, "Exists", null, null);
  }
}
