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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkAppResourceSpec;
import org.apache.spark.k8s.operator.SparkAppSubmissionWorker;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.spec.BaseApplicationTemplateSpec;

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
        SparkAppResourceSpecFactory.buildResourceSpec(app, mockClient, mockWorker);
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
  void testBuildResourceSpecWritesAndCleansTempFilesForPodTemplateSpec() {
    SparkApplication app = new SparkApplication();
    app.setMetadata(
        new ObjectMetaBuilder().withNamespace("foo").withName("bar-app").withUid("uid").build());

    BaseApplicationTemplateSpec driverSpec = new BaseApplicationTemplateSpec();
    driverSpec.setPodTemplateSpec(new PodTemplateSpec());
    BaseApplicationTemplateSpec executorSpec = new BaseApplicationTemplateSpec();
    executorSpec.setPodTemplateSpec(new PodTemplateSpec());

    ApplicationSpec appSpec = new ApplicationSpec();
    appSpec.setDriverSpec(driverSpec);
    appSpec.setExecutorSpec(executorSpec);
    app.setSpec(appSpec);

    KubernetesClient mockClient = mock(KubernetesClient.class);
    Pod mockDriver = mock(Pod.class);
    when(mockDriver.getMetadata()).thenReturn(new ObjectMeta());
    SparkAppResourceSpec mockSpec = mock(SparkAppResourceSpec.class);
    when(mockSpec.getConfiguredPod()).thenReturn(mockDriver);

    AtomicReference<String> driverTempPath = new AtomicReference<>();
    AtomicReference<String> executorTempPath = new AtomicReference<>();
    SparkAppSubmissionWorker mockWorker = mock(SparkAppSubmissionWorker.class);
    when(mockWorker.getResourceSpec(any(), any(), any()))
        .thenAnswer(
            invocation -> {
              Map<String, String> conf = invocation.getArgument(2);
              driverTempPath.set(conf.get(Constants.DRIVER_SPARK_TEMPLATE_FILE_PROP_KEY));
              executorTempPath.set(conf.get(Constants.EXECUTOR_SPARK_TEMPLATE_FILE_PROP_KEY));
              return mockSpec;
            });

    SparkAppResourceSpecFactory.buildResourceSpec(app, mockClient, mockWorker);

    if (driverTempPath.get() != null) {
      assertFalse(new File(driverTempPath.get()).exists(), "Driver temp file should be deleted");
    }
    if (executorTempPath.get() != null) {
      assertFalse(
          new File(executorTempPath.get()).exists(), "Executor temp file should be deleted");
    }
  }
}
