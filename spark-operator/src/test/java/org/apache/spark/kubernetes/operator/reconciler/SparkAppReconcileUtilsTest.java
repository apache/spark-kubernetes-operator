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

package org.apache.spark.kubernetes.operator.reconciler;

import java.time.Instant;
import java.util.Map;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import org.apache.spark.kubernetes.operator.SparkAppSubmissionWorker;
import org.apache.spark.kubernetes.operator.SparkAppResourceSpec;
import org.apache.spark.kubernetes.operator.SparkApplication;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SparkAppReconcileUtilsTest {

  @Test
  void testForceDeleteEnabled() {
    SparkApplication app = new SparkApplication();
    app.getStatus().getCurrentState().setLastTransitionTime(
        Instant.now().minusSeconds(5).toString());
    app.getSpec().getApplicationTolerations().getApplicationTimeoutConfig()
        .setForceTerminationGracePeriodMillis(3000L);
    Assertions.assertTrue(SparkAppReconcileUtils.enableForceDelete(app));
  }

  @Test
  void testBuildResourceSpecCoversBasicOverride() {
    SparkApplication app = new SparkApplication();
    app.setMetadata(new ObjectMetaBuilder()
        .withNamespace("foo")
        .withName("bar-app")
        .withUid("uid")
        .build());
    KubernetesClient mockClient = mock(KubernetesClient.class);
    Pod mockDriver = mock(Pod.class);
    when(mockDriver.getMetadata()).thenReturn(new ObjectMeta());
    SparkAppResourceSpec mockSpec = mock(SparkAppResourceSpec.class);
    when(mockSpec.getConfiguredPod()).thenReturn(mockDriver);
    ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);
    SparkAppSubmissionWorker mockWorker = mock(SparkAppSubmissionWorker.class);
    when(mockWorker.getResourceSpec(any(), any(), captor.capture())).thenReturn(mockSpec);
    SparkAppResourceSpec spec = SparkAppReconcileUtils.buildResourceSpec(app, mockClient,
        mockWorker);
    verify(mockWorker).getResourceSpec(eq(app), eq(mockClient), any());
    Map<String, String> props = captor.getValue();
    Assertions.assertTrue(props.containsKey("spark.kubernetes.namespace"));
    Assertions.assertEquals("foo", props.get("spark.kubernetes.namespace"));
    ArgumentCaptor<ObjectMeta> metaArgumentCaptor = ArgumentCaptor.forClass(ObjectMeta.class);
    verify(mockDriver).setMetadata(metaArgumentCaptor.capture());
    Assertions.assertEquals(mockSpec, spec);
    ObjectMeta metaOverride = metaArgumentCaptor.getValue();
    Assertions.assertEquals(1, metaOverride.getOwnerReferences().size());
    Assertions.assertEquals("bar-app", metaOverride.getOwnerReferences().get(0).getName());
    Assertions.assertEquals("uid", metaOverride.getOwnerReferences().get(0).getUid());
    Assertions.assertEquals(app.getKind(), metaOverride.getOwnerReferences().get(0).getKind());
  }
}
