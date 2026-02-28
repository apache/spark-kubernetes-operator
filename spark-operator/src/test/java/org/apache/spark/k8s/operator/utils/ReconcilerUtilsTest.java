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

package org.apache.spark.k8s.operator.utils;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.NamespaceableResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ReconcilerUtilsTest {

  private Pod buildPod() {
    return new PodBuilder()
        .withNewMetadata()
        .withName("test-pod")
        .withNamespace("default")
        .endMetadata()
        .build();
  }

  @SuppressWarnings("unchecked")
  private NamespaceableResource<Pod> mockClientReturning(
      KubernetesClient mockClient, Pod pod) {
    NamespaceableResource<Pod> mockResource = mock(NamespaceableResource.class);
    when(mockClient.resource(pod)).thenReturn(mockResource);
    return mockResource;
  }

  @ParameterizedTest
  @ValueSource(ints = {500, 502, 503, 504})
  void retriesCreateOnTransient5xxAndSucceeds(int errorCode) {
    Pod pod = buildPod();
    KubernetesClient mockClient = mock(KubernetesClient.class);
    NamespaceableResource<Pod> mockResource = mockClientReturning(mockClient, pod);
    when(mockResource.get()).thenReturn(null);
    // 1st CREATE -> fail; 2nd CREATE -> success
    when(mockResource.create())
        .thenThrow(new KubernetesClientException("Transient error", errorCode, null))
        // succeeds on 2nd attempt
        .thenReturn(pod);

    Optional<Pod> result = ReconcilerUtils.getOrCreateSecondaryResource(mockClient, pod);
    assertTrue(result.isPresent());
  }

  @ParameterizedTest
  @ValueSource(ints = {500, 502, 503, 504})
  void returnsResourceFoundByGetAfterTransient5xx(int errorCode) {
    Pod pod = buildPod();
    KubernetesClient mockClient = mock(KubernetesClient.class);
    NamespaceableResource<Pod> mockResource = mockClientReturning(mockClient, pod);
    // 1st GET -> not found; 2nd GET (after failed create) -> resource found
    // mimic create landed on server but response was lost
    when(mockResource.get()).thenReturn(null).thenReturn(pod);
    when(mockResource.create())
        .thenThrow(new KubernetesClientException("Transient error", errorCode, null));

    Optional<Pod> result = ReconcilerUtils.getOrCreateSecondaryResource(mockClient, pod);
    assertTrue(result.isPresent());
  }

  @Test
  void retriesOnNetworkLevelTimeout() {
    // Network-level timeout surfaces as KubernetesClientException with code 0 in fabric8
    Pod pod = buildPod();
    KubernetesClient mockClient = mock(KubernetesClient.class);
    NamespaceableResource<Pod> mockResource = mockClientReturning(mockClient, pod);
    // 1st GET -> not found; and GET (after timeout) -> resource found
    when(mockResource.get()).thenReturn(null).thenReturn(pod);
    when(mockResource.create())
        .thenThrow(new KubernetesClientException("Connection timeout", 0, null));

    Optional<Pod> result = ReconcilerUtils.getOrCreateSecondaryResource(mockClient, pod);
    assertTrue(result.isPresent());
  }

  @Test
  void throwsAfterMaxAttemptsExceededOnTransientError() {
    Pod pod = buildPod();
    KubernetesClient mockClient = mock(KubernetesClient.class);
    NamespaceableResource<Pod> mockResource = mockClientReturning(mockClient, pod);
    // resource never appears in any GET
    when(mockResource.get()).thenReturn(null);
    when(mockResource.create())
        .thenThrow(new KubernetesClientException("Service unavailable", 503, null));

    assertThrows(
        KubernetesClientException.class,
        () -> ReconcilerUtils.getOrCreateSecondaryResource(mockClient, pod));
  }

  @Test
  void doesNotRetryOnNonTransientError() {
    Pod pod = buildPod();
    KubernetesClient mockClient = mock(KubernetesClient.class);
    NamespaceableResource<Pod> mockResource = mockClientReturning(mockClient, pod);
    when(mockResource.get()).thenReturn(null);
    when(mockResource.create())
        .thenThrow(new KubernetesClientException("Unprocessable entity", 422, null));

    assertThrows(
        KubernetesClientException.class,
        () -> ReconcilerUtils.getOrCreateSecondaryResource(mockClient, pod));
  }
}
