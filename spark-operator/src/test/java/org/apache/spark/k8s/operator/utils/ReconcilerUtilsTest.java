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

import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.SSLHandshakeException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.StatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.NamespaceableResource;
import io.vertx.core.http.HttpClosedException;
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
    // A connection that broke carries no response status, so fabric8 reports it with the absent
    // response code and the exception that broke it, which is what marks it as retriable.
    Pod pod = buildPod();
    KubernetesClient mockClient = mock(KubernetesClient.class);
    NamespaceableResource<Pod> mockResource = mockClientReturning(mockClient, pod);
    // 1st GET -> not found; and GET (after timeout) -> resource found
    when(mockResource.get()).thenReturn(null).thenReturn(pod);
    when(mockResource.create())
        .thenThrow(
            new KubernetesClientException("Connection timeout", new SocketTimeoutException()));

    Optional<Pod> result = ReconcilerUtils.getOrCreateSecondaryResource(mockClient, pod);
    assertTrue(result.isPresent());
  }

  @Test
  void classifiesTransportFailureAsTransient() {
    // fabric8 reports a broken connection with no response code and the exception that broke it.
    assertTrue(
        ReconcilerUtils.isTransientError(
            new KubernetesClientException("Connection reset", new SocketException())));
  }

  @Test
  void classifiesNestedConnectionFailureAsTransient() {
    // fabric8 wraps the failure its HTTP client already wrapped, so the chain has to be walked.
    assertTrue(
        ReconcilerUtils.isTransientError(
            new KubernetesClientException(
                "Operation failed", new IOException("wrapped", new ConnectException("refused")))));
  }

  @Test
  void classifiesRequestTimeoutOfTheHttpClientAsTransient() {
    // The HTTP client reports its request timeout outside the IOException hierarchy.
    assertTrue(
        ReconcilerUtils.isTransientError(
            new KubernetesClientException("Timed out", new TimeoutException("1000ms exceeded"))));
  }

  @Test
  void classifiesConnectionClosedByThePeerAsTransient() {
    // The connection the API server closes mid-response, as it does on a rolling restart, is
    // reported by the HTTP client in use with a type of its own that is not an IOException. It is
    // the most common way a request goes unanswered, so no list of socket types may gate it.
    assertTrue(
        ReconcilerUtils.isTransientError(
            new KubernetesClientException(
                "Operation failed", new HttpClosedException("Connection was closed"))));
  }

  @Test
  void doesNotClassifyUnreadableResponseAsTransient() {
    // An answer that arrived but could not be parsed carries the parsing failure as its cause,
    // and shares the absent response code of a connection that broke. The server answered and the
    // body will not change, so waiting it out would retry forever.
    assertFalse(
        ReconcilerUtils.isTransientError(
            new KubernetesClientException(
                "Unreadable response",
                MismatchedInputException.from(
                    (JsonParser) null, Pod.class, "Unrecognized field \"foo\""))));
  }

  @Test
  void doesNotClassifyCertificateFailureAsTransient() {
    // A handshake rejected over a certificate, a host name or a trust store is answer-less like a
    // broken connection, but repeating it cannot fix it.
    assertFalse(
        ReconcilerUtils.isTransientError(
            new KubernetesClientException(
                "Handshake failed",
                new IOException(
                    "wrapped", new SSLHandshakeException("PKIX path building failed")))));
  }

  @Test
  void doesNotClassifyPermanentClientSideRejectionAsTransient() {
    // A rejection raised before the request is sent shares the response code of a transport
    // failure and is told apart only by having no cause. It never clears on its own, so it must
    // keep its Warning event and its longer requeue interval.
    assertFalse(
        ReconcilerUtils.isTransientError(
            new KubernetesClientException("resourceVersion cannot be null")));
  }

  @ParameterizedTest
  @ValueSource(ints = {408, 502, 503, 504})
  void classifiesTransientHttpStatusAsTransient(int errorCode) {
    assertTrue(
        ReconcilerUtils.isTransientError(
            new KubernetesClientException("Transient error", errorCode, null)));
  }

  @Test
  void doesNotClassifyInternalServerErrorAsTransient() {
    // 500 is retried by getOrCreateSecondaryResource on its own, but it is a decision by a server
    // that answered, so the callers that suppress events on a transient failure must still warn.
    assertFalse(
        ReconcilerUtils.isTransientError(
            new KubernetesClientException("Internal error", HTTP_INTERNAL_ERROR, null)));
  }

  @Test
  void backsOffBeforeRetryingTransportFailure() {
    Pod pod = buildPod();
    KubernetesClient mockClient = mock(KubernetesClient.class);
    NamespaceableResource<Pod> mockResource = mockClientReturning(mockClient, pod);
    // the resource never appears via GET, so the retry loop must go through backoffSleep
    when(mockResource.get()).thenReturn(null);
    // 1st CREATE -> connection reset; 2nd CREATE -> success
    when(mockResource.create())
        .thenThrow(new KubernetesClientException("Connection reset", new SocketException()))
        .thenReturn(pod);

    long start = System.nanoTime();
    Optional<Pod> result = ReconcilerUtils.getOrCreateSecondaryResource(mockClient, pod);
    long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

    assertTrue(result.isPresent());
    assertTrue(
        elapsedMillis >= 1000L, "a broken connection should back off before the next attempt");
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

  @Test
  void retriesCreateOn429AndSucceedsHonoringRetryAfter() {
    Pod pod = buildPod();
    KubernetesClient mockClient = mock(KubernetesClient.class);
    NamespaceableResource<Pod> mockResource = mockClientReturning(mockClient, pod);
    when(mockResource.get()).thenReturn(null);
    // server reports 1s Retry-After, mirrored into status.details.retryAfterSeconds
    KubernetesClientException tooManyRequests =
        new KubernetesClientException(
            "Too Many Requests",
            429,
            new StatusBuilder()
                .withCode(429)
                .withNewDetails()
                .withRetryAfterSeconds(1)
                .endDetails()
                .build());
    // 1st CREATE -> 429; 2nd CREATE -> success
    when(mockResource.create()).thenThrow(tooManyRequests).thenReturn(pod);

    long start = System.nanoTime();
    Optional<Pod> result = ReconcilerUtils.getOrCreateSecondaryResource(mockClient, pod);
    long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

    assertTrue(result.isPresent());
    assertTrue(elapsedMillis >= 1000L, "should have slept for the server-requested 1s");
  }

  @Test
  void retriesCreateOnTransient5xxHonoringRetryAfterWhenPresent() {
    Pod pod = buildPod();
    KubernetesClient mockClient = mock(KubernetesClient.class);
    NamespaceableResource<Pod> mockResource = mockClientReturning(mockClient, pod);
    // resource never appears via GET, so the retry loop must go through backoffSleep
    when(mockResource.get()).thenReturn(null);
    // 503 is otherwise retried immediately, but a server-requested 1s Retry-After
    // should still be honored before the next attempt
    KubernetesClientException serviceUnavailable =
        new KubernetesClientException(
            "Service unavailable",
            503,
            new StatusBuilder()
                .withCode(503)
                .withNewDetails()
                .withRetryAfterSeconds(1)
                .endDetails()
                .build());
    // 1st CREATE -> 503; 2nd CREATE -> success
    when(mockResource.create()).thenThrow(serviceUnavailable).thenReturn(pod);

    long start = System.nanoTime();
    Optional<Pod> result = ReconcilerUtils.getOrCreateSecondaryResource(mockClient, pod);
    long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

    assertTrue(result.isPresent());
    assertTrue(elapsedMillis >= 1000L, "should have slept for the server-requested 1s");
  }

  @Test
  void returnsExistingResourceWithoutCreating() {
    Pod pod = buildPod();
    KubernetesClient mockClient = mock(KubernetesClient.class);
    NamespaceableResource<Pod> mockResource = mockClientReturning(mockClient, pod);
    when(mockResource.get()).thenReturn(pod);

    Optional<Pod> result = ReconcilerUtils.getOrCreateSecondaryResource(mockClient, pod);

    assertTrue(result.isPresent());
    verify(mockResource, never()).create();
  }

  @ParameterizedTest
  @ValueSource(ints = {401, 403, 422})
  void propagatesRefusedInitialReadInsteadOfReportingMissingResource(int errorCode) {
    Pod pod = buildPod();
    KubernetesClient mockClient = mock(KubernetesClient.class);
    NamespaceableResource<Pod> mockResource = mockClientReturning(mockClient, pod);
    when(mockResource.get())
        .thenThrow(new KubernetesClientException("Read failed", errorCode, null));

    KubernetesClientException e =
        assertThrows(
            KubernetesClientException.class,
            () -> ReconcilerUtils.getOrCreateSecondaryResource(mockClient, pod));

    assertEquals(errorCode, e.getCode());
    // the resource state is unknown, so it must not be created as if it were missing
    verify(mockResource, never()).create();
  }

  @ParameterizedTest
  @ValueSource(ints = {408, 429, 500, 502, 503, 504})
  void createsResourceWhenInitialReadFailsRetriably(int errorCode) {
    Pod pod = buildPod();
    KubernetesClient mockClient = mock(KubernetesClient.class);
    NamespaceableResource<Pod> mockResource = mockClientReturning(mockClient, pod);
    when(mockResource.get())
        .thenThrow(new KubernetesClientException("Read failed", errorCode, null));
    when(mockResource.create()).thenReturn(pod);

    // the create path re-reads on an AlreadyExists conflict, so it resolves the actual state
    Optional<Pod> result = ReconcilerUtils.getOrCreateSecondaryResource(mockClient, pod);

    assertTrue(result.isPresent());
  }

  @Test
  void createsResourceWhenInitialReadFailsInTransport() {
    Pod pod = buildPod();
    KubernetesClient mockClient = mock(KubernetesClient.class);
    NamespaceableResource<Pod> mockResource = mockClientReturning(mockClient, pod);
    // a broken connection carries no response code, so only its cause marks it as retriable
    when(mockResource.get())
        .thenThrow(new KubernetesClientException("Read failed", new SocketException()));
    when(mockResource.create()).thenReturn(pod);

    Optional<Pod> result = ReconcilerUtils.getOrCreateSecondaryResource(mockClient, pod);

    assertTrue(result.isPresent());
  }

  @Test
  void propagatesClientSideRejectionOfInitialReadInsteadOfReportingMissingResource() {
    Pod pod = buildPod();
    KubernetesClient mockClient = mock(KubernetesClient.class);
    NamespaceableResource<Pod> mockResource = mockClientReturning(mockClient, pod);
    // the client rejected the read before sending it, which shares the absent response code of a
    // broken connection but says nothing about whether the resource exists
    when(mockResource.get())
        .thenThrow(new KubernetesClientException("resourceVersion cannot be null"));

    assertThrows(
        KubernetesClientException.class,
        () -> ReconcilerUtils.getOrCreateSecondaryResource(mockClient, pod));

    verify(mockResource, never()).create();
  }

  @Test
  void createsResourceWhenInitialReadReportsNotFound() {
    Pod pod = buildPod();
    KubernetesClient mockClient = mock(KubernetesClient.class);
    NamespaceableResource<Pod> mockResource = mockClientReturning(mockClient, pod);
    when(mockResource.get()).thenThrow(new KubernetesClientException("Not found", 404, null));
    when(mockResource.create()).thenReturn(pod);

    Optional<Pod> result = ReconcilerUtils.getOrCreateSecondaryResource(mockClient, pod);

    assertTrue(result.isPresent());
  }

  @Test
  void keepsRetryPathReadLenientOnFailure() {
    Pod pod = buildPod();
    KubernetesClient mockClient = mock(KubernetesClient.class);
    NamespaceableResource<Pod> mockResource = mockClientReturning(mockClient, pod);
    // 1st GET -> not found; GET after the failed create -> refused, which only means retry.
    // The code has to be one the strict read rethrows, or the lenient read is never exercised.
    when(mockResource.get())
        .thenReturn(null)
        .thenThrow(new KubernetesClientException("Forbidden", 403, null));
    // 1st CREATE -> transient failure; 2nd CREATE -> success
    when(mockResource.create())
        .thenThrow(new KubernetesClientException("Service unavailable", 503, null))
        .thenReturn(pod);

    Optional<Pod> result = ReconcilerUtils.getOrCreateSecondaryResource(mockClient, pod);

    assertTrue(result.isPresent());
  }

  @Test
  void lenientReadReportsRefusedReadAsAbsent() {
    Pod pod = buildPod();
    KubernetesClient mockClient = mock(KubernetesClient.class);
    NamespaceableResource<Pod> mockResource = mockClientReturning(mockClient, pod);
    when(mockResource.get()).thenThrow(new KubernetesClientException("Forbidden", 403, null));

    assertTrue(ReconcilerUtils.getResource(mockClient, pod).isEmpty());
  }
}
