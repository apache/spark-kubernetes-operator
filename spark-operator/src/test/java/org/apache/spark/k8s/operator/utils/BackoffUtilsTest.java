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

import static org.apache.spark.k8s.operator.config.SparkOperatorConf.API_SECONDARY_RESOURCE_CREATE_BACKOFF_JITTER_MILLIS;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.API_SECONDARY_RESOURCE_CREATE_MAX_BACKOFF_MILLIS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.fabric8.kubernetes.api.model.StatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import org.junit.jupiter.api.Test;

class BackoffUtilsTest {

  @Test
  void retryAfterMillisReturnsValueFromStatusDetails() {
    KubernetesClientException e =
        new KubernetesClientException(
            "Too Many Requests",
            429,
            new StatusBuilder()
                .withCode(429)
                .withNewDetails()
                .withRetryAfterSeconds(5)
                .endDetails()
                .build());

    assertEquals(5000L, BackoffUtils.getRetryAfterMillis(e));
  }

  @Test
  void retryAfterMillisReturnsNullWhenAbsent() {
    KubernetesClientException e =
        new KubernetesClientException(
            "Too Many Requests", 429, new StatusBuilder().withCode(429).build());

    assertNull(BackoffUtils.getRetryAfterMillis(e));
  }

  @Test
  void retryAfterMillisReturnsNullWhenNonPositive() {
    KubernetesClientException e =
        new KubernetesClientException(
            "Too Many Requests",
            429,
            new StatusBuilder()
                .withCode(429)
                .withNewDetails()
                .withRetryAfterSeconds(0)
                .endDetails()
                .build());

    assertNull(BackoffUtils.getRetryAfterMillis(e));
  }

  @Test
  void retryAfterMillisReturnsNullWhenNoStatus() {
    KubernetesClientException e = new KubernetesClientException("Too Many Requests", 429, null);

    assertNull(BackoffUtils.getRetryAfterMillis(e));
  }

  @Test
  void computeDelayMillisCapsHugeRetryAfterAtConfiguredMaxBackoffPlusJitter() {
    // 1 hour, far beyond any sane reconciler wait
    long delay = BackoffUtils.computeDelayMillis(3_600_000L, 1);
    long maxBackoffMs = API_SECONDARY_RESOURCE_CREATE_MAX_BACKOFF_MILLIS.getValue();
    long jitterMs = API_SECONDARY_RESOURCE_CREATE_BACKOFF_JITTER_MILLIS.getValue();

    assertTrue(delay >= maxBackoffMs && delay <= maxBackoffMs + jitterMs);
  }

  @Test
  void computeDelayMillisUsesRetryAfterPlusJitterWhenBelowMaxBackoff() {
    long delay = BackoffUtils.computeDelayMillis(1000L, 1);
    long jitterMs = API_SECONDARY_RESOURCE_CREATE_BACKOFF_JITTER_MILLIS.getValue();

    assertTrue(delay >= 1000L && delay <= 1000L + jitterMs);
  }

  @Test
  void computeDelayMillisFallsBackToExponentialBackoffWhenRetryAfterAbsent() {
    long maxBackoffMs = API_SECONDARY_RESOURCE_CREATE_MAX_BACKOFF_MILLIS.getValue();
    long jitterMs = API_SECONDARY_RESOURCE_CREATE_BACKOFF_JITTER_MILLIS.getValue();

    long delay = BackoffUtils.computeDelayMillis(null, 2);

    assertTrue(delay >= 0 && delay <= maxBackoffMs + jitterMs);
  }

  @Test
  void computeBackoffDelayCapsAtMaxAndAddsJitterWithinBounds() {
    long delay = BackoffUtils.computeBackoffDelay(1000L, 2.0, 10, 40000L, 500L);
    assertTrue(delay >= 40000L && delay <= 40500L);
  }

  @Test
  void computeBackoffDelayGrowsExponentiallyBeforeCap() {
    long delayAttempt2 = BackoffUtils.computeBackoffDelay(1000L, 2.0, 2, 40000L, 0L);
    long delayAttempt3 = BackoffUtils.computeBackoffDelay(1000L, 2.0, 3, 40000L, 0L);
    assertEquals(1000L, delayAttempt2);
    assertEquals(2000L, delayAttempt3);
  }
}
