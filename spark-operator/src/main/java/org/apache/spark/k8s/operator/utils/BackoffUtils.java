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

import static org.apache.spark.k8s.operator.config.SparkOperatorConf.*;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import io.fabric8.kubernetes.api.model.Status;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.client.KubernetesClientException;
import lombok.extern.slf4j.Slf4j;

/** Utility class for exponential backoff with jitter on retryable API errors. */
@Slf4j
public final class BackoffUtils {

  private BackoffUtils() {}

  /**
   * Sleeps before retrying resource creation. Honors the server-provided retryAfterSeconds
   * (surfaced from the HTTP {@code Retry-After} header by the Kubernetes API server) when
   * present, otherwise falls back to jitter exponential backoff.
   *
   * @see <a href="https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/">
   *     Exponential Backoff and Jitter</a>
   */
  public static void backoffSleep(
      KubernetesClientException e, long attemptCount, long maxAttempts) {
    long actualDelay = computeDelayMillis(e, attemptCount);
    if (getRetryAfterMillis(e) != null) {
      log.info(
          "Retrying resource creation honoring server Retry-After. "
              + "Delay: {}ms, responseCode: {}, attempt: {}/{}",
          actualDelay,
          e.getCode(),
          attemptCount,
          maxAttempts);
    } else {
      log.info(
          "Retrying resource creation with exponential backoff. "
              + "Delay: {}ms, responseCode: {}, attempt: {}/{}",
          actualDelay,
          e.getCode(),
          attemptCount,
          maxAttempts);
    }
    try {
      Thread.sleep(actualDelay);
    } catch (InterruptedException ex) {
      log.info("Backoff sleep interrupted, waking up early to retry resource creation");
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Computes the delay before the next retry, in milliseconds. Honors the server-provided
   * retryAfterSeconds when present, capped at the configured max backoff so that a server or
   * intermediate proxy cannot stall the reconciler thread for an unbounded amount of time.
   * Otherwise falls back to jitter exponential backoff.
   */
  static long computeDelayMillis(KubernetesClientException e, long attemptCount) {
    long maxBackoffMs = API_SECONDARY_RESOURCE_CREATE_MAX_BACKOFF_MILLIS.getValue();
    Long retryAfterMillis = getRetryAfterMillis(e);
    if (retryAfterMillis != null) {
      return Math.min(retryAfterMillis, maxBackoffMs);
    }
    long initialBackoffMs = API_SECONDARY_RESOURCE_CREATE_INITIAL_BACKOFF_MILLIS.getValue();
    double backoffMultiplier = API_SECONDARY_RESOURCE_CREATE_BACKOFF_MULTIPLIER.getValue();
    long jitterMs = API_SECONDARY_RESOURCE_CREATE_BACKOFF_JITTER_MILLIS.getValue();
    return computeBackoffDelay(
        initialBackoffMs, backoffMultiplier, attemptCount, maxBackoffMs, jitterMs);
  }

  /**
   * Extracts the server-requested retry delay, in milliseconds, from a Kubernetes API error
   * response. The fabric8 client does not expose the raw {@code Retry-After} HTTP header, but
   * the API server mirrors its value into {@code status.details.retryAfterSeconds} for
   * rate-limited (429) and unavailable (503) responses, so that field is used instead.
   *
   * @param e the exception raised by the Kubernetes API call.
   * @return the requested delay in milliseconds, or {@code null} if the server did not specify
   *     one.
   */
  static Long getRetryAfterMillis(KubernetesClientException e) {
    Status status = e.getStatus();
    StatusDetails details = status == null ? null : status.getDetails();
    Integer retryAfterSeconds = details == null ? null : details.getRetryAfterSeconds();
    if (retryAfterSeconds == null || retryAfterSeconds <= 0) {
      return null;
    }
    return TimeUnit.SECONDS.toMillis(retryAfterSeconds);
  }

  static long computeBackoffDelay(
      long initialBackoffMs,
      double backoffMultiplier,
      long attemptCount,
      long maxBackoffMs,
      long jitterMs) {
    long exponentialDelay =
        (long) (initialBackoffMs * Math.pow(backoffMultiplier, attemptCount - 2));
    long cappedDelay = Math.min(exponentialDelay, maxBackoffMs);
    long jitter = jitterMs > 0 ? ThreadLocalRandom.current().nextLong(0, jitterMs + 1) : 0;
    return cappedDelay + jitter;
  }
}
