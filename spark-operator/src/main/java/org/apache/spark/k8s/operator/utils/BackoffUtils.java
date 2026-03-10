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

import lombok.extern.slf4j.Slf4j;

/** Utility class for exponential backoff with jitter on retryable API errors. */
@Slf4j
public final class BackoffUtils {

  private BackoffUtils() {}

  /**
   * Sleeps with jitter exponential backoff before retrying resource creation.
   *
   * @see <a href="https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/">
   *     Exponential Backoff and Jitter</a>
   */
  public static void backoffSleep(int responseCode, long attemptCount, long maxAttempts) {
    long initialBackoffMs = API_SECONDARY_RESOURCE_CREATE_INITIAL_BACKOFF_MILLIS.getValue();
    long maxBackoffMs = API_SECONDARY_RESOURCE_CREATE_MAX_BACKOFF_MILLIS.getValue();
    double backoffMultiplier = API_SECONDARY_RESOURCE_CREATE_BACKOFF_MULTIPLIER.getValue();
    long jitterMs = API_SECONDARY_RESOURCE_CREATE_BACKOFF_JITTER_MILLIS.getValue();
    long actualDelay =
        computeBackoffDelay(
            initialBackoffMs, backoffMultiplier, attemptCount, maxBackoffMs, jitterMs);
    log.info(
        "Retrying resource creation with exponential backoff. "
            + "Delay: {}ms, responseCode: {}, attempt: {}/{}",
        actualDelay,
        responseCode,
        attemptCount,
        maxAttempts);
    try {
      Thread.sleep(actualDelay);
    } catch (InterruptedException ex) {
      log.info("Backoff sleep interrupted, waking up early to retry resource creation");
      Thread.currentThread().interrupt();
    }
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
