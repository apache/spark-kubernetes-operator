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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.fabric8.kubernetes.api.model.HasMetadata;

/**
 * Tracks, in memory only, how many consecutive attempts a secondary resource creation has
 * already made across reconciles, so that a backoff parked via {@link
 * SecondaryResourceBackoffException} resumes where it left off instead of restarting from the
 * first attempt. There is no delete hook to clear an entry whose owner is removed mid-backoff, so
 * a stale entry is instead dropped lazily the next time its key is looked up.
 */
final class SecondaryResourceRetryTracker {

  private static final long STALE_AFTER_NANOS = TimeUnit.MINUTES.toNanos(30);

  private static final ConcurrentHashMap<String, Entry> ATTEMPT_COUNTS = new ConcurrentHashMap<>();

  private SecondaryResourceRetryTracker() {}

  static long startAttemptCount(HasMetadata resource) {
    Entry entry = ATTEMPT_COUNTS.get(key(resource));
    if (entry == null || System.nanoTime() - entry.recordedAtNanos() > STALE_AFTER_NANOS) {
      return 1L;
    }
    return entry.attemptCount();
  }

  static void recordAttemptCount(HasMetadata resource, long attemptCount) {
    ATTEMPT_COUNTS.put(key(resource), new Entry(attemptCount, System.nanoTime()));
  }

  static void clear(HasMetadata resource) {
    ATTEMPT_COUNTS.remove(key(resource));
  }

  private static String key(HasMetadata resource) {
    return resource.getClass().getName()
        + '/'
        + resource.getMetadata().getNamespace()
        + '/'
        + resource.getMetadata().getName();
  }

  private record Entry(long attemptCount, long recordedAtNanos) {}
}
