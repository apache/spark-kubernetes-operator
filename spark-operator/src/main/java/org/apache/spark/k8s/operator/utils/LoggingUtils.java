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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.MDC;

import org.apache.spark.k8s.operator.BaseResource;
import org.apache.spark.k8s.operator.spec.BaseSpec;
import org.apache.spark.k8s.operator.status.BaseAttemptInfo;
import org.apache.spark.k8s.operator.status.BaseAttemptSummary;
import org.apache.spark.k8s.operator.status.BaseState;
import org.apache.spark.k8s.operator.status.BaseStatus;

/** Utility class for logging. */
public class LoggingUtils {
  /** Utility class for managing MDC (Mapped Diagnostic Context) for logging. */
  public static final class TrackedMDC {
    public static final String AppAttemptIdKey = "resource.app.attemptId";
    public static final String ResourceNameKey = "resource.name";
    public static final String ResourceNamespaceKey = "resource.namespace";
    private final ReentrantLock lock = new ReentrantLock();
    private final Set<String> keys = new HashSet<>();

    /**
     * Sets the MDC (Mapped Diagnostic Context) with resource name, namespace, and
     * application-specific attempt ID if available.
     *
     * @param resource The BaseResource object (SparkApplication or SparkCluster).
     */
    public void set(final BaseResource<
            ?,
            ? extends BaseAttemptSummary<? extends BaseAttemptInfo>,
            ? extends BaseState<?>,
            ? extends BaseSpec,
            ? extends BaseStatus<?, ?, ?>> resource) {
      if (resource == null) {
        return;
      }
      try {
        lock.lock();
        if (resource.getMetadata() != null) {
          if (resource.getMetadata().getName() != null) {
            MDC.put(ResourceNameKey, resource.getMetadata().getName());
            keys.add(ResourceNameKey);
          }
          if (resource.getMetadata().getNamespace() != null) {
            MDC.put(ResourceNamespaceKey, resource.getMetadata().getNamespace());
            keys.add(ResourceNamespaceKey);
          }
        }
        if (resource.getStatus() != null) {
          BaseAttemptSummary<? extends BaseAttemptInfo> summary =
              resource.getStatus().getCurrentAttemptSummary();
          if (summary != null && summary.getAttemptInfo() != null) {
            MDC.put(AppAttemptIdKey, String.valueOf(summary.getAttemptInfo().getId()));
            keys.add(AppAttemptIdKey);
          }
        }
      } finally {
        lock.unlock();
      }
    }

    /** Resets the MDC by removing all previously set keys. */
    public void reset() {
      try {
        lock.lock();
        for (String mdcKey : keys) {
          MDC.remove(mdcKey);
        }
        keys.clear();
      } finally {
        lock.unlock();
      }
    }
  }
}
