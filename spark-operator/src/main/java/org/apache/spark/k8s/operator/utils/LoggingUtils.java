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

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.status.ApplicationAttemptSummary;

public class LoggingUtils {
  public static final class TrackedMDC {
    public static final String AppAttemptIdKey = "resource.app.attemptId";
    private final ReentrantLock lock = new ReentrantLock();
    private final Set<String> keys = new HashSet<>();

    public void set(final SparkApplication application) {
      if (application != null && application.getStatus() != null) {
        try {
          lock.lock();
          ApplicationAttemptSummary summary = application.getStatus().getCurrentAttemptSummary();
          if (summary != null && summary.getAttemptInfo() != null) {
            MDC.put(AppAttemptIdKey, String.valueOf(summary.getAttemptInfo().getId()));
            keys.add(AppAttemptIdKey);
          }
        } finally {
          lock.unlock();
        }
      }
    }

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
