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

package org.apache.spark.kubernetes.operator.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.kubernetes.operator.SparkApplication;
import org.slf4j.MDC;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

public class LoggingUtils {
    public static final class TrackedMDC {
        public static final String NamespaceKey = "app_namespace";
        public static final String NameKey = "app_name";
        public static final String UuidKey = "app_uuid";
        public static final String GenerationKey = "app_generation";
        private final ReentrantLock lock = new ReentrantLock();
        private Set<String> keys = new HashSet<>();

        public void set(final SparkApplication application) {
            if (application != null && application.getMetadata() != null) {
                try {
                    lock.lock();
                    if (StringUtils.isNotEmpty(application.getMetadata().getNamespace())) {
                        MDC.put(NamespaceKey, application.getMetadata().getNamespace());
                        keys.add(NamespaceKey);
                    }
                    if (StringUtils.isNotEmpty(application.getMetadata().getName())) {
                        MDC.put(NameKey, application.getMetadata().getName());
                        keys.add(NameKey);
                    }
                    if (StringUtils.isNotEmpty(application.getMetadata().getUid())) {
                        MDC.put(UuidKey, application.getMetadata().getUid());
                        keys.add(UuidKey);
                    }
                    MDC.put(GenerationKey,
                            String.valueOf(application.getMetadata().getGeneration()));
                    keys.add(GenerationKey);
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
