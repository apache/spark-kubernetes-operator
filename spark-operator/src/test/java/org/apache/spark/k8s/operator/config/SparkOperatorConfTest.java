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

package org.apache.spark.k8s.operator.config;

import static org.apache.spark.k8s.operator.utils.TestUtils.setConfigKey;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SparkOperatorConfTest {
  @Test
  void testOperatorRetryWithoutMaxIntervalByDefault() {
    // JOSDK GenericRetry caps the interval at GradualRetry.DEFAULT_MAX_INTERVAL unless disabled.
    Assertions.assertEquals(-1L, SparkOperatorConf.getOperatorRetry().getMaxInterval());
  }

  @Test
  void testOperatorRetryWithMaxInterval() {
    int maxIntervalSeconds = SparkOperatorConf.RECONCILER_RETRY_MAX_INTERVAL_SECONDS.getValue();
    try {
      setConfigKey(SparkOperatorConf.RECONCILER_RETRY_MAX_INTERVAL_SECONDS, 30);
      Assertions.assertEquals(30_000L, SparkOperatorConf.getOperatorRetry().getMaxInterval());
    } finally {
      setConfigKey(SparkOperatorConf.RECONCILER_RETRY_MAX_INTERVAL_SECONDS, maxIntervalSeconds);
    }
  }
}
