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

package org.apache.spark.kubernetes.operator.metrics;

import java.util.Properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.spark.kubernetes.operator.metrics.MetricsSystemFactory.parseSinkProperties;
import static org.junit.Assert.assertThrows;

class MetricsSystemFactoryTest {

  @Test
  void testMetricsSystemFailFastWithNoClassFullName() {
    Properties properties = new Properties();
    properties.put("sink.mocksink.period", "10");
    properties.put("sink.console.class", "org.apache.spark.metrics.sink.ConsoleSink");
    RuntimeException e =
        assertThrows(RuntimeException.class, () -> parseSinkProperties(properties));
    Assertions.assertEquals(
        "mocksink provides properties, but does not provide full class name",
        e.getMessage());
  }

  @Test
  void testMetricsSystemFailFastWithNotFoundClassName() {
    Properties properties = new Properties();
    properties.put("sink.console.class", "org.apache.spark.metrics.sink.FooSink");
    RuntimeException e =
        assertThrows(RuntimeException.class, () -> parseSinkProperties(properties));
    Assertions.assertEquals("Fail to find class org.apache.spark.metrics.sink.FooSink",
        e.getMessage());
  }
}
