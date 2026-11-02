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

package org.apache.spark.k8s.operator.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import org.junit.jupiter.api.Test;

class BaseOperatorSourceTest {

  @Test
  void testGetHistogram() {
    MetricRegistry registry = new MetricRegistry();
    BaseOperatorSource source = new BaseOperatorSource(registry);

    Histogram histogram = source.getHistogram("Foo", "Bar");
    // The histograms of custom metrics sources track the sum of their values, see SummingHistogram.
    assertEquals(SummingHistogram.class, histogram.getClass());
    assertSame(histogram, source.getHistogram("Foo", "Bar"));
    assertSame(histogram, registry.getHistograms().get("foo.bar"));
  }

  @Test
  void testGetNanosHistogram() {
    MetricRegistry registry = new MetricRegistry();
    BaseOperatorSource source = new BaseOperatorSource(registry);

    Histogram histogram = source.getNanosHistogram("Foo", "Bar");
    assertEquals(NanosHistogram.class, histogram.getClass());
    assertSame(histogram, source.getNanosHistogram("Foo", "Bar"));
    assertSame(histogram, registry.getHistograms().get("foo.bar"));
  }
}
