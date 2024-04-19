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

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.FileDescriptorRatioGauge;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;

public class JVMMetricSet implements MetricSet {
  public static final String FILE_DESC_RATIO_OPEN_MAX = "fileDesc.ratio.open/max";
  private final BufferPoolMetricSet bufferPoolMetricSet;
  private final FileDescriptorRatioGauge fileDescriptorRatioGauge;
  private final GarbageCollectorMetricSet garbageCollectorMetricSet;
  private final MemoryUsageGaugeSet memoryUsageGaugeSet;
  private final ThreadStatesGaugeSet threadStatesGaugeSet;

  public JVMMetricSet() {
    bufferPoolMetricSet = new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer());
    fileDescriptorRatioGauge = new FileDescriptorRatioGauge();
    garbageCollectorMetricSet = new GarbageCollectorMetricSet();
    memoryUsageGaugeSet = new MemoryUsageGaugeSet();
    threadStatesGaugeSet = new ThreadStatesGaugeSet();
  }

  @Override
  public Map<String, Metric> getMetrics() {
    final Map<String, Metric> jvmMetrics = new HashMap<>();
    putAllMetrics(jvmMetrics, bufferPoolMetricSet, "bufferPool");
    jvmMetrics.put(FILE_DESC_RATIO_OPEN_MAX, fileDescriptorRatioGauge);
    putAllMetrics(jvmMetrics, garbageCollectorMetricSet, "gc");
    putAllMetrics(jvmMetrics, memoryUsageGaugeSet, "memoryUsage");
    putAllMetrics(jvmMetrics, threadStatesGaugeSet, "threadStates");
    return jvmMetrics;
  }

  private void putAllMetrics(final Map<String, Metric> destination, final MetricSet origin,
                             final String prefix) {
    for (Map.Entry<String, Metric> entry : origin.getMetrics().entrySet()) {
      destination.put(prefix + "." + entry.getKey(), entry.getValue());
    }
  }
}
