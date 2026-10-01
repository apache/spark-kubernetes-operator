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

import java.util.concurrent.atomic.LongAdder;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Reservoir;

/**
 * A Histogram which also tracks the sum of all recorded values.
 *
 * <p>The snapshot of the default reservoir represents roughly the last 5 minutes, so its mean
 * multiplied by the count is not the sum of all values, and can even decrease. {@link
 * PrometheusPullModelHandler} exports this sum as the {@code _sum} of a summary.
 */
public class SummingHistogram extends Histogram {
  private final LongAdder sum = new LongAdder();

  /** Constructs a new SummingHistogram with an exponentially decaying reservoir, the default. */
  public SummingHistogram() {
    this(new ExponentiallyDecayingReservoir());
  }

  /**
   * Constructs a histogram sampling the values into the given reservoir, for tests.
   *
   * @param reservoir The reservoir to sample the recorded values.
   */
  SummingHistogram(Reservoir reservoir) {
    super(reservoir);
  }

  /**
   * Records a value and adds it to the sum.
   *
   * @param value The value to record.
   */
  @Override
  public void update(long value) {
    super.update(value);
    sum.add(value);
  }

  /**
   * Returns the sum of all recorded values.
   *
   * @return The sum of all recorded values.
   */
  public long getSum() {
    return sum.sum();
  }
}
