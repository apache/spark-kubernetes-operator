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

import com.codahale.metrics.Clock;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

/** A Timer which also tracks the sum of all recorded durations, see {@link SummingHistogram}. */
public class SummingTimer extends Timer {
  private final SummingHistogram histogram;

  /** Constructs a new SummingTimer with an exponentially decaying reservoir, the default. */
  public SummingTimer() {
    this(new SummingHistogram());
  }

  /**
   * Constructs a timer recording the durations into the given histogram, for tests.
   *
   * @param histogram The histogram to record the durations in nanoseconds.
   */
  SummingTimer(SummingHistogram histogram) {
    super(new Meter(), histogram, Clock.defaultClock());
    this.histogram = histogram;
  }

  /**
   * Returns the sum of all recorded durations.
   *
   * @return The sum of all recorded durations in nanoseconds.
   */
  public long getSum() {
    return histogram.getSum();
  }
}
