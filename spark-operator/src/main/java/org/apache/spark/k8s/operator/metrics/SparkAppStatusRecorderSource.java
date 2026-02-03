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

import java.time.Duration;
import java.time.Instant;

import com.codahale.metrics.MetricRegistry;
import io.fabric8.kubernetes.api.model.ObjectMeta;

import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.metrics.source.Source;

/** Metric source for recording Spark application status updates. */
public class SparkAppStatusRecorderSource extends BaseOperatorSource implements Source {

  public static final String RESOURCE_TYPE = "sparkapp";
  public static final String LATENCY_METRIC_FORMAT = "latency.from.%s.to.%s";
  public static final String DISCOVER_LATENCY_NAME = "latency.discover";

  /** Constructs a new SparkAppStatusRecorderSource. */
  public SparkAppStatusRecorderSource() {
    super(new MetricRegistry());
  }

  /**
   * Returns the name of the metric source.
   *
   * @return The name of the source.
   */
  @Override
  public String sourceName() {
    return "SparkAppStatusRecorder";
  }

  /**
   * Returns the MetricRegistry associated with this source.
   *
   * @return The MetricRegistry.
   */
  @Override
  public MetricRegistry metricRegistry() {
    return metricRegistry;
  }

  /**
   * Records the latency of a status update.
   *
   * @param status The current application status.
   * @param newState The new application state.
   */
  public void recordStatusUpdateLatency(final ObjectMeta metadata,
                                        final ApplicationStatus status,
                                        final ApplicationState newState) {
    if (status != null && status.getCurrentState() != null) {
      ApplicationState currentState = status.getCurrentState();
      Duration duration =
          Duration.between(
              Instant.parse(currentState.getLastTransitionTime()),
              Instant.parse(newState.getLastTransitionTime()));
      getTimer(
              RESOURCE_TYPE,
              String.format(
                  LATENCY_METRIC_FORMAT,
                  currentState.getCurrentStateSummary().name(),
                  newState.getCurrentStateSummary().name()))
          .update(duration);
    } else if (newState.getCurrentStateSummary() == ApplicationStateSummary.Submitted) {
      Duration discoverTime = Duration.between(
          Instant.parse(metadata.getCreationTimestamp()),
          Instant.parse(newState.getLastTransitionTime()));
      getTimer(RESOURCE_TYPE, DISCOVER_LATENCY_NAME).update(discoverTime);
    }
  }
}
