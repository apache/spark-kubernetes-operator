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

package org.apache.spark.k8s.operator.spec;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import org.apache.spark.k8s.operator.status.ApplicationState;

/** Toleration settings for a Spark application. */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ApplicationTolerations {
  @Builder.Default protected RestartConfig restartConfig = new RestartConfig();

  @Builder.Default
  protected ApplicationTimeoutConfig applicationTimeoutConfig = new ApplicationTimeoutConfig();

  /** Determine the toleration behavior for executor / worker instances. */
  @Builder.Default protected ExecutorInstanceConfig instanceConfig = new ExecutorInstanceConfig();

  @Builder.Default protected ResourceRetainPolicy resourceRetainPolicy = ResourceRetainPolicy.Never;

  /**
   * Time-to-live in milliseconds for secondary resources of SparkApplications after termination. If
   * set to a negative value, secondary resources could be retained with the same lifecycle as the
   * application according to the retain policy.
   */
  @Builder.Default protected Long resourceRetainDurationMillis = -1L;

  /**
   * Check whether a terminated application has exceeded the resource retain duration at the
   * provided instant
   *
   * @param lastObservedState last observed state of the application
   * @return true if the app has terminated and resource retain duration is configured to a positive
   *     value and the app is not within retain duration; false otherwise.
   */
  public boolean exceedRetainDurationAtInstant(
      ApplicationState lastObservedState, Instant instant) {
    return lastObservedState != null
        && lastObservedState.getCurrentStateSummary().isTerminated()
        && resourceRetainDurationMillis > 0L
        && Instant.parse(lastObservedState.getLastTransitionTime())
            .plusMillis(resourceRetainDurationMillis)
            .isBefore(instant);
  }

  /**
   * Indicates whether the reconciler need to perform retain duration check
   *
   * @return true `resourceRetainDurationMillis` is set to non-negative value
   */
  public boolean isRetainDurationEnabled() {
    return resourceRetainDurationMillis >= 0L;
  }
}
