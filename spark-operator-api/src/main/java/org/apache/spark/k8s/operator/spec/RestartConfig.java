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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.fabric8.generator.annotation.Default;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import org.apache.spark.k8s.operator.status.ApplicationStateSummary;

/** Restart configuration for a Spark application. */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RestartConfig {
  @Builder.Default protected RestartPolicy restartPolicy = RestartPolicy.Never;
  @Builder.Default protected Long maxRestartAttempts = 3L;
  @Builder.Default protected Long restartBackoffMillis = 30000L;

  @Default("-1")
  @Builder.Default
  protected Long restartCounterResetMillis = -1L;
  @Builder.Default protected Long maxRestartOnFailure = null;
  @Builder.Default protected Long restartBackoffMillisForFailure = null;
  @Builder.Default protected Long maxRestartOnSchedulingFailure = null;
  @Builder.Default protected Long restartBackoffMillisForSchedulingFailure = null;
  /**
   * Returns the effective restart backoff time in milliseconds based on the current application
   * state.
   */
  public long getEffectiveRestartBackoffMillis(
      ApplicationStateSummary stateSummary) {
    if (ApplicationStateSummary.SchedulingFailure == stateSummary
        && restartBackoffMillisForSchedulingFailure != null) {
      return restartBackoffMillisForSchedulingFailure;
    }
    if (stateSummary.isFailure() && restartBackoffMillisForFailure != null) {
      return restartBackoffMillisForFailure;
    }
    return restartBackoffMillis;
  }
}
