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

package org.apache.spark.k8s.operator.status;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Information about an attempt.
 *
 * <p>Maintains counters for different restart limit checks:
 * <ul>
 *   <li><b>id</b>: Unique attempt identifier, always increments</li>
 *   <li><b>restartCounter</b>: Total restart count, checked against maxRestartAttempts</li>
 *   <li><b>failureRestartCounter</b>: Consecutive failure count, checked against
 *       maxRestartOnFailure</li>
 *   <li><b>schedulingFailureRestartCounter</b>: Consecutive scheduling failure count, checked
 *       against maxRestartOnSchedulingFailure</li>
 * </ul>
 *
 */
@NoArgsConstructor
@AllArgsConstructor
@Builder
@EqualsAndHashCode
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AttemptInfo {
  @Getter @Builder.Default protected final long id = 0L;
  @Getter @Setter protected long restartCounter;
  @Getter @Setter protected long failureRestartCounter;
  @Getter @Setter protected long schedulingFailureRestartCounter;

  /**
   * Creates a new AttemptInfo object representing the next attempt.
   */
  public AttemptInfo createNextAttemptInfo(
      boolean resetRestartCounter, ApplicationStateSummary currentStateSummary) {
    long newRestartCounter = resetRestartCounter ? 1L : restartCounter + 1;
    long newFailureCounter;
    long newSchedulingFailureCounter;

    if (resetRestartCounter) {
      // Reset all counters when restart counter is reset
      newRestartCounter = 1L;
      newFailureCounter = 0L;
      newSchedulingFailureCounter = 0L;
    } else if (ApplicationStateSummary.SchedulingFailure == currentStateSummary) {
      // Scheduling failure: increment both counters
      newSchedulingFailureCounter = schedulingFailureRestartCounter + 1;
      newFailureCounter = failureRestartCounter + 1;
    } else if (currentStateSummary.isFailure()) {
      // Other failure: increment general failure counter, reset schedulingFailureRestartCounter
      newFailureCounter = failureRestartCounter + 1;
      newSchedulingFailureCounter = 0L;
    } else {
      // Success: reset all failure counters (break the failure streak)
      newFailureCounter = 0L;
      newSchedulingFailureCounter = 0L;
    }

    return new AttemptInfo(
        id + 1L,
        newRestartCounter,
        newFailureCounter,
        newSchedulingFailureCounter);
  }
}
