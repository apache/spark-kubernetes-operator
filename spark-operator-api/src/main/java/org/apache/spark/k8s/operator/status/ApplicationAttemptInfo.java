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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Information about an attempt.
 *
 * <p>Maintains counters for different restart limit checks:
 * <ul>
 *   <li><b>failureRestartCounter</b>: Consecutive failure count, checked against
 *       maxRestartOnFailure</li>
 *   <li><b>schedulingFailureRestartCounter</b>: Consecutive scheduling failure count, checked
 *       against maxRestartOnSchedulingFailure</li>
 * </ul>
 *
 * @since 0.8.0
 */
@Setter
@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ApplicationAttemptInfo extends BaseAttemptInfo {
  protected long failureRestartCounter;
  protected long schedulingFailureRestartCounter;

  /**
   * The topology zone which the driver and executors of this attempt are pinned to, or null if the
   * attempt is not pinned. It is chosen once per attempt, before the driver is requested.
   *
   * @since 1.1.0
   */
  protected String zone;

  /**
   * Constructs a new ApplicationAttemptInfo with all counters set to zero.
   *
   * @since 0.8.0
   */
  public ApplicationAttemptInfo() {
    super();
    failureRestartCounter = 0L;
    schedulingFailureRestartCounter = 0L;
  }

  /**
   * Constructs a new ApplicationAttemptInfo with the given id and restart counters, which is not
   * pinned to a zone.
   *
   * @since 0.8.0
   */
  public ApplicationAttemptInfo(
      long id,
      long restartCounter,
      long failureRestartCounter,
      long schedulingFailureRestartCounter) {
    this(id, restartCounter, failureRestartCounter, schedulingFailureRestartCounter, null);
  }

  /**
   * Constructs a new ApplicationAttemptInfo with the given id, restart counters and zone.
   *
   * @since 1.1.0
   */
  @JsonCreator
  public ApplicationAttemptInfo(
      @JsonProperty("id") long id,
      @JsonProperty("restartCounter") long restartCounter,
      @JsonProperty("failureRestartCounter") long failureRestartCounter,
      @JsonProperty("schedulingFailureRestartCounter") long schedulingFailureRestartCounter,
      @JsonProperty("zone") String zone) {
    super(id, restartCounter);
    this.failureRestartCounter = failureRestartCounter;
    this.schedulingFailureRestartCounter = schedulingFailureRestartCounter;
    this.zone = zone;
  }

  /**
   * Returns a copy of this attempt info which is pinned to the given zone.
   *
   * @param pinnedZone The zone of the attempt.
   * @return A new ApplicationAttemptInfo with the same id and counters.
   * @since 1.1.0
   */
  public ApplicationAttemptInfo withZone(String pinnedZone) {
    return new ApplicationAttemptInfo(
        id, restartCounter, failureRestartCounter, schedulingFailureRestartCounter, pinnedZone);
  }
}
