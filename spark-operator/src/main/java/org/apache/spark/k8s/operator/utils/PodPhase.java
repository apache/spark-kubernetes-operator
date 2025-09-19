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

package org.apache.spark.k8s.operator.utils;

import io.fabric8.kubernetes.api.model.Pod;
import lombok.extern.slf4j.Slf4j;

/**
 * Defines common (driver) pod phases that may need to be handled explicitly by the operator. For
 * pod phase definition: {@see
 * https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase}
 */
@Slf4j
public enum PodPhase {
  PENDING("pending"),
  RUNNING("running"),
  FAILED("failed"),
  SUCCEEDED("succeeded"),
  UNKNOWN("unknown");

  private final String description;

  PodPhase(String description) {
    this.description = description;
  }

  /**
   * Returns the PodPhase for a given Pod.
   *
   * @param pod The Pod object.
   * @return The corresponding PodPhase, or UNKNOWN if the phase cannot be determined.
   */
  public static PodPhase getPhase(final Pod pod) {
    if (pod == null || pod.getStatus() == null) {
      return UNKNOWN;
    }
    for (PodPhase podPhase : values()) {
      if (podPhase.description.equalsIgnoreCase(pod.getStatus().getPhase())) {
        return podPhase;
      }
    }
    if (log.isErrorEnabled()) {
      log.error("Unable to determine pod phase from status: {}", pod.getStatus());
    }
    return UNKNOWN;
  }
}
