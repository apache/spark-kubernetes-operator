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

package org.apache.spark.kubernetes.operator.utils;

import io.fabric8.kubernetes.api.model.Pod;

public enum PodPhase {
  // hope this is provided by k8s client in future
  PENDING("pending"),
  RUNNING("running"),
  FAILED("failed"),
  SUCCEEDED("succeeded"),
  TERMINATING("terminating"),
  UNKNOWN("unknown");

  private final String description;

  PodPhase(String description) {
    this.description = description;
  }

  public static PodPhase getPhase(final Pod pod) {
    if (pod != null && pod.getStatus() != null) {
      for (PodPhase podPhase : values()) {
        if (podPhase.description.equalsIgnoreCase(pod.getStatus().getPhase())) {
          return podPhase;
        }
      }
    }
    return UNKNOWN;
  }
}

