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

import static org.apache.spark.k8s.operator.utils.ModelUtils.findDriverMainContainerStatus;

import java.util.List;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;

import org.apache.spark.k8s.operator.spec.ApplicationSpec;

public class PodUtils {

  public static final String POD_READY_CONDITION_TYPE = "ready";

  /** Determine whether given pod is up running and ready */
  public static boolean isPodReady(final Pod pod) {
    if (!PodPhase.RUNNING.equals(PodPhase.getPhase(pod))) {
      return false;
    }
    if (pod == null
        || pod.getStatus() == null
        || pod.getStatus().getConditions() == null
        || pod.getStatus().getConditions().isEmpty()) {
      return false;
    }
    return pod.getStatus().getConditions().parallelStream()
        .anyMatch(
            condition ->
                POD_READY_CONDITION_TYPE.equalsIgnoreCase(condition.getType())
                    && Boolean.parseBoolean(condition.getStatus()));
  }

  /**
   * Determine whether the driver pod is started. Driver is considered as 'started' if any of Spark
   * container is started and ready
   *
   * @param driver the driver pod
   * @param spec expected spec for the SparkApp
   */
  public static boolean isDriverPodStarted(final Pod driver, final ApplicationSpec spec) {
    // Consider pod as 'started' if any of Spark container is started and ready
    if (driver == null
        || driver.getStatus() == null
        || driver.getStatus().getContainerStatuses() == null
        || driver.getStatus().getContainerStatuses().isEmpty()) {
      return false;
    }

    List<ContainerStatus> containerStatusList = driver.getStatus().getContainerStatuses();

    // If there's only one container in given pod, evaluate it
    // Otherwise, use the provided name as filter.
    if (containerStatusList.size() == 1) {
      return containerStatusList.get(0).getReady();
    }

    return findDriverMainContainerStatus(spec, containerStatusList).stream()
        .anyMatch(ContainerStatus::getReady);
  }

  /** Returns true if the given container has terminated */
  public static boolean isContainerTerminated(final ContainerStatus containerStatus) {
    return containerStatus != null
        && containerStatus.getState() != null
        && containerStatus.getState().getTerminated() != null;
  }

  /** Returns true if the given container has ever restarted */
  public static boolean isContainerRestarted(final ContainerStatus containerStatus) {
    return containerStatus != null && containerStatus.getRestartCount() > 0;
  }

  /** Returns true if the given container has exited with non-zero status */
  public static boolean isContainerFailed(final ContainerStatus containerStatus) {
    return isContainerTerminated(containerStatus)
        && containerStatus.getState().getTerminated().getExitCode() > 0;
  }
}
