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

import java.util.List;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;

import org.apache.spark.kubernetes.operator.spec.ApplicationSpec;

import static org.apache.spark.kubernetes.operator.utils.ModelUtils.isDriverMainContainer;

public class PodUtils {

  public static boolean isPodReady(final Pod pod) {
    if (pod == null || pod.getStatus() == null
        || pod.getStatus().getConditions() == null
        || pod.getStatus().getConditions().isEmpty()) {
      return false;
    }
    return pod.getStatus().getConditions().parallelStream()
        .anyMatch(condition -> "ready".equalsIgnoreCase(condition.getType())
            && "true".equalsIgnoreCase(condition.getStatus()));
  }

  public static boolean isPodStarted(final Pod driver,
                                     final ApplicationSpec spec) {
    // Consider pod as 'started' if any of Spark container is started and ready
    if (driver == null || driver.getStatus() == null
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

    return containerStatusList
        .stream()
        .filter(c -> isDriverMainContainer(spec, c.getName()))
        .anyMatch(ContainerStatus::getReady);
  }

  public static boolean isContainerExited(final ContainerStatus containerStatus) {
    return containerStatus != null
        && containerStatus.getState() != null
        && containerStatus.getState().getTerminated() != null;
  }

  public static boolean isContainerRestarted(final ContainerStatus containerStatus) {
    return containerStatus != null
        && containerStatus.getRestartCount() > 0;
  }

  public static boolean isContainerFailed(final ContainerStatus containerStatus) {
    return isContainerExited(containerStatus)
        && containerStatus.getState().getTerminated().getExitCode() > 0;
  }

}
