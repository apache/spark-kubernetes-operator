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

package org.apache.spark.k8s.operator.reconciler.observers;

import static org.apache.spark.k8s.operator.Constants.*;
import static org.apache.spark.k8s.operator.status.ApplicationStateSummary.Failed;
import static org.apache.spark.k8s.operator.status.ApplicationStateSummary.Succeeded;

import java.util.List;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.status.ApplicationAttemptSummary;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.utils.ModelUtils;
import org.apache.spark.k8s.operator.utils.PodPhase;
import org.apache.spark.k8s.operator.utils.PodUtils;

/** Observes driver pod status and update Application status as needed. */
@Slf4j
public abstract class BaseAppDriverObserver
    extends BaseSecondaryResourceObserver<
        ApplicationStateSummary,
        ApplicationAttemptSummary,
        ApplicationState,
        ApplicationSpec,
        ApplicationStatus,
        Pod> {

  /**
   * Check whether the driver pod (and thus the application) has actually terminated This would be
   * determined by status of containers - and only by the containers with name matches given filter
   * e.g. you can use "s -> 'true'" to evaluate all containers Driver is considered as 'failed', if
   * any init container failed, or any of the matched container(s) has
   *
   * <ul>
   *   <li>1. failed (isTerminated, non-zero)
   *   <li>2. restarted
   *   <li>3. (corner case) exited 0 without SparkContext / SparkSession initialization
   * </ul>
   *
   * <p>Driver is considered as 'succeeded', if
   *
   * <ul>
   *   <li>1. The pod is succeeded phase, or
   *   <li>2. The container(s) has exited 0 after SparkContext / SparkSession initialization
   * </ul>
   *
   * @param driverPod The driver Pod object.
   * @param driverReady A boolean indicating if the SparkContext/SparkSession was initialized.
   * @param spec The ApplicationSpec of the Spark application.
   * @return An Optional containing the new ApplicationState if the driver is terminated, otherwise
   *     empty.
   */
  protected Optional<ApplicationState> observeDriverTermination(
      final Pod driverPod, final boolean driverReady, final ApplicationSpec spec) {
    PodStatus status = driverPod.getStatus();
    if (status == null
        || status.getContainerStatuses() == null
        || status.getContainerStatuses().isEmpty()) {
      log.debug("Cannot determine driver pod status, the pod may in pending state.");
      return Optional.empty();
    }

    if (PodPhase.FAILED == PodPhase.getPhase(driverPod)) {
      ApplicationState state = new ApplicationState(Failed, DRIVER_FAILED_MESSAGE);
      if ("Evicted".equalsIgnoreCase(status.getReason())) {
        state = new ApplicationState(ApplicationStateSummary.DriverEvicted, DRIVER_FAILED_MESSAGE);
      }
      state.setLastObservedDriverStatus(status);
      return Optional.of(state);
    }

    if (PodPhase.SUCCEEDED == PodPhase.getPhase(driverPod)) {
      ApplicationState state;
      if (driverReady) {
        state = new ApplicationState(Succeeded, DRIVER_COMPLETED_MESSAGE);
      } else {
        state = new ApplicationState(Failed, DRIVER_TERMINATED_BEFORE_INITIALIZATION_MESSAGE);
        state.setLastObservedDriverStatus(status);
      }
      return Optional.of(state);
    }

    List<ContainerStatus> initContainerStatusList = status.getInitContainerStatuses();
    if (initContainerStatusList != null
        && initContainerStatusList.parallelStream().anyMatch(PodUtils::isContainerFailed)) {
      ApplicationState applicationState =
          new ApplicationState(Failed, DRIVER_FAILED_INIT_CONTAINERS_MESSAGE);
      applicationState.setLastObservedDriverStatus(status);
      return Optional.of(applicationState);
    }
    List<ContainerStatus> containerStatusList = status.getContainerStatuses();
    List<ContainerStatus> terminatedCriticalContainers =
        ModelUtils.findDriverMainContainerStatus(spec, containerStatusList).stream()
            .filter(PodUtils::isContainerTerminated)
            .toList();

    if (!terminatedCriticalContainers.isEmpty()) {
      ApplicationState state;
      if (terminatedCriticalContainers.parallelStream().anyMatch(PodUtils::isContainerFailed)) {
        state = new ApplicationState(Failed, DRIVER_FAILED_MESSAGE);
      } else {
        state = new ApplicationState(Succeeded, DRIVER_SUCCEEDED_MESSAGE);
      }
      state.setLastObservedDriverStatus(status);
      return Optional.of(state);
    }

    boolean isDriverRestarted =
        ModelUtils.findDriverMainContainerStatus(spec, containerStatusList).stream()
            .anyMatch(PodUtils::isContainerRestarted);
    if (isDriverRestarted) {
      ApplicationState state = new ApplicationState(Failed, DRIVER_RESTARTED_MESSAGE);
      state.setLastObservedDriverStatus(status);
      return Optional.of(state);
    }
    return Optional.empty();
  }
}
