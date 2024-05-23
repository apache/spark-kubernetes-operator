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

import java.util.List;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.status.ApplicationAttemptSummary;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.utils.ModelUtils;
import org.apache.spark.k8s.operator.utils.PodPhase;
import org.apache.spark.k8s.operator.utils.PodUtils;

/** Observes driver pod status and update Application status as needed */
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
   * @param driverPod the driverPod
   * @param driverReady whether SparkContext / SparkSession has ever been initialized for this pod
   * @return the ApplicationState to be updated if pod is terminated. Returning empty if pod is
   *     still running
   */
  protected Optional<ApplicationState> observeDriverTermination(
      final Pod driverPod, final boolean driverReady, final ApplicationSpec spec) {
    if (driverPod.getStatus() == null
        || driverPod.getStatus().getContainerStatuses() == null
        || driverPod.getStatus().getContainerStatuses().isEmpty()) {
      log.warn("Cannot determine driver pod status, the pod may in pending state.");
      return Optional.empty();
    }

    if (PodPhase.FAILED.equals(PodPhase.getPhase(driverPod))) {
      ApplicationState applicationState =
          new ApplicationState(ApplicationStateSummary.Failed, Constants.DRIVER_FAILED_MESSAGE);
      if ("Evicted".equalsIgnoreCase(driverPod.getStatus().getReason())) {
        applicationState =
            new ApplicationState(
                ApplicationStateSummary.DriverEvicted, Constants.DRIVER_FAILED_MESSAGE);
      }
      applicationState.setLastObservedDriverStatus(driverPod.getStatus());
      return Optional.of(applicationState);
    }

    if (PodPhase.SUCCEEDED.equals(PodPhase.getPhase(driverPod))) {
      ApplicationState state;
      if (driverReady) {
        state =
            new ApplicationState(
                ApplicationStateSummary.Succeeded, Constants.DRIVER_COMPLETED_MESSAGE);
      } else {
        state =
            new ApplicationState(
                ApplicationStateSummary.Failed,
                Constants.DRIVER_TERMINATED_BEFORE_INITIALIZATION_MESSAGE);
        state.setLastObservedDriverStatus(driverPod.getStatus());
      }
      return Optional.of(state);
    }

    List<ContainerStatus> initContainerStatusList =
        driverPod.getStatus().getInitContainerStatuses();
    if (initContainerStatusList != null
        && initContainerStatusList.parallelStream().anyMatch(PodUtils::isContainerFailed)) {
      ApplicationState applicationState =
          new ApplicationState(
              ApplicationStateSummary.Failed, Constants.DRIVER_FAILED_INIT_CONTAINERS_MESSAGE);
      applicationState.setLastObservedDriverStatus(driverPod.getStatus());
      return Optional.of(applicationState);
    }
    List<ContainerStatus> containerStatusList = driverPod.getStatus().getContainerStatuses();
    List<ContainerStatus> terminatedCriticalContainers =
        ModelUtils.findDriverMainContainerStatus(spec, containerStatusList).stream()
            .filter(PodUtils::isContainerTerminated)
            .toList();

    if (!terminatedCriticalContainers.isEmpty()) {
      ApplicationState applicationState;
      if (terminatedCriticalContainers.parallelStream().anyMatch(PodUtils::isContainerFailed)) {
        applicationState =
            new ApplicationState(ApplicationStateSummary.Failed, Constants.DRIVER_FAILED_MESSAGE);
      } else {
        applicationState =
            new ApplicationState(
                ApplicationStateSummary.Succeeded, Constants.DRIVER_SUCCEEDED_MESSAGE);
      }
      applicationState.setLastObservedDriverStatus(driverPod.getStatus());
      return Optional.of(applicationState);
    }
    boolean driverRestarted =
        ModelUtils.findDriverMainContainerStatus(spec, containerStatusList).stream()
            .anyMatch(PodUtils::isContainerRestarted);

    if (driverRestarted) {
      ApplicationState state =
          new ApplicationState(ApplicationStateSummary.Failed, Constants.DRIVER_RESTARTED_MESSAGE);
      state.setLastObservedDriverStatus(driverPod.getStatus());
      return Optional.of(state);
    }
    return Optional.empty();
  }
}
