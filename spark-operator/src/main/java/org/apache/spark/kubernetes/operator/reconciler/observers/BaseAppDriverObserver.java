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

package org.apache.spark.kubernetes.operator.reconciler.observers;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.kubernetes.operator.spec.ApplicationSpec;
import org.apache.spark.kubernetes.operator.status.ApplicationAttemptSummary;
import org.apache.spark.kubernetes.operator.status.ApplicationState;
import org.apache.spark.kubernetes.operator.status.ApplicationStateSummary;
import org.apache.spark.kubernetes.operator.status.ApplicationStatus;
import org.apache.spark.kubernetes.operator.utils.PodPhase;
import org.apache.spark.kubernetes.operator.utils.PodUtils;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.spark.kubernetes.operator.Constants.DriverCompletedMessage;
import static org.apache.spark.kubernetes.operator.Constants.DriverFailedInitContainersMessage;
import static org.apache.spark.kubernetes.operator.Constants.DriverFailedMessage;
import static org.apache.spark.kubernetes.operator.Constants.DriverRestartedMessage;
import static org.apache.spark.kubernetes.operator.Constants.DriverSucceededMessage;
import static org.apache.spark.kubernetes.operator.Constants.DriverTerminatedBeforeInitializationMessage;
import static org.apache.spark.kubernetes.operator.utils.ModelUtils.isDriverMainContainer;

/**
 * Observes driver pod status and update Application status as needed
 */
@Slf4j
public abstract class BaseAppDriverObserver extends
        BaseSecondaryResourceObserver<ApplicationStateSummary,
                ApplicationAttemptSummary, ApplicationState, ApplicationSpec,
                ApplicationStatus, Pod> {

    /**
     * Check whether the driver pod (and thus the application) has actually terminated
     * This would be determined by status of containers - and only by the containers with name
     * matches given filter
     * e.g. you can use "s -> 'true'" to evaluate all containers
     * Driver is considered as 'failed', if any init container failed, or any of the matched
     * container(s) has
     * 1. failed (isTerminated, non-zero)
     * 2. restarted
     * 3. (corner case) exited 0 without SparkContext / SparkSession initialization
     * Driver is considered as 'succeeded', if
     * 1. The pod is succeeded phase, or
     * 2. The container(s) has exited 0 after SparkContext / SparkSession initialization
     *
     * @param driverPod   the driverPod
     * @param driverReady whether SparkContext / SparkSession has ever been initialized for this
     *                    pod
     * @return the ApplicationState to be updated if pod is terminated. Returning empty if pod
     *         is still running
     */
    protected Optional<ApplicationState> observeDriverTermination(final Pod driverPod,
                                                                  final boolean driverReady,
                                                                  final ApplicationSpec spec) {
        if (driverPod.getStatus() == null
                || driverPod.getStatus().getContainerStatuses() == null
                || driverPod.getStatus().getContainerStatuses().isEmpty()) {
            log.warn("Cannot determine driver pod status, the pod may in pending state.");
            return Optional.empty();
        }

        if (PodPhase.FAILED.equals(PodPhase.getPhase(driverPod))) {
            ApplicationState applicationState = new ApplicationState(ApplicationStateSummary.FAILED,
                    DriverFailedMessage);
            if ("Evicted".equalsIgnoreCase(driverPod.getStatus().getReason())) {
                applicationState = new ApplicationState(ApplicationStateSummary.DRIVER_EVICTED,
                        DriverFailedMessage);
            }
            applicationState.setLastObservedDriverStatus(driverPod.getStatus());
            return Optional.of(applicationState);
        }

        if (PodPhase.SUCCEEDED.equals(PodPhase.getPhase(driverPod))) {
            ApplicationState state;
            if (driverReady) {
                state = new ApplicationState(ApplicationStateSummary.SUCCEEDED,
                        DriverCompletedMessage);
            } else {
                state = new ApplicationState(ApplicationStateSummary.FAILED,
                        DriverTerminatedBeforeInitializationMessage);
                state.setLastObservedDriverStatus(driverPod.getStatus());
            }
            return Optional.of(state);
        }

        List<ContainerStatus> initContainerStatusList =
                driverPod.getStatus().getInitContainerStatuses();
        if (initContainerStatusList != null
                && initContainerStatusList.parallelStream().anyMatch(PodUtils::isContainerFailed)) {
            ApplicationState applicationState = new ApplicationState(ApplicationStateSummary.FAILED,
                    DriverFailedInitContainersMessage);
            applicationState.setLastObservedDriverStatus(driverPod.getStatus());
            return Optional.of(applicationState);
        }
        List<ContainerStatus> containerStatusList = driverPod.getStatus().getContainerStatuses();
        List<ContainerStatus> terminatedCriticalContainers = containerStatusList.parallelStream()
                .filter(c -> isDriverMainContainer(spec, c.getName()))
                .filter(PodUtils::isContainerExited)
                .collect(Collectors.toList());
        if (!terminatedCriticalContainers.isEmpty()) {
            ApplicationState applicationState;
            if (terminatedCriticalContainers.parallelStream()
                    .anyMatch(PodUtils::isContainerFailed)) {
                applicationState =
                        new ApplicationState(ApplicationStateSummary.FAILED, DriverFailedMessage);
            } else {
                applicationState = new ApplicationState(ApplicationStateSummary.SUCCEEDED,
                        DriverSucceededMessage);
            }
            applicationState.setLastObservedDriverStatus(driverPod.getStatus());
            return Optional.of(applicationState);
        }
        if (containerStatusList.parallelStream()
                .filter(c -> isDriverMainContainer(spec, c.getName()))
                .anyMatch(PodUtils::isContainerRestarted)) {
            ApplicationState state =
                    new ApplicationState(ApplicationStateSummary.FAILED, DriverRestartedMessage);
            state.setLastObservedDriverStatus(driverPod.getStatus());
            return Optional.of(state);
        }
        return Optional.empty();
    }

}
