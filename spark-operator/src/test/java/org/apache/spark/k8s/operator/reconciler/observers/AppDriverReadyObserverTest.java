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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;

class AppDriverReadyObserverTest {
  private final AppDriverReadyObserver observer = new AppDriverReadyObserver();

  private Optional<ApplicationState> observe(Pod driver) {
    ApplicationStatus status =
        new ApplicationStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""));
    return observer.observe(driver, new ApplicationSpec(), status);
  }

  private Pod podWithoutContainerStatuses(String phase, String reason) {
    return new PodBuilder()
        .withNewStatus()
        .withPhase(phase)
        .withReason(reason)
        .endStatus()
        .build();
  }

  @Test
  void failedDriverWithoutContainerStatusesIsFailed() {
    Optional<ApplicationState> state = observe(podWithoutContainerStatuses("Failed", null));
    assertTrue(state.isPresent());
    assertEquals(ApplicationStateSummary.Failed, state.get().getCurrentStateSummary());
  }

  @Test
  void evictedDriverWithoutContainerStatusesIsDriverEvicted() {
    Optional<ApplicationState> state = observe(podWithoutContainerStatuses("Failed", "Evicted"));
    assertTrue(state.isPresent());
    assertEquals(ApplicationStateSummary.DriverEvicted, state.get().getCurrentStateSummary());
  }

  @Test
  void succeededDriverWithoutContainerStatusesIsSucceeded() {
    Optional<ApplicationState> state = observe(podWithoutContainerStatuses("Succeeded", null));
    assertTrue(state.isPresent());
    assertEquals(ApplicationStateSummary.Succeeded, state.get().getCurrentStateSummary());
  }

  @Test
  void pendingDriverWithoutContainerStatusesIsNotTerminated() {
    assertTrue(observe(podWithoutContainerStatuses("Pending", null)).isEmpty());
  }
}
