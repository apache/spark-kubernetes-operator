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

import static org.apache.spark.k8s.operator.Constants.DRIVER_FAILED_INIT_CONTAINERS_MESSAGE;
import static org.apache.spark.k8s.operator.Constants.DRIVER_FAILED_MESSAGE;
import static org.apache.spark.k8s.operator.Constants.DRIVER_RESTARTED_MESSAGE;
import static org.apache.spark.k8s.operator.Constants.DRIVER_SPARK_CONTAINER_PROP_KEY;
import static org.apache.spark.k8s.operator.Constants.DRIVER_SUCCEEDED_MESSAGE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.spec.BaseApplicationTemplateSpec;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;

class AppDriverReadyObserverTest {
  private final AppDriverReadyObserver observer = new AppDriverReadyObserver();

  private Optional<ApplicationState> observe(Pod driver) {
    return observe(driver, new ApplicationSpec());
  }

  private Optional<ApplicationState> observe(Pod driver, ApplicationSpec spec) {
    ApplicationStatus status =
        new ApplicationStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.DriverRequested, ""));
    return observer.observe(driver, spec, status);
  }

  private Pod podWithoutContainerStatuses(String phase, String reason) {
    return new PodBuilder()
        .withNewStatus()
        .withPhase(phase)
        .withReason(reason)
        .endStatus()
        .build();
  }

  private Pod runningPod(ContainerStatus driverContainer, ContainerStatus... initContainers) {
    return new PodBuilder()
        .withNewStatus()
        .withPhase("Running")
        .withInitContainerStatuses(initContainers)
        .withContainerStatuses(driverContainer)
        .endStatus()
        .build();
  }

  private Pod runningPodWithContainers(ContainerStatus... containers) {
    return new PodBuilder()
        .withNewStatus()
        .withPhase("Running")
        .withContainerStatuses(containers)
        .endStatus()
        .build();
  }

  private Pod runningPodWithNativeSidecar(
      ContainerStatus sidecar, ContainerStatus driverContainer) {
    return new PodBuilder(runningPod(driverContainer, sidecar))
        .withNewSpec()
        .addNewInitContainer()
        .withName(sidecar.getName())
        .withRestartPolicy("Always")
        .endInitContainer()
        .endSpec()
        .build();
  }

  private ApplicationSpec specWithDriverPodTemplate(String... containerNames) {
    PodTemplateSpecBuilder builder = new PodTemplateSpecBuilder();
    for (String name : containerNames) {
      builder.editOrNewSpec().addNewContainer().withName(name).endContainer().endSpec();
    }
    ApplicationSpec spec = new ApplicationSpec();
    spec.setDriverSpec(new BaseApplicationTemplateSpec(builder.build()));
    return spec;
  }

  private ContainerStatus runningContainer(String name) {
    return new ContainerStatusBuilder()
        .withName(name)
        .withRestartCount(0)
        .withNewState()
        .withNewRunning()
        .endRunning()
        .endState()
        .build();
  }

  private ContainerStatus terminatedContainer(String name, int exitCode) {
    return new ContainerStatusBuilder()
        .withName(name)
        .withNewState()
        .withNewTerminated()
        .withExitCode(exitCode)
        .endTerminated()
        .endState()
        .build();
  }

  private ContainerStatus driverContainer(int restartCount) {
    return new ContainerStatusBuilder()
        .withName("spark-kubernetes-driver")
        .withRestartCount(restartCount)
        .withNewState()
        .withNewRunning()
        .endRunning()
        .endState()
        .build();
  }

  private void assertState(
      ApplicationStateSummary expectedSummary, String expectedMessage, Pod driver) {
    assertState(expectedSummary, expectedMessage, driver, new ApplicationSpec());
  }

  private void assertState(
      ApplicationStateSummary expectedSummary,
      String expectedMessage,
      Pod driver,
      ApplicationSpec spec) {
    Optional<ApplicationState> state = observe(driver, spec);
    assertTrue(state.isPresent());
    assertEquals(expectedSummary, state.get().getCurrentStateSummary());
    assertEquals(expectedMessage, state.get().getMessage());
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

  @Test
  void terminatedDriverContainerWithNonZeroExitIsFailed() {
    assertState(
        ApplicationStateSummary.Failed,
        DRIVER_FAILED_MESSAGE,
        runningPod(terminatedContainer("spark-kubernetes-driver", 1)));
  }

  @Test
  void terminatedDriverContainerWithZeroExitIsSucceeded() {
    assertState(
        ApplicationStateSummary.Succeeded,
        DRIVER_SUCCEEDED_MESSAGE,
        runningPod(terminatedContainer("spark-kubernetes-driver", 0)));
  }

  @Test
  void restartedDriverContainerIsFailed() {
    assertState(
        ApplicationStateSummary.Failed, DRIVER_RESTARTED_MESSAGE, runningPod(driverContainer(1)));
  }

  @Test
  void failedInitContainerIsFailed() {
    assertState(
        ApplicationStateSummary.Failed,
        DRIVER_FAILED_INIT_CONTAINERS_MESSAGE,
        runningPod(driverContainer(0), terminatedContainer("init", 1)));
  }

  @Test
  void runningDriverContainerIsNotTerminated() {
    assertTrue(observe(runningPod(driverContainer(0))).isEmpty());
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1})
  void terminatedSidecarIsNotDriverTermination(int exitCode) {
    assertTrue(
        observe(
                runningPodWithContainers(
                    terminatedContainer("sidecar", exitCode), driverContainer(0)))
            .isEmpty());
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1})
  void terminatedSidecarWithUnusedPodTemplateContainerNameIsNotDriverTermination(int exitCode) {
    ApplicationSpec spec = new ApplicationSpec();
    spec.getSparkConf().put(DRIVER_SPARK_CONTAINER_PROP_KEY, "driver");
    assertTrue(
        observe(
                runningPodWithContainers(
                    terminatedContainer("sidecar", exitCode), driverContainer(0)),
                spec)
            .isEmpty());
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1})
  void terminatedSidecarOfPodTemplateIsNotDriverTermination(int exitCode) {
    ApplicationSpec spec = specWithDriverPodTemplate("main", "sidecar");
    assertTrue(
        observe(
                runningPodWithContainers(
                    terminatedContainer("sidecar", exitCode), runningContainer("main")),
                spec)
            .isEmpty());
    assertState(
        ApplicationStateSummary.Failed,
        DRIVER_FAILED_MESSAGE,
        runningPodWithContainers(runningContainer("sidecar"), terminatedContainer("main", 1)),
        spec);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1})
  void terminatedSidecarWithPodTemplateContainerNameIsNotDriverTermination(int exitCode) {
    ApplicationSpec spec = specWithDriverPodTemplate("sidecar", "main");
    spec.getSparkConf().put(DRIVER_SPARK_CONTAINER_PROP_KEY, "main");
    assertTrue(
        observe(
                runningPodWithContainers(
                    terminatedContainer("sidecar", exitCode), runningContainer("main")),
                spec)
            .isEmpty());
    assertState(
        ApplicationStateSummary.Succeeded,
        DRIVER_SUCCEEDED_MESSAGE,
        runningPodWithContainers(runningContainer("sidecar"), terminatedContainer("main", 0)),
        spec);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1})
  void terminatedNativeSidecarIsNotDriverTermination(int exitCode) {
    assertTrue(
        observe(
                runningPodWithNativeSidecar(
                    terminatedContainer("sidecar", exitCode), driverContainer(0)))
            .isEmpty());
  }

  @Test
  void nativeSidecarStoppedAfterDriverSucceededIsSucceeded() {
    assertState(
        ApplicationStateSummary.Succeeded,
        DRIVER_SUCCEEDED_MESSAGE,
        runningPodWithNativeSidecar(
            terminatedContainer("sidecar", 143),
            terminatedContainer("spark-kubernetes-driver", 0)));
  }

  @Test
  void failedInitContainerBesideNativeSidecarIsFailed() {
    Pod driver =
        new PodBuilder(
                runningPodWithNativeSidecar(runningContainer("sidecar"), driverContainer(0)))
            .editSpec()
            .addNewInitContainer()
            .withName("init")
            .endInitContainer()
            .endSpec()
            .editStatus()
            .addToInitContainerStatuses(terminatedContainer("init", 1))
            .endStatus()
            .build();
    assertState(ApplicationStateSummary.Failed, DRIVER_FAILED_INIT_CONTAINERS_MESSAGE, driver);
  }
}
