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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.status.ExecutorState;
import org.apache.spark.k8s.operator.utils.DeletedExecutorPods.DeletedExecutor;

class DeletedExecutorPodsTest {
  private static final ResourceID APP = new ResourceID("app", "ns");

  private static Pod pod(String name, String role, String appName, String phase) {
    PodBuilder builder =
        new PodBuilder()
            .withNewMetadata()
            .withName(name)
            .withNamespace("ns")
            .addToLabels(Constants.LABEL_SPARK_APP_ID, "app-0")
            .endMetadata()
            .withNewStatus()
            .withPhase(phase)
            .endStatus();
    if (role != null) {
      builder.editMetadata().addToLabels(Constants.LABEL_SPARK_ROLE_NAME, role).endMetadata();
    }
    if (appName != null) {
      builder
          .editMetadata()
          .addToLabels(Constants.LABEL_SPARK_APPLICATION_NAME, appName)
          .endMetadata();
    }
    return builder.build();
  }

  private static Pod executor(String name, String phase) {
    return pod(name, Constants.LABEL_SPARK_ROLE_EXECUTOR_VALUE, "app", phase);
  }

  @Test
  void recordsTheExecutorPodsWhichCompletedOrFailed() {
    DeletedExecutorPods deleted = new DeletedExecutorPods();
    deleted.record(executor("app-exec-1", "Succeeded"));
    deleted.record(executor("app-exec-2", "Failed"));
    assertEquals(
        Set.of(
            new DeletedExecutor("app-exec-1", "app-0", ExecutorState.COMPLETED),
            new DeletedExecutor("app-exec-2", "app-0", ExecutorState.FAILED)),
        Set.copyOf(deleted.get(APP)));
  }

  @Test
  void ignoresOtherPods() {
    DeletedExecutorPods deleted = new DeletedExecutorPods();
    deleted.record(executor("app-exec-1", "Running"));
    deleted.record(executor("app-exec-2", "Pending"));
    deleted.record(pod("app-driver", Constants.LABEL_SPARK_ROLE_DRIVER_VALUE, "app", "Succeeded"));
    deleted.record(pod("app-exec-3", null, "app", "Succeeded"));
    deleted.record(pod("app-exec-4", Constants.LABEL_SPARK_ROLE_EXECUTOR_VALUE, null, "Failed"));
    deleted.record(new PodBuilder().withNewMetadata().withName("bare").endMetadata().build());
    assertTrue(deleted.get(APP).isEmpty());
  }

  @Test
  void removeForgetsOnlyTheGivenExecutors() {
    DeletedExecutorPods deleted = new DeletedExecutorPods();
    deleted.record(executor("app-exec-1", "Succeeded"));
    List<DeletedExecutor> recorded = List.copyOf(deleted.get(APP));
    deleted.record(executor("app-exec-2", "Failed"));
    deleted.remove(APP, recorded);
    assertEquals(
        List.of(new DeletedExecutor("app-exec-2", "app-0", ExecutorState.FAILED)),
        List.copyOf(deleted.get(APP)));
    deleted.remove(APP, deleted.get(APP));
    assertTrue(deleted.get(APP).isEmpty());
    deleted.remove(APP, List.of());
    assertTrue(deleted.get(APP).isEmpty());
  }

  @Test
  void removeKeepsAnExecutorWhichWasRecordedAgainMeanwhile() {
    DeletedExecutorPods deleted = new DeletedExecutorPods();
    deleted.record(executor("app-exec-1", "Succeeded"));
    List<DeletedExecutor> recorded = List.copyOf(deleted.get(APP));
    // A pod of the same name, e.g. of another attempt with a configured spark.app.id, which failed
    deleted.record(executor("app-exec-1", "Failed"));
    deleted.remove(APP, recorded);
    assertEquals(
        List.of(new DeletedExecutor("app-exec-1", "app-0", ExecutorState.FAILED)),
        List.copyOf(deleted.get(APP)));
  }

  @Test
  void clearForgetsTheExecutorsOfOneApplication() {
    DeletedExecutorPods deleted = new DeletedExecutorPods();
    deleted.record(executor("app-exec-1", "Succeeded"));
    deleted.record(
        pod("other-exec-1", Constants.LABEL_SPARK_ROLE_EXECUTOR_VALUE, "other", "Succeeded"));
    deleted.clear(APP);
    assertTrue(deleted.get(APP).isEmpty());
    assertEquals(1, deleted.get(new ResourceID("other", "ns")).size());
  }
}
