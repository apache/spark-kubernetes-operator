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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.fabric8.kubernetes.api.model.Pod;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.status.ExecutorState;

/**
 * Remembers the final state of the executor pods which were deleted, until the executor states of
 * their application are recorded. An executor pod usually completes when it is deleted, and is gone
 * from the informer cache soon after, so a reconciliation which runs after that would not see that
 * it completed or failed, and would record it as UNKNOWN. The informer delivers the last state of a
 * deleted pod with its delete event, which is recorded here.
 *
 * <p>Only the pods which completed or failed are remembered, and only in memory: after an operator
 * restart, the executors which were deleted meanwhile are inferred as before.
 */
public final class DeletedExecutorPods {
  private final ConcurrentMap<ResourceID, ConcurrentMap<String, DeletedExecutor>> byApplication =
      new ConcurrentHashMap<>();

  /**
   * The final state of an executor pod which was deleted.
   *
   * @param podName The name of the executor pod.
   * @param sparkAppId The Spark application id of the pod, see {@link
   *     Constants#LABEL_SPARK_APP_ID}.
   * @param state The state of the executor, COMPLETED or FAILED.
   */
  public record DeletedExecutor(String podName, String sparkAppId, ExecutorState state) {}

  /**
   * Remembers a deleted pod if it is an executor pod of a Spark application which completed or
   * failed. Other pods are ignored.
   *
   * @param pod The last state of the deleted pod.
   */
  public void record(final Pod pod) {
    Map<String, String> labels = pod.getMetadata().getLabels();
    if (labels == null
        || !Constants.LABEL_SPARK_ROLE_EXECUTOR_VALUE.equals(
            labels.get(Constants.LABEL_SPARK_ROLE_NAME))) {
      return;
    }
    String appName = labels.get(Constants.LABEL_SPARK_APPLICATION_NAME);
    ExecutorState state = ExecutorStateUtils.fromPodPhase(pod);
    if (appName == null || !state.isTerminated()) {
      return;
    }
    String podName = pod.getMetadata().getName();
    DeletedExecutor executor =
        new DeletedExecutor(podName, labels.get(Constants.LABEL_SPARK_APP_ID), state);
    // Atomically with remove, which drops the map of an application once it is empty
    byApplication.compute(
        new ResourceID(appName, pod.getMetadata().getNamespace()),
        (id, remembered) -> {
          ConcurrentMap<String, DeletedExecutor> executors =
              remembered == null ? new ConcurrentHashMap<>() : remembered;
          executors.put(podName, executor);
          return executors;
        });
  }

  /**
   * Returns the deleted executors of an application which are remembered.
   *
   * @param application The id of the SparkApplication.
   * @return The deleted executors, possibly empty.
   */
  public Collection<DeletedExecutor> get(final ResourceID application) {
    ConcurrentMap<String, DeletedExecutor> executors = byApplication.get(application);
    return executors == null ? List.of() : List.copyOf(executors.values());
  }

  /**
   * Forgets the given deleted executors of an application, once their states are recorded. An
   * executor which was remembered again meanwhile is kept.
   *
   * @param application The id of the SparkApplication.
   * @param executors The deleted executors to forget, as returned by {@link #get}.
   */
  public void remove(final ResourceID application, final Collection<DeletedExecutor> executors) {
    if (executors.isEmpty()) {
      return;
    }
    byApplication.computeIfPresent(
        application,
        (id, remembered) -> {
          for (DeletedExecutor executor : executors) {
            remembered.remove(executor.podName(), executor);
          }
          // No mapping is left for an application without remembered executors
          return remembered.isEmpty() ? null : remembered;
        });
  }

  /**
   * Forgets all the deleted executors of an application, e.g. when it is deleted.
   *
   * @param application The id of the SparkApplication.
   */
  public void clear(final ResourceID application) {
    byApplication.remove(application);
  }
}
