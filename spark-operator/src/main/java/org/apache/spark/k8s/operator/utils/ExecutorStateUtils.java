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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import io.fabric8.kubernetes.api.model.Pod;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkAppSubmissionWorker;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.status.ExecutorState;

/**
 * Infers the state of each executor of a Spark application from its pods, for its {@code
 * status.executorState}: each executor pod of the current attempt is recorded with the state of its
 * phase, and an executor which was recorded is kept after its pod is gone. The number of recorded
 * executors is bounded by evicting the oldest executors which are gone first.
 */
@Slf4j
public final class ExecutorStateUtils {

  /** The Spark configuration which sets the application id, see {@link #currentAttemptAppId}. */
  private static final String SPARK_APP_ID_CONF = "spark.app.id";

  /** The states in which the driver of the current attempt is running. */
  private static final Set<ApplicationStateSummary> DRIVER_RUNNING_STATES =
      EnumSet.of(
          ApplicationStateSummary.DriverStarted,
          ApplicationStateSummary.DriverReady,
          ApplicationStateSummary.InitializedBelowThresholdExecutors,
          ApplicationStateSummary.RunningHealthy,
          ApplicationStateSummary.RunningWithPartialCapacity,
          ApplicationStateSummary.RunningWithBelowThresholdExecutors);

  private ExecutorStateUtils() {}

  /**
   * Returns the state of an executor from the phase of its pod.
   *
   * @param pod The executor pod.
   * @return PENDING, RUNNING, COMPLETED or FAILED for the phases Pending, Running, Succeeded and
   *     Failed, and UNKNOWN for any other phase.
   */
  public static ExecutorState fromPodPhase(final Pod pod) {
    String phase = pod.getStatus() == null ? null : pod.getStatus().getPhase();
    if (phase == null) {
      return ExecutorState.UNKNOWN;
    }
    return switch (phase) {
      case "Pending" -> ExecutorState.PENDING;
      case "Running" -> ExecutorState.RUNNING;
      case "Succeeded" -> ExecutorState.COMPLETED;
      case "Failed" -> ExecutorState.FAILED;
      default -> ExecutorState.UNKNOWN;
    };
  }

  /**
   * Returns the executor states of the application after observing its executor pods, without any
   * deleted executor pods, see {@link #observe(SparkApplication, Collection, Collection, int)}.
   *
   * @param app The Spark application, with the executor states recorded so far.
   * @param executorPods The executor pods of the application, of any attempt.
   * @param maxTrackedExecutorPerApp The maximum number of executors to record, or a non-positive
   *     number to record every executor.
   * @return The executor states by executor pod name.
   */
  public static SortedMap<String, ExecutorState> observe(
      final SparkApplication app,
      final Collection<Pod> executorPods,
      final int maxTrackedExecutorPerApp) {
    return observe(app, executorPods, List.of(), maxTrackedExecutorPerApp);
  }

  /**
   * Returns the executor states of the application after observing its executor pods, and the
   * executor pods which were deleted since the executor states were last recorded.
   *
   * <ul>
   *   <li>A deleted executor pod of the current attempt, see {@link #currentAttemptAppId}, which
   *       completed or failed is recorded as COMPLETED or FAILED, whatever was recorded for it.
   *   <li>An executor pod of the current attempt is recorded with the state of its phase.
   *   <li>A recorded executor which has not terminated and whose pod is gone is kept as UNKNOWN
   *       while the driver is running, as COMPLETED after the application succeeded, and as
   *       FAILED otherwise.
   *   <li>Any other recorded executor is kept as it is.
   *   <li>The executor states are then bounded to maxTrackedExecutorPerApp entries, see {@link
   *       #enforceCap}.
   * </ul>
   *
   * @param app The Spark application, with the executor states recorded so far.
   * @param executorPods The executor pods of the application, of any attempt.
   * @param deletedExecutors The final states of the executor pods of the application which were
   *     deleted, of any attempt, see {@link DeletedExecutorPods}.
   * @param maxTrackedExecutorPerApp The maximum number of executors to record, or a non-positive
   *     number to record every executor.
   * @return The executor states by executor pod name.
   */
  public static SortedMap<String, ExecutorState> observe(
      final SparkApplication app,
      final Collection<Pod> executorPods,
      final Collection<DeletedExecutorPods.DeletedExecutor> deletedExecutors,
      final int maxTrackedExecutorPerApp) {
    ApplicationStatus status = app.getStatus();
    SortedMap<String, ExecutorState> executorState = new TreeMap<>(status.getExecutorState());
    String appId = currentAttemptAppId(app);
    Set<String> observed = new HashSet<>();
    for (DeletedExecutorPods.DeletedExecutor deleted : deletedExecutors) {
      if (appId.equals(deleted.sparkAppId())) {
        executorState.put(deleted.podName(), deleted.state());
        observed.add(deleted.podName());
      }
    }
    for (Pod pod : executorPods) {
      if (!appId.equals(pod.getMetadata().getLabels().get(Constants.LABEL_SPARK_APP_ID))) {
        continue;
      }
      String name = pod.getMetadata().getName();
      executorState.put(name, fromPodPhase(pod));
      observed.add(name);
    }
    ApplicationStateSummary currentState = status.getCurrentState().getCurrentStateSummary();
    for (Map.Entry<String, ExecutorState> entry : status.getExecutorState().entrySet()) {
      if (entry.getValue().isTerminated() || observed.contains(entry.getKey())) {
        continue;
      }
      if (DRIVER_RUNNING_STATES.contains(currentState)) {
        executorState.put(entry.getKey(), ExecutorState.UNKNOWN);
      } else if (hasSucceeded(status)) {
        executorState.put(entry.getKey(), ExecutorState.COMPLETED);
      } else {
        executorState.put(entry.getKey(), ExecutorState.FAILED);
      }
    }
    enforceCap(app, executorState, maxTrackedExecutorPerApp);
    return executorState;
  }

  /**
   * Bounds the executor states to maxTrackedExecutorPerApp entries. The executors which are gone,
   * i.e. COMPLETED, FAILED or UNKNOWN, are evicted first, oldest first. If the executors which are
   * still pending or running exceed the limit by themselves, the newest of them are evicted, with a
   * warning. Spark assigns increasing executor ids within an application, so a lower id is an older
   * executor. Ties, including names without an id, are broken by name, so that the same executors
   * are evicted in each reconciliation.
   *
   * @param app The Spark application, for the warning.
   * @param executorState The executor states to bound, in place.
   * @param maxTrackedExecutorPerApp The maximum number of entries, or a non-positive number for no
   *     limit.
   */
  static void enforceCap(
      final SparkApplication app,
      final SortedMap<String, ExecutorState> executorState,
      final int maxTrackedExecutorPerApp) {
    if (maxTrackedExecutorPerApp <= 0 || executorState.size() <= maxTrackedExecutorPerApp) {
      return;
    }
    Comparator<String> oldestFirst =
        Comparator.<String>comparingLong(ExecutorStateUtils::executorIdFromPodName)
            .thenComparing(Comparator.<String>naturalOrder());
    List<String> gone = new ArrayList<>();
    List<String> live = new ArrayList<>();
    for (Map.Entry<String, ExecutorState> entry : executorState.entrySet()) {
      if (entry.getValue().isTerminated() || ExecutorState.UNKNOWN == entry.getValue()) {
        gone.add(entry.getKey());
      } else {
        live.add(entry.getKey());
      }
    }
    gone.sort(oldestFirst);
    for (String name : gone) {
      if (executorState.size() <= maxTrackedExecutorPerApp) {
        return;
      }
      executorState.remove(name);
    }
    if (executorState.size() <= maxTrackedExecutorPerApp) {
      return;
    }
    live.sort(
        Comparator.<String>comparingLong(ExecutorStateUtils::executorIdFromPodName)
            .reversed()
            .thenComparing(Comparator.<String>naturalOrder()));
    int dropped = 0;
    for (String name : live) {
      if (executorState.size() <= maxTrackedExecutorPerApp) {
        break;
      }
      executorState.remove(name);
      dropped++;
    }
    if (dropped > 0) {
      log.warn(
          "Dropped {} pending or running executors of {}/{} from status.executorState, since its "
              + "limit of {} is taken by pending or running executors",
          dropped,
          app.getMetadata().getNamespace(),
          app.getMetadata().getName(),
          maxTrackedExecutorPerApp);
    }
  }

  /**
   * Returns the number of active executors of the current attempt, for its {@code
   * status.activeExecutors}: its executor pods which are pending, including those held by a
   * scheduling gate, or running, including those being deleted. It is counted from the pods, so it
   * does not depend on the bound of the executor states.
   *
   * @param app The Spark application.
   * @param executorPods The executor pods of the application, of any attempt.
   * @return The number of active executors of the current attempt.
   */
  public static int countActiveExecutors(
      final SparkApplication app, final Collection<Pod> executorPods) {
    String appId = currentAttemptAppId(app);
    int active = 0;
    for (Pod pod : executorPods) {
      if (appId.equals(pod.getMetadata().getLabels().get(Constants.LABEL_SPARK_APP_ID))) {
        ExecutorState state = fromPodPhase(pod);
        if (ExecutorState.PENDING == state || ExecutorState.RUNNING == state) {
          active++;
        }
      }
    }
    return active;
  }

  /**
   * Returns the number of ready executors of the current attempt, for its {@code
   * status.readyExecutors}: its executor pods which are running and ready, see {@link
   * PodUtils#isPodReady}, and not being deleted. Unlike the active executors, it leaves out the
   * pending executors and those being deleted, so that it tells whether the executors have started.
   *
   * @param app The Spark application.
   * @param executorPods The executor pods of the application, of any attempt.
   * @return The number of ready executors of the current attempt.
   */
  public static int countReadyExecutors(
      final SparkApplication app, final Collection<Pod> executorPods) {
    String appId = currentAttemptAppId(app);
    int ready = 0;
    for (Pod pod : executorPods) {
      if (appId.equals(pod.getMetadata().getLabels().get(Constants.LABEL_SPARK_APP_ID))
          && pod.getMetadata().getDeletionTimestamp() == null
          && PodUtils.isPodReady(pod)) {
        ready++;
      }
    }
    return ready;
  }

  /**
   * Returns the highest executor ID among the executor pods of the current attempt, from their
   * {@link Constants#LABEL_SPARK_EXECUTOR_ID} label, or the trailing digits of their name for a pod
   * without a valid label.
   *
   * @param app The Spark application.
   * @param executorPods The executor pods of the application, of any attempt.
   * @return The highest executor ID, or empty if no executor pod of the current attempt has one.
   */
  public static OptionalLong highestExecutorId(
      final SparkApplication app, final Collection<Pod> executorPods) {
    String appId = currentAttemptAppId(app);
    OptionalLong highest = OptionalLong.empty();
    for (Pod pod : executorPods) {
      if (!appId.equals(pod.getMetadata().getLabels().get(Constants.LABEL_SPARK_APP_ID))) {
        continue;
      }
      OptionalLong id = executorId(pod);
      if (id.isPresent() && (highest.isEmpty() || id.getAsLong() > highest.getAsLong())) {
        highest = id;
      }
    }
    return highest;
  }

  /**
   * Returns the executor ID of an executor pod: its {@link Constants#LABEL_SPARK_EXECUTOR_ID}
   * label, or the trailing digits of its name if the label is missing or not a positive number.
   *
   * @param pod The executor pod.
   * @return The executor ID, or empty if neither is a positive number.
   */
  static OptionalLong executorId(final Pod pod) {
    OptionalLong fromLabel =
        positiveId(pod.getMetadata().getLabels().get(Constants.LABEL_SPARK_EXECUTOR_ID));
    if (fromLabel.isPresent()) {
      return fromLabel;
    }
    long id = executorIdFromPodName(pod.getMetadata().getName());
    // 0 is a name without trailing digits, and Long.MAX_VALUE one whose digits overflow
    return id > 0 && id < Long.MAX_VALUE ? OptionalLong.of(id) : OptionalLong.empty();
  }

  private static OptionalLong positiveId(final String value) {
    if (value == null) {
      return OptionalLong.empty();
    }
    try {
      long id = Long.parseLong(value.trim());
      return id > 0 ? OptionalLong.of(id) : OptionalLong.empty();
    } catch (NumberFormatException e) {
      return OptionalLong.empty();
    }
  }

  /**
   * Returns the Spark application id of the current attempt, which Spark sets as the {@link
   * Constants#LABEL_SPARK_APP_ID} label of its executor pods. The operator generates a different id
   * for each attempt, unless the application sets {@code spark.app.id} itself.
   *
   * @param app The Spark application.
   * @return The Spark application id of the current attempt.
   */
  static String currentAttemptAppId(final SparkApplication app) {
    String configured = app.getSpec().getSparkConf().get(SPARK_APP_ID_CONF);
    if (configured == null) {
      return SparkAppSubmissionWorker.generateSparkAppId(app);
    }
    return configured;
  }

  /**
   * Returns the id of an executor from the trailing digits of its pod name, e.g. 12 for {@code
   * pi-0-exec-12}, or 0 if the name does not end with digits. The executor states are keyed by pod
   * name, and the pods of the executors which are gone cannot be read any more.
   *
   * @param podName The name of the executor pod.
   * @return The executor id, or 0.
   */
  static long executorIdFromPodName(final String podName) {
    int start = podName.length();
    while (start > 0 && isAsciiDigit(podName.charAt(start - 1))) {
      start--;
    }
    if (start == podName.length()) {
      return 0L;
    }
    try {
      return Long.parseLong(podName.substring(start));
    } catch (NumberFormatException e) {
      return Long.MAX_VALUE;
    }
  }

  private static boolean isAsciiDigit(final char c) {
    return c >= '0' && c <= '9';
  }

  /**
   * Checks whether the current attempt of the application succeeded: its latest stopping state is
   * Succeeded. This also holds once the resources of a succeeded attempt are released.
   *
   * @param status The status of the application.
   * @return True if the current attempt succeeded.
   */
  static boolean hasSucceeded(final ApplicationStatus status) {
    for (ApplicationState state : status.getStateTransitionHistory().reversed().values()) {
      if (state.getCurrentStateSummary().isStopping()) {
        return ApplicationStateSummary.Succeeded == state.getCurrentStateSummary();
      }
    }
    return false;
  }
}
