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

import java.util.Set;

import io.javaoperatorsdk.operator.api.event.EventRecord;
import io.javaoperatorsdk.operator.api.event.EventType;
import io.javaoperatorsdk.operator.api.event.ResourceEventRecorder;

import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.BaseStateSummary;

/**
 * Utility class for publishing Kubernetes events about Spark resources.
 *
 * <p>Whether events are published at all is decided by {@link ConfigurableEventRecorder}, which is
 * registered for the whole operator, so callers here do not read the config themselves. Writing an
 * event is best effort: {@link
 * io.javaoperatorsdk.operator.api.event.DefaultEventRecorder#record(EventRecord,
 * io.javaoperatorsdk.operator.api.reconciler.Context)} logs and swallows write failures rather than
 * failing the reconciliation.
 */
public final class EventUtils {

  /** Reason for an event describing an unhandled error thrown out of a reconciliation. */
  public static final String REASON_RECONCILE_ERROR = "ReconcileError";

  /** Reason for an event describing a failure to persist the resource status. */
  public static final String REASON_STATUS_UPDATE_FAILED = "StatusUpdateFailed";

  /** Reason for an event describing an unhandled error thrown out of a cleanup. */
  public static final String REASON_CLEANUP_ERROR = "CleanupError";

  /** Reason for an event describing that a resource waits for Kueue to admit its Workload. */
  public static final String REASON_KUEUE_ADMISSION_PENDING = "KueueAdmissionPending";

  /** Reason for an event describing that Kueue admitted the Workload of a resource. */
  public static final String REASON_KUEUE_ADMITTED = "KueueAdmitted";

  /** Reason for an event describing a failure to request the Kueue admission of a resource. */
  public static final String REASON_KUEUE_ADMISSION_REQUEST_FAILED = "KueueAdmissionRequestFailed";

  /** Maximum number of characters an event message may have, including the ellipsis. */
  static final int MAX_MESSAGE_LENGTH = 1024;

  /** Appended in place of the characters dropped from an over-long message. */
  private static final String ELLIPSIS = "...";

  /** Maximum number of links followed when looking for the innermost cause of a failure. */
  private static final int MAX_CAUSE_DEPTH = 10;

  /**
   * States that are not failures but still deserve the attention of users, because nothing else
   * reports them: {@code RunningWithBelowThresholdExecutors} persists without any timeout, and
   * {@code TerminatedWithoutReleaseResources} is not meant for production use. A state that
   * escalates to a failure on its own, such as {@code InitializedBelowThresholdExecutors} timing
   * out into {@code ExecutorsStartTimedOut}, warns through that failure instead and stays normal.
   */
  private static final Set<BaseStateSummary> NON_FAILURE_WARNING_STATES =
      Set.of(
          ApplicationStateSummary.RunningWithBelowThresholdExecutors,
          ApplicationStateSummary.TerminatedWithoutReleaseResources);

  private EventUtils() {}

  /**
   * Returns the type of the event published when a resource transitions into the given state.
   * Failure states and the states listed in {@link #NON_FAILURE_WARNING_STATES} are warnings, every
   * other state is normal.
   *
   * @param summary The state the resource has transitioned into.
   * @return {@link EventType#WARNING} or {@link EventType#NORMAL}.
   */
  public static EventType eventTypeOf(BaseStateSummary summary) {
    return summary.isFailure() || NON_FAILURE_WARNING_STATES.contains(summary)
        ? EventType.WARNING
        : EventType.NORMAL;
  }

  /**
   * Publishes a warning event about a Spark resource.
   *
   * <p>The reason doubles as the event key, so repeated warnings for the same reason on the same
   * resource collapse into a count increment on a single Event object instead of creating one
   * Event per occurrence. Messages therefore must not be part of the identity: they typically vary
   * between attempts, for instance because a {@code KubernetesClientException} message embeds the
   * request URL.
   *
   * @param recorder The event recorder bound to the resource.
   * @param reason A short CamelCase reason, as expected by Kubernetes.
   * @param message The message, truncated to {@value #MAX_MESSAGE_LENGTH} characters.
   */
  public static void warn(ResourceEventRecorder recorder, String reason, String message) {
    record(recorder, EventType.WARNING, reason, message);
  }

  /**
   * Publishes a normal event about a Spark resource. The reason doubles as the event key, see
   * {@link #warn(ResourceEventRecorder, String, String)}.
   *
   * @param recorder The event recorder bound to the resource.
   * @param reason A short CamelCase reason, as expected by Kubernetes.
   * @param message The message, truncated to {@value #MAX_MESSAGE_LENGTH} characters.
   */
  public static void normal(ResourceEventRecorder recorder, String reason, String message) {
    record(recorder, EventType.NORMAL, reason, message);
  }

  /**
   * Publishes an event of the given type about a Spark resource. The reason doubles as the event
   * key, see {@link #warn(ResourceEventRecorder, String, String)}.
   *
   * @param recorder The event recorder bound to the resource.
   * @param type The event type.
   * @param reason A short CamelCase reason, as expected by Kubernetes.
   * @param message The message, truncated to {@value #MAX_MESSAGE_LENGTH} characters.
   */
  public static void record(
      ResourceEventRecorder recorder, EventType type, String reason, String message) {
    recorder.record(
        EventRecord.builder()
            .type(type)
            .reason(reason)
            .message(truncate(message))
            .key(reason)
            .build());
  }

  /**
   * Builds a concise single-line description of an exception, suitable for an event message. The
   * innermost cause is appended when the exception has one, since that is usually where the
   * actionable detail is. The full stack trace is intentionally omitted, it belongs in the
   * operator log.
   *
   * @param throwable The exception to describe, may be null.
   * @return A description of the exception.
   */
  public static String describe(Throwable throwable) {
    if (throwable == null) {
      return "";
    }
    StringBuilder builder = new StringBuilder(describeSingle(throwable));
    Throwable rootCause = rootCauseOf(throwable);
    if (rootCause != null) {
      builder.append(", caused by: ").append(describeSingle(rootCause));
    }
    return builder.toString();
  }

  private static String describeSingle(Throwable throwable) {
    String message = throwable.getMessage();
    String type = throwable.getClass().getSimpleName();
    return StringUtils.isBlank(message) ? type : type + ": " + message;
  }

  /**
   * Returns the innermost cause of the given throwable, or null when it has none.
   *
   * @param throwable The exception to walk, must not be null.
   * @return The innermost cause found within the depth bound, or null if there is no cause.
   */
  private static Throwable rootCauseOf(Throwable throwable) {
    Throwable rootCause = throwable.getCause();
    if (rootCause == null) {
      return null;
    }
    // Bounded walk: a cyclic cause chain must not hang the reconciler. Throwable#getCause returns
    // null for a self referencing link, so only a longer cycle can come back around. Throwable
    // does not override equals, so this compares identity.
    for (int depth = 1; depth < MAX_CAUSE_DEPTH; depth++) {
      Throwable next = rootCause.getCause();
      if (next == null || next.equals(throwable)) {
        break;
      }
      rootCause = next;
    }
    return rootCause;
  }

  /**
   * Shortens a message to at most {@value #MAX_MESSAGE_LENGTH} characters, marking a shortened
   * message with a trailing ellipsis.
   *
   * @param message The message to shorten, must not be null.
   * @return The message, at most {@value #MAX_MESSAGE_LENGTH} characters long.
   */
  static String truncate(String message) {
    if (message.length() <= MAX_MESSAGE_LENGTH) {
      return message;
    }
    return message.substring(0, MAX_MESSAGE_LENGTH - ELLIPSIS.length()) + ELLIPSIS;
  }
}
