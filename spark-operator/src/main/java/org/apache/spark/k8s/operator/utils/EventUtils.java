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

import static org.apache.spark.k8s.operator.config.SparkOperatorConf.KUBERNETES_EVENTS_ENABLED;

import java.util.function.Supplier;

import io.javaoperatorsdk.operator.api.event.ResourceEventRecorder;
import lombok.extern.slf4j.Slf4j;

/** Utility class for publishing Kubernetes events about Spark resources. */
@Slf4j
public final class EventUtils {

    /** Reason for an event describing an unhandled error thrown out of a reconciliation. */
    public static final String REASON_RECONCILE_ERROR = "ReconcileError";

    /** Reason for an event describing a failure to persist the resource status. */
    public static final String REASON_STATUS_UPDATE_FAILED = "StatusUpdateFailed";

    /** Reason for an event describing an unhandled error thrown out of a cleanup. */
    public static final String REASON_CLEANUP_ERROR = "CleanupError";

    /** Maximum number of characters kept from an event message. */
    static final int MAX_MESSAGE_LENGTH = 1024;

    /** Maximum number of links followed when looking for the innermost cause of a failure. */
    private static final int MAX_CAUSE_DEPTH = 10;

    private EventUtils() {}

    /**
     * Publishes a warning event about a Spark resource, if event publishing is enabled.
     *
     * <p>Publishing is best effort. Callers are never failed by an observability concern.
     *
     * @param recorderSupplier Supplies the event recorder bound to the resource.
     * @param reason A short CamelCase reason, as expected by Kubernetes.
     * @param message The message, truncated if it exceeds {@value #MAX_MESSAGE_LENGTH} characters.
     */
    public static void warn(
            Supplier<ResourceEventRecorder> recorderSupplier, String reason, String message) {
        if (!KUBERNETES_EVENTS_ENABLED.getValue()) {
            return;
        }
        try {
            recorderSupplier.get().warn(reason, truncate(message));
        } catch (RuntimeException e) {
            log.warn("Failed to publish {} event", reason, e);
        }
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
        // Bounded walk, a self referencing or cyclic cause chain must not hang the reconciler.
        // Throwable does not override equals, so these compare identity.
        for (int depth = 1; depth < MAX_CAUSE_DEPTH; depth++) {
            Throwable next = rootCause.getCause();
            if (next == null || next.equals(rootCause) || next.equals(throwable)) {
                break;
            }
            rootCause = next;
        }
        return rootCause;
    }

    private static String truncate(String message) {
        if (message == null) {
            return "";
        }
        return message.length() <= MAX_MESSAGE_LENGTH
                ? message
                : message.substring(0, MAX_MESSAGE_LENGTH) + "...";
    }
}
