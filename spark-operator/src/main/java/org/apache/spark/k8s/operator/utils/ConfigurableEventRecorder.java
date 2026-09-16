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
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.KUBERNETES_EVENTS_EXCLUDED_REASONS;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.event.DefaultEventRecorder;
import io.javaoperatorsdk.operator.api.event.DefaultEventSink;
import io.javaoperatorsdk.operator.api.event.EventRecord;
import io.javaoperatorsdk.operator.api.event.EventRecorder;
import io.javaoperatorsdk.operator.api.event.ResourceEventRecorder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import lombok.extern.slf4j.Slf4j;

/**
 * An {@link EventRecorder} that drops every event unless {@link
 * org.apache.spark.k8s.operator.config.SparkOperatorConf#KUBERNETES_EVENTS_ENABLED} is set, and
 * drops events whose reason matches one of the patterns in {@link
 * org.apache.spark.k8s.operator.config.SparkOperatorConf#KUBERNETES_EVENTS_EXCLUDED_REASONS}.
 *
 * <p>Registered once for the whole operator via {@link
 * io.javaoperatorsdk.operator.api.config.ConfigurationServiceOverrider#withEventRecorder}, so that
 * {@link Context#eventRecorder()} is safe to call unconditionally and no caller has to read the
 * config itself. Both options are read per emitted event rather than when the recorder is bound to
 * a context, which keeps their dynamic overrides responsive within a running reconciliation.
 */
@Slf4j
public class ConfigurableEventRecorder implements EventRecorder {

  private final EventRecorder delegate;

  /**
   * Constructs a recorder that forwards to the given delegate while events are enabled.
   *
   * @param delegate The recorder that assembles and writes the events.
   */
  public ConfigurableEventRecorder(EventRecorder delegate) {
    this.delegate = delegate;
  }

  /**
   * Constructs a recorder over the JOSDK defaults, writing events with the given client.
   *
   * @param client The Kubernetes client used to write events.
   * @return A recorder gated on the events config option.
   */
  public static ConfigurableEventRecorder withDefaultSink(KubernetesClient client) {
    return new ConfigurableEventRecorder(new DefaultEventRecorder(new DefaultEventSink(client)));
  }

  /**
   * Records the given event, unless event publishing is disabled or its reason is excluded.
   *
   * @param event The event to record.
   * @param context The reconciliation the event is recorded from.
   */
  @Override
  public void record(EventRecord event, Context<?> context) {
    if (!eventsEnabled() || isExcluded(event.reason())) {
      return;
    }
    delegate.record(event, context);
  }

  /**
   * Returns a recorder bound to the given context. Binding is always allowed, the enabled flag is
   * consulted when an event is actually recorded.
   *
   * @param context The reconciliation to bind to.
   * @return A recorder bound to the primary resource of the given context.
   */
  @Override
  public ResourceEventRecorder forContext(Context<?> context) {
    return new BoundRecorder(this, context);
  }

  private static boolean eventsEnabled() {
    // Boolean.TRUE.equals guards against a null resolved value, which the option can yield for a
    // malformed override. Publishing events must never break a reconciliation.
    return Boolean.TRUE.equals(KUBERNETES_EVENTS_ENABLED.getValue());
  }

  private static boolean isExcluded(String reason) {
    // A null resolved value is treated as an empty list.
    return Utils.sanitizeCommaSeparatedStrAsSet(KUBERNETES_EVENTS_EXCLUDED_REASONS.getValue())
        .stream()
        .anyMatch(regex -> matches(regex, reason));
  }

  private static boolean matches(String regex, String reason) {
    try {
      return Pattern.matches(regex, reason);
    } catch (PatternSyntaxException e) {
      // A malformed pattern must not break a reconciliation, so it only matches literally.
      log.warn(
          "Invalid regex in {}: {}", KUBERNETES_EVENTS_EXCLUDED_REASONS.getKey(), e.getMessage());
      return regex.equals(reason);
    }
  }

  private record BoundRecorder(EventRecorder delegate, Context<?> context)
      implements ResourceEventRecorder {

    @Override
    public void normal(String reason, String message) {
      record(EventRecord.normal(reason, message));
    }

    @Override
    public void warn(String reason, String message) {
      record(EventRecord.warning(reason, message));
    }

    @Override
    public void record(EventRecord event) {
      delegate.record(event, context);
    }
  }
}
