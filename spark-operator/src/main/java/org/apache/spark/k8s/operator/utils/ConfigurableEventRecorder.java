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
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.KUBERNETES_EVENTS_MIN_INTERVAL_SECONDS;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
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
 * org.apache.spark.k8s.operator.config.SparkOperatorConf#KUBERNETES_EVENTS_ENABLED} is set, drops
 * events whose reason matches one of the patterns in {@link
 * org.apache.spark.k8s.operator.config.SparkOperatorConf#KUBERNETES_EVENTS_EXCLUDED_REASONS}, and
 * drops an event repeated on the same resource with the same reason and the same message within
 * {@link
 * org.apache.spark.k8s.operator.config.SparkOperatorConf#KUBERNETES_EVENTS_MIN_INTERVAL_SECONDS}.
 *
 * <p>Registered once for the whole operator via {@link
 * io.javaoperatorsdk.operator.api.config.ConfigurationServiceOverrider#withEventRecorder}, so that
 * {@link Context#eventRecorder()} is safe to call unconditionally and no caller has to read the
 * config itself. All three options are read per emitted event rather than when the recorder is
 * bound to a context, which keeps their dynamic overrides responsive within a running
 * reconciliation.
 *
 * <p>The minimum interval starts when an event is handed to the delegate, not when it reaches the
 * API server. {@link DefaultEventRecorder#record} logs and swallows write failures, so this class
 * has no success signal to wait for, and a write that failed still holds the interval. Every
 * reason that is republished on a timer recovers on its next repeat.
 */
@Slf4j
public class ConfigurableEventRecorder implements EventRecorder {

  private final EventRecorder delegate;

  /** Monotonic time source, in nanoseconds, so that a wall clock adjustment cannot stall events. */
  private final LongSupplier nanoTime;

  /** Time at which each {@link RecordedEvent} was last recorded, see {@link #withinMinInterval}. */
  private final Map<RecordedEvent, Long> lastRecorded = new ConcurrentHashMap<>();

  /** Time at which {@link #lastRecorded} was last swept, see {@link #sweep}. */
  private final AtomicLong lastSweep;

  /**
   * Constructs a recorder that forwards to the given delegate while events are enabled.
   *
   * @param delegate The recorder that assembles and writes the events.
   */
  public ConfigurableEventRecorder(EventRecorder delegate) {
    this(delegate, System::nanoTime);
  }

  /**
   * Constructs a recorder reading the time from the given source, for tests.
   *
   * @param delegate The recorder that assembles and writes the events.
   * @param nanoTime A monotonic time source, in nanoseconds.
   */
  ConfigurableEventRecorder(EventRecorder delegate, LongSupplier nanoTime) {
    this.delegate = delegate;
    this.nanoTime = nanoTime;
    // Seeded with the current reading, since the origin of a nanosecond clock is arbitrary.
    this.lastSweep = new AtomicLong(nanoTime.getAsLong());
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
   * Records the given event, unless event publishing is disabled, its reason is excluded, or the
   * same reason and message were already recorded on the same resource within the minimum interval.
   *
   * @param event The event to record.
   * @param context The reconciliation the event is recorded from.
   */
  @Override
  public void record(EventRecord event, Context<?> context) {
    if (!eventsEnabled()) {
      // Nothing sweeps the interval state while no event is published, so it is dropped here
      // rather than left resident for the life of the process.
      discardIntervalState();
      return;
    }
    if (isExcluded(event.reason())) {
      // Only this reason is blocked, so the state of the other reasons is left alone.
      return;
    }
    if (withinMinInterval(event, uidOf(context))) {
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
    // A null or blank resolved value is treated as an empty list. The comma splitting is local
    // rather than Utils.sanitizeCommaSeparatedStrAsSet, whose "*" sentinel means "no restriction"
    // for the watched-namespaces option and would silently empty this list instead.
    String value = KUBERNETES_EVENTS_EXCLUDED_REASONS.getValue();
    return StringUtils.isNotBlank(value)
        && Arrays.stream(value.split(","))
            .map(String::trim)
            .filter(StringUtils::isNotBlank)
            .anyMatch(regex -> matches(regex, reason));
  }

  /**
   * Returns whether the given event was already recorded on the given resource less than {@link
   * org.apache.spark.k8s.operator.config.SparkOperatorConf#KUBERNETES_EVENTS_MIN_INTERVAL_SECONDS}
   * ago. The first record of a {@link RecordedEvent} always returns false, so a rare event such as
   * a failure is never delayed. The uid rather than the name identifies the resource, since a name
   * can be reused by a later resource.
   *
   * <p>The message is part of the identity because {@link DefaultEventSink} rewrites the message
   * and the timestamp of the existing Event on every repeat, so only a repeat that says exactly
   * what the last one said is free of new information. A reason whose message varies between
   * repeats, such as one embedding the cause of a failure, keeps reaching the user.
   *
   * @param event The event to record.
   * @param uid The uid of the resource the event is about, null when it cannot be determined.
   * @return Whether the event is a repeat that should be dropped.
   */
  private boolean withinMinInterval(EventRecord event, String uid) {
    long minInterval = minIntervalNanos();
    if (minInterval <= 0L) {
      // Limiting is off, so the state is dropped rather than left behind with nothing to sweep it.
      discardIntervalState();
      return false;
    }
    if (uid == null) {
      // The resource cannot be identified, so the event is published rather than attributed to
      // the wrong resource. Every event the operator publishes is about a primary resource.
      return false;
    }
    long now = nanoTime.getAsLong();
    sweep(now, minInterval);
    AtomicBoolean admitted = new AtomicBoolean();
    // compute is atomic per key, so concurrent reconcile threads cannot both admit the same event.
    lastRecorded.compute(
        new RecordedEvent(uid, event.reason(), event.message()),
        (key, previous) -> {
          if (previous == null || now - previous >= minInterval) {
            admitted.set(true);
            return now;
          }
          return previous;
        });
    return !admitted.get();
  }

  /**
   * Drops the interval state. Guarded on {@link Map#isEmpty()} because this runs on every event
   * while limiting is off, while {@link ConcurrentHashMap#clear()} walks the whole table, which the
   * map never shrinks: clearing an already empty map that once held many entries is not free.
   */
  private void discardIntervalState() {
    if (!lastRecorded.isEmpty()) {
      lastRecorded.clear();
    }
  }

  /**
   * Drops the entries older than the minimum interval, at most once per interval. An entry that old
   * admits the next event anyway, so removing it changes no decision, which is why the map can be
   * kept to the events recorded within the last interval instead of growing with every resource the
   * operator has ever seen. Sweeping inline avoids a background thread for a map that is only ever
   * touched while an event is published.
   *
   * <p>The due time is the elapsed time since the last sweep rather than a deadline computed from
   * the interval in force when that sweep ran. A deadline would keep a lowered interval waiting
   * out the old one, and would overflow for an interval large enough to saturate {@link
   * TimeUnit#toNanos}, leaving the map unswept for the life of the process.
   *
   * @param now The current time, in nanoseconds.
   * @param minInterval The minimum interval between two events, in nanoseconds.
   */
  private void sweep(long now, long minInterval) {
    long previous = lastSweep.get();
    // Only the thread that wins the compareAndSet sweeps, the others carry on recording.
    if (now - previous >= minInterval && lastSweep.compareAndSet(previous, now)) {
      lastRecorded.entrySet().removeIf(entry -> now - entry.getValue() >= minInterval);
    }
  }

  /**
   * Returns how many events are currently tracked for the minimum interval. The sweep has no
   * effect on which events are published, so only this makes it observable to a test.
   *
   * @return The number of tracked events.
   */
  int trackedEventCount() {
    return lastRecorded.size();
  }

  private static String uidOf(Context<?> context) {
    HasMetadata resource = context.getPrimaryResource();
    ObjectMeta metadata = resource == null ? null : resource.getMetadata();
    return metadata == null ? null : metadata.getUid();
  }

  private static long minIntervalNanos() {
    Long seconds = KUBERNETES_EVENTS_MIN_INTERVAL_SECONDS.getValue();
    // An unparseable override falls back to the default, but an override of the literal 'null'
    // resolves to null, which is treated as no limit rather than throwing out of a reconciliation.
    return seconds == null ? 0L : TimeUnit.SECONDS.toNanos(seconds);
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

  /** Identity of an event for the purpose of the minimum interval. */
  private record RecordedEvent(String uid, String reason, String message) {}

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
