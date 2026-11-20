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

import static org.apache.spark.k8s.operator.config.SparkOperatorConf.KUEUE_ENABLED;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.ZONE_PINNING_SPARK_CONF;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.ZONE_PINNING_TOPOLOGY_KEY;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.ZONE_PINNING_ZONES;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadFactory;
import org.apache.spark.k8s.operator.status.ApplicationStatus;

/**
 * Pins each attempt of a SparkApplication to a single topology zone, so that the traffic between
 * its driver, its executors and a zone-aware remote shuffle service stays within the zone.
 *
 * <p>The zone is chosen once per attempt, before the driver is requested, and recorded in the
 * attempt info of the status, so that a resource spec which is built again for the same attempt,
 * e.g. after a failed status update, gets the same zone. A restarted or resumed attempt chooses
 * again.
 */
public final class ZonePinningUtils {

  /** The placeholder for the zone in the values of the pinned Spark configurations. */
  public static final String ZONE_PLACEHOLDER = "{zone}";

  private static final String DRIVER_NODE_SELECTOR_PREFIX =
      "spark.kubernetes.driver.node.selector.";
  private static final String EXECUTOR_NODE_SELECTOR_PREFIX =
      "spark.kubernetes.executor.node.selector.";

  // A Kubernetes label value, which a node selector requires.
  private static final Pattern LABEL_VALUE =
      Pattern.compile("([A-Za-z0-9]([-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?)");

  /** How long a zone chosen by this operator counts before the informer cache shows it. */
  static final Duration RECENT_PIN_TTL = Duration.ofSeconds(60);

  // Zones chosen by this operator by the uid of the application, counted until the cache shows
  // them, so that applications which are pinned at the same time spread over the zones.
  private static final Map<String, RecentPin> RECENT_PINS = new ConcurrentHashMap<>();

  // Takes turns among the least loaded zones.
  private static final AtomicLong TIE_BREAKER = new AtomicLong();

  private record RecentPin(String zone, Instant pinnedAt) {}

  private ZonePinningUtils() {}

  /**
   * Returns the zone which the current attempt of the application is pinned to.
   *
   * @param app The SparkApplication.
   * @return The zone, or empty if the current attempt is not pinned.
   */
  public static Optional<String> currentAttemptZone(final SparkApplication app) {
    ApplicationStatus status = app.getStatus();
    if (status == null || status.getCurrentAttemptSummary() == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(status.getCurrentAttemptSummary().getAttemptInfo().getZone())
        .filter(StringUtils::isNotEmpty);
  }

  /**
   * Returns the zones which applications without the zone annotation are pinned to.
   *
   * @return The zones in the configured order, without blanks and duplicates.
   */
  public static List<String> configuredZones() {
    return Arrays.stream(ZONE_PINNING_ZONES.getValue().split(","))
        .map(String::trim)
        .filter(StringUtils::isNotEmpty)
        .distinct()
        .toList();
  }

  /**
   * Checks whether the current attempt of the application is to be pinned to a zone. It is, unless
   * it is pinned already or queued through Kueue, which owns the placement of a queued application,
   * if it has the zone annotation or zones are configured.
   *
   * @param app The SparkApplication.
   * @return True if a zone is to be chosen for the current attempt.
   */
  public static boolean needsZone(final SparkApplication app) {
    if (currentAttemptZone(app).isPresent()) {
      return false;
    }
    if (KUEUE_ENABLED.getValue() && KueueWorkloadFactory.hasQueueName(app)) {
      return false;
    }
    return requestedZone(app).isPresent() || !configuredZones().isEmpty();
  }

  /**
   * Chooses the zone of the current attempt of the application. The zone annotation wins.
   * Otherwise, it is the configured zone with the fewest other applications which are not
   * terminated and whose current attempt is pinned to it, counting the zones which this operator
   * chose recently and the cache does not show yet. Ties are broken by taking turns, so that
   * applications which are submitted together spread over the tied zones.
   *
   * @param app The SparkApplication.
   * @param otherApps The applications known to the operator, which may include the given one.
   * @return The zone.
   * @throws IllegalArgumentException if the annotation is not a valid label value, or no zone is
   *     configured.
   */
  public static String chooseZone(
      final SparkApplication app, final Stream<SparkApplication> otherApps) {
    return chooseZone(app, otherApps, Instant.now());
  }

  static String chooseZone(
      final SparkApplication app, final Stream<SparkApplication> otherApps, final Instant now) {
    String uid = app.getMetadata().getUid();
    Optional<String> requested = requestedZone(app);
    if (requested.isPresent()) {
      String zone = requested.get();
      if (!LABEL_VALUE.matcher(zone).matches()) {
        throw new IllegalArgumentException(
            "Annotation "
                + Constants.ANNOTATION_ZONE
                + " must be a valid label value, but is: "
                + zone);
      }
      rememberPin(uid, zone, now);
      return zone;
    }
    List<String> zones = configuredZones();
    if (zones.isEmpty()) {
      throw new IllegalArgumentException(
          "No zone to pin to, set " + ZONE_PINNING_ZONES.getKey() + " or the annotation.");
    }
    Map<String, Long> pinned = new HashMap<>();
    Set<String> cachedPins = new HashSet<>();
    otherApps
        .filter(other -> !Objects.equals(uid, other.getMetadata().getUid()))
        .forEach(
            other ->
                currentAttemptZone(other)
                    .ifPresent(
                        zone -> {
                          cachedPins.add(other.getMetadata().getUid());
                          if (isActive(other)) {
                            pinned.merge(zone, 1L, Long::sum);
                          }
                        }));
    RECENT_PINS
        .entrySet()
        .removeIf(
            entry ->
                cachedPins.contains(entry.getKey())
                    || entry.getValue().pinnedAt().plus(RECENT_PIN_TTL).isBefore(now));
    RECENT_PINS.forEach(
        (otherUid, pin) -> {
          if (!Objects.equals(uid, otherUid)) {
            pinned.merge(pin.zone(), 1L, Long::sum);
          }
        });
    long fewest = zones.stream().mapToLong(zone -> pinned.getOrDefault(zone, 0L)).min().orElse(0L);
    List<String> leastLoaded =
        zones.stream().filter(zone -> pinned.getOrDefault(zone, 0L) == fewest).toList();
    int index = (int) Math.floorMod(TIE_BREAKER.getAndIncrement(), (long) leastLoaded.size());
    String zone = leastLoaded.get(index);
    rememberPin(uid, zone, now);
    return zone;
  }

  /** Forgets the zones chosen recently, for tests. */
  static void forgetRecentPins() {
    RECENT_PINS.clear();
    TIE_BREAKER.set(0);
  }

  private static void rememberPin(final String uid, final String zone, final Instant now) {
    if (uid != null) {
      RECENT_PINS.put(uid, new RecentPin(zone, now));
    }
  }

  /**
   * Returns the Spark configurations which pin the driver and the executors of the current attempt
   * to its zone: a node selector on the configured topology key for each, and the configured
   * zone-dependent configurations which the application does not set itself.
   *
   * @param app The SparkApplication.
   * @return The configurations, or an empty map if the current attempt is not pinned.
   */
  public static Map<String, String> sparkConfOverrides(final SparkApplication app) {
    Optional<String> zone = currentAttemptZone(app);
    if (zone.isEmpty()) {
      return Map.of();
    }
    Map<String, String> overrides = new HashMap<>();
    String topologyKey = ZONE_PINNING_TOPOLOGY_KEY.getValue().trim();
    overrides.put(DRIVER_NODE_SELECTOR_PREFIX + topologyKey, zone.get());
    overrides.put(EXECUTOR_NODE_SELECTOR_PREFIX + topologyKey, zone.get());
    Map<String, String> appConf = app.getSpec().getSparkConf();
    configuredSparkConf()
        .forEach(
            (key, template) -> {
              if (appConf == null || !appConf.containsKey(key)) {
                overrides.put(key, template.replace(ZONE_PLACEHOLDER, zone.get()));
              }
            });
    return overrides;
  }

  /**
   * Parses the configured zone-dependent Spark configurations.
   *
   * @return The value templates by the configuration key, in the configured order. An entry
   *     without a key or a '=' is ignored.
   */
  static Map<String, String> configuredSparkConf() {
    Map<String, String> conf = new LinkedHashMap<>();
    for (String entry : ZONE_PINNING_SPARK_CONF.getValue().split(";")) {
      int separator = entry.indexOf('=');
      if (separator <= 0) {
        continue;
      }
      String key = entry.substring(0, separator).trim();
      if (StringUtils.isNotEmpty(key)) {
        conf.put(key, entry.substring(separator + 1).trim());
      }
    }
    return conf;
  }

  private static Optional<String> requestedZone(final SparkApplication app) {
    Map<String, String> annotations = app.getMetadata().getAnnotations();
    if (annotations == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(annotations.get(Constants.ANNOTATION_ZONE))
        .map(String::trim)
        .filter(StringUtils::isNotEmpty);
  }

  private static boolean isActive(final SparkApplication app) {
    ApplicationStatus status = app.getStatus();
    return status != null
        && status.getCurrentState() != null
        && status.getCurrentState().getCurrentStateSummary() != null
        && !status.getCurrentState().getCurrentStateSummary().isTerminated();
  }
}
