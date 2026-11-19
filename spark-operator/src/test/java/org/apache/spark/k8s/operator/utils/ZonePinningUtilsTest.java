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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.config.SparkOperatorConf;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;

class ZonePinningUtilsTest {

  @AfterEach
  void resetConf() {
    TestUtils.setConfigKey(SparkOperatorConf.ZONE_PINNING_ZONES, "");
    TestUtils.setConfigKey(SparkOperatorConf.ZONE_PINNING_SPARK_CONF, "");
    TestUtils.setConfigKey(
        SparkOperatorConf.ZONE_PINNING_TOPOLOGY_KEY, "topology.kubernetes.io/zone");
    TestUtils.setConfigKey(SparkOperatorConf.KUEUE_ENABLED, false);
  }

  @Test
  void notPinnedByDefault() {
    SparkApplication app = app("uid-1", Map.of(), Map.of(), null);
    assertFalse(ZonePinningUtils.needsZone(app));
    assertEquals(Map.of(), ZonePinningUtils.sparkConfOverrides(app));
  }

  @Test
  void needsZoneWhenZonesAreConfiguredOrAnnotated() {
    TestUtils.setConfigKey(SparkOperatorConf.ZONE_PINNING_ZONES, " us-east-1a, ,us-east-1b ");
    assertEquals(List.of("us-east-1a", "us-east-1b"), ZonePinningUtils.configuredZones());
    assertTrue(ZonePinningUtils.needsZone(app("uid-1", Map.of(), Map.of(), null)));
    // Pinned already
    assertFalse(ZonePinningUtils.needsZone(app("uid-1", Map.of(), Map.of(), "us-east-1a")));

    TestUtils.setConfigKey(SparkOperatorConf.ZONE_PINNING_ZONES, "");
    assertTrue(
        ZonePinningUtils.needsZone(
            app("uid-1", Map.of(Constants.ANNOTATION_ZONE, "us-east-1c"), Map.of(), null)));
  }

  @Test
  void kueueQueuedApplicationIsNotPinned() {
    TestUtils.setConfigKey(SparkOperatorConf.ZONE_PINNING_ZONES, "us-east-1a");
    SparkApplication app = app("uid-1", Map.of(), Map.of(), null);
    app.getMetadata().setLabels(Map.of(Constants.LABEL_QUEUE_NAME, "queue"));
    // The label is ignored while the Kueue integration is disabled
    assertTrue(ZonePinningUtils.needsZone(app));
    TestUtils.setConfigKey(SparkOperatorConf.KUEUE_ENABLED, true);
    assertFalse(ZonePinningUtils.needsZone(app));
  }

  @Test
  void annotationWinsOverConfiguredZones() {
    TestUtils.setConfigKey(SparkOperatorConf.ZONE_PINNING_ZONES, "us-east-1a");
    SparkApplication app =
        app("uid-1", Map.of(Constants.ANNOTATION_ZONE, " us-east-1c "), Map.of(), null);
    assertEquals("us-east-1c", ZonePinningUtils.chooseZone(app, Stream.empty()));
  }

  @Test
  void invalidAnnotationIsRejected() {
    SparkApplication app =
        app("uid-1", Map.of(Constants.ANNOTATION_ZONE, "us east/1a"), Map.of(), null);
    assertThrows(
        IllegalArgumentException.class, () -> ZonePinningUtils.chooseZone(app, Stream.empty()));
  }

  @Test
  void choosesZoneWithFewestActiveApplications() {
    TestUtils.setConfigKey(SparkOperatorConf.ZONE_PINNING_ZONES, "a,b,c");
    SparkApplication app = app("uid-0", Map.of(), Map.of(), null);
    Stream<SparkApplication> others =
        Stream.of(
            app("uid-1", Map.of(), Map.of(), "a"),
            app("uid-2", Map.of(), Map.of(), "a"),
            app("uid-3", Map.of(), Map.of(), "b"),
            terminated(app("uid-4", Map.of(), Map.of(), "c")),
            terminated(app("uid-5", Map.of(), Map.of(), "c")),
            // Not a configured zone, and the application itself, which do not count
            app("uid-6", Map.of(), Map.of(), "d"),
            app("uid-0", Map.of(), Map.of(), "c"),
            app("uid-7", Map.of(), Map.of(), null));
    assertEquals("c", ZonePinningUtils.chooseZone(app, others));
  }

  @Test
  void tieIsBrokenByUid() {
    TestUtils.setConfigKey(SparkOperatorConf.ZONE_PINNING_ZONES, "a,b,c");
    Set<String> chosen = new HashSet<>();
    for (int i = 0; i < 50; i++) {
      SparkApplication app = app("uid-" + i, Map.of(), Map.of(), null);
      String zone =
          ZonePinningUtils.chooseZone(app, Stream.of(app("other", Map.of(), Map.of(), "a")));
      // The same application always gets the same zone
      assertEquals(
          zone,
          ZonePinningUtils.chooseZone(app, Stream.of(app("other", Map.of(), Map.of(), "a"))));
      chosen.add(zone);
    }
    assertEquals(Set.of("b", "c"), chosen);
  }

  @Test
  void noZoneToChooseIsRejected() {
    SparkApplication app = app("uid-1", Map.of(), Map.of(), null);
    assertThrows(
        IllegalArgumentException.class, () -> ZonePinningUtils.chooseZone(app, Stream.empty()));
  }

  @Test
  void pinnedAttemptGetsNodeSelectorsAndZoneConf() {
    TestUtils.setConfigKey(
        SparkOperatorConf.ZONE_PINNING_SPARK_CONF,
        " spark.celeborn.tags.tagsExpr = az-{zone} ; malformed ; =x ; spark.user.set=keep-{zone}");
    SparkApplication app =
        app("uid-1", Map.of(), Map.of("spark.user.set", "mine"), "us-east-1b");
    assertEquals(
        Map.of(
            "spark.kubernetes.driver.node.selector.topology.kubernetes.io/zone", "us-east-1b",
            "spark.kubernetes.executor.node.selector.topology.kubernetes.io/zone", "us-east-1b",
            "spark.celeborn.tags.tagsExpr", "az-us-east-1b"),
        ZonePinningUtils.sparkConfOverrides(app));
  }

  @Test
  void topologyKeyIsConfigurable() {
    TestUtils.setConfigKey(SparkOperatorConf.ZONE_PINNING_TOPOLOGY_KEY, "example.com/zone");
    SparkApplication app = app("uid-1", Map.of(), Map.of(), "z1");
    assertEquals(
        Map.of(
            "spark.kubernetes.driver.node.selector.example.com/zone", "z1",
            "spark.kubernetes.executor.node.selector.example.com/zone", "z1"),
        ZonePinningUtils.sparkConfOverrides(app));
  }

  private static SparkApplication app(
      String uid, Map<String, String> annotations, Map<String, String> sparkConf, String zone) {
    SparkApplication app = new SparkApplication();
    app.setMetadata(
        new ObjectMetaBuilder()
            .withName("app-" + uid)
            .withNamespace("default")
            .withUid(uid)
            .withAnnotations(new HashMap<>(annotations))
            .build());
    app.getSpec().setSparkConf(new HashMap<>(sparkConf));
    ApplicationStatus status = new ApplicationStatus();
    app.setStatus(zone == null ? status : status.pinCurrentAttemptToZone(zone));
    return app;
  }

  private static SparkApplication terminated(SparkApplication app) {
    app.setStatus(
        app.getStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.ResourceReleased, "")));
    return app;
  }
}
