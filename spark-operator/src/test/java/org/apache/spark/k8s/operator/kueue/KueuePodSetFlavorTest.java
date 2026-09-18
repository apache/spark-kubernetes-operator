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

package org.apache.spark.k8s.operator.kueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Toleration;
import org.junit.jupiter.api.Test;

class KueuePodSetFlavorTest {

  @Test
  void applyToAddsNodeSelectorAndTolerationsToEmptyPodSpec() {
    PodSpec podSpec = new PodSpec();
    Toleration toleration = toleration("spot", "Equal", "true", "NoSchedule", null);

    new KueuePodSetFlavor(Map.of("instance-type", "spot"), List.of(toleration)).applyTo(podSpec);

    assertEquals(Map.of("instance-type", "spot"), podSpec.getNodeSelector());
    assertEquals(List.of(toleration), podSpec.getTolerations());
  }

  @Test
  void applyToKeepsExistingNodeSelectorAndTolerations() {
    Toleration existing = toleration("gpu", "Exists", null, "NoSchedule", null);
    Toleration added = toleration("spot", "Equal", "true", "NoSchedule", null);
    PodSpec podSpec =
        new PodSpecBuilder()
            .withNodeSelector(Map.of("zone", "a", "instance-type", "spot"))
            .withTolerations(existing)
            .build();

    new KueuePodSetFlavor(Map.of("instance-type", "spot", "pool", "p1"), List.of(added))
        .applyTo(podSpec);

    assertEquals(
        Map.of("zone", "a", "instance-type", "spot", "pool", "p1"), podSpec.getNodeSelector());
    assertEquals(List.of(existing, added), podSpec.getTolerations());
  }

  @Test
  void applyToDoesNotDuplicateTolerations() {
    // Like Kueue, an empty operator is the same as `Equal` and tolerationSeconds is ignored, and
    // the existing toleration wins.
    Toleration existing = toleration("spot", "Equal", "true", "NoExecute", 30L);
    PodSpec podSpec = new PodSpecBuilder().withTolerations(existing).build();

    new KueuePodSetFlavor(
            Map.of(),
            List.of(
                toleration("spot", null, "true", "NoExecute", 60L),
                toleration("spot", "Equal", "false", "NoExecute", null),
                toleration("spot", "Equal", "false", "NoExecute", null)))
        .applyTo(podSpec);

    assertEquals(
        List.of(existing, toleration("spot", "Equal", "false", "NoExecute", null)),
        podSpec.getTolerations());
  }

  @Test
  void isSameToleration() {
    assertTrue(
        KueuePodSetFlavor.isSameToleration(
            toleration("k", "", "v", "NoSchedule", null),
            toleration("k", "Equal", "v", "NoSchedule", 10L)));
    assertTrue(
        KueuePodSetFlavor.isSameToleration(
            toleration("k", "Exists", null, null, null),
            toleration("k", "Exists", null, null, null)));
    assertFalse(
        KueuePodSetFlavor.isSameToleration(
            toleration("k", "Exists", null, null, null),
            toleration("k", "Equal", null, null, null)));
    assertFalse(
        KueuePodSetFlavor.isSameToleration(
            toleration("k", "Equal", "v", "NoSchedule", null),
            toleration("k", "Equal", "v", "NoExecute", null)));
  }

  private static Toleration toleration(
      String key, String operator, String value, String effect, Long seconds) {
    return new Toleration(effect, key, operator, seconds, value);
  }
}
