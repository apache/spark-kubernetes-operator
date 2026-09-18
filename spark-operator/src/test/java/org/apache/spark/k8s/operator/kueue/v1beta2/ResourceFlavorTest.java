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

package org.apache.spark.k8s.operator.kueue.v1beta2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.Toleration;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.utils.ModelUtils;

class ResourceFlavorTest {

  @Test
  void testResourceFlavorDeserialization() throws Exception {
    String json =
        "{\"apiVersion\":\"kueue.x-k8s.io/v1beta2\",\"kind\":\"ResourceFlavor\","
            + "\"metadata\":{\"name\":\"spot\"},"
            + "\"spec\":{\"nodeLabels\":{\"instance-type\":\"spot\"},"
            + "\"nodeTaints\":[{\"key\":\"spot\",\"value\":\"true\",\"effect\":\"NoSchedule\"}],"
            + "\"tolerations\":[{\"key\":\"spot\",\"operator\":\"Equal\",\"value\":\"true\","
            + "\"effect\":\"NoSchedule\"}],"
            + "\"topologyName\":\"default\"}}";

    ResourceFlavor flavor = ModelUtils.objectMapper.readValue(json, ResourceFlavor.class);

    assertEquals("spot", flavor.getMetadata().getName());
    assertNull(flavor.getMetadata().getNamespace());
    assertEquals(Map.of("instance-type", "spot"), flavor.getSpec().getNodeLabels());
    assertEquals(
        List.of(new Toleration("NoSchedule", "spot", "Equal", null, "true")),
        flavor.getSpec().getTolerations());
  }

  @Test
  void testResourceFlavorWithoutSpecFields() throws Exception {
    ResourceFlavor flavor =
        ModelUtils.objectMapper.readValue(
            "{\"metadata\":{\"name\":\"default-flavor\"},\"spec\":{}}", ResourceFlavor.class);

    assertEquals(Map.of(), flavor.getSpec().getNodeLabels());
    assertEquals(List.of(), flavor.getSpec().getTolerations());
  }
}
