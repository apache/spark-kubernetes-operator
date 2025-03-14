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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.ConfigMap;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.spec.ConfigMapSpec;

class ConfigMapSpecUtilsTest {

  @Test
  void buildConfigMaps() {

    assertEquals(Collections.emptyList(), ConfigMapSpecUtils.buildConfigMaps(null));
    assertEquals(
        Collections.emptyList(), ConfigMapSpecUtils.buildConfigMaps(Collections.emptyList()));

    Map<String, String> labels = Map.of("foo-label-key", "foo-label-value");
    Map<String, String> annotations = Map.of("foo-annotation-key", "foo-annotation-value");
    Map<String, String> configMapData = new HashMap<>();
    // literal value
    configMapData.put("foo", "bar");
    // escaped property file
    configMapData.put(
        "conf.properties", "spark.foo.keyOne=\"valueOne\"\nspark.foo.keyTwo=\"valueTwo\"");
    // escaped yaml file
    configMapData.put("conf.yaml", "conf:\n  foo: bar\n  nestedField:\n    keyName: value");
    ConfigMapSpec mount =
        ConfigMapSpec.builder()
            .name("test-config-map")
            .labels(labels)
            .annotations(annotations)
            .data(configMapData)
            .build();
    List<ConfigMap> configMaps =
        ConfigMapSpecUtils.buildConfigMaps(Collections.singletonList(mount));
    assertEquals(1, configMaps.size());
    ConfigMap configMap = configMaps.get(0);
    assertEquals("test-config-map", configMap.getMetadata().getName());
    assertEquals("foo-label-value", configMap.getMetadata().getLabels().get("foo-label-key"));
    assertEquals(
        "foo-annotation-value", configMap.getMetadata().getAnnotations().get("foo-annotation-key"));
    assertEquals(configMapData, configMap.getData());
  }
}
