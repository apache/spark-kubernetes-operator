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

import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.config.SparkOperatorConf;
import org.apache.spark.k8s.operator.config.SparkOperatorConfManager;

class UtilsTest {
  private static final String REDACTED_TEXT =
      org.apache.spark.util.Utils$.MODULE$.REDACTION_REPLACEMENT_TEXT();
  private static final String REDACTION_REGEX_KEY = SparkOperatorConf.REDACTION_REGEX.getKey();

  @Test
  void redactionRegexConfigReusesSparkRedactionConfig() {
    assertEquals(
        org.apache.spark.internal.config.package$.MODULE$.SECRET_REDACTION_PATTERN().key(),
        SparkOperatorConf.REDACTION_REGEX.getKey());
    assertEquals(
        org.apache.spark.internal.config.package$
            .MODULE$
            .SECRET_REDACTION_PATTERN()
            .defaultValueString(),
        SparkOperatorConf.REDACTION_REGEX.getDefaultValue());
  }

  @AfterEach
  void resetConfigOverrides() {
    SparkOperatorConfManager.INSTANCE.refresh(Map.of());
  }

  @Test
  void redactSensitiveInfoMasksSensitiveKeysWithSparkDefaultPattern() {
    Map<String, String> props =
        Map.of(
            "spark.hadoop.fs.s3a.access.key", "sensitive-access-key",
            "spark.ssl.keyStorePassword", "sensitive-keystore-value",
            "my.service.token", "sensitive-service-value",
            "custom.api.secret", "sensitive-api-value",
            "spark.kubernetes.operator.name", "spark-kubernetes-operator");
    Map<String, String> redacted = Utils.redactSensitiveInfo(props);
    assertEquals(REDACTED_TEXT, redacted.get("spark.hadoop.fs.s3a.access.key"));
    assertEquals(REDACTED_TEXT, redacted.get("spark.ssl.keyStorePassword"));
    assertEquals(REDACTED_TEXT, redacted.get("my.service.token"));
    assertEquals(REDACTED_TEXT, redacted.get("custom.api.secret"));
    assertEquals("spark-kubernetes-operator", redacted.get("spark.kubernetes.operator.name"));
    assertEquals(props.keySet(), redacted.keySet());
  }

  @Test
  void redactSensitiveInfoAcceptsProperties() {
    Properties props = new Properties();
    props.setProperty("my.db.password", "sensitive-db-value");
    props.setProperty("spark.kubernetes.operator.namespace", "default");
    Map<String, String> redacted = Utils.redactSensitiveInfo(props);
    assertEquals(REDACTED_TEXT, redacted.get("my.db.password"));
    assertEquals("default", redacted.get("spark.kubernetes.operator.namespace"));
  }

  @Test
  void redactSensitiveInfoHonorsRedactionRegexFromGivenProperties() {
    Map<String, String> props =
        Map.of(
            REDACTION_REGEX_KEY, "(?i)confidential",
            "custom.confidential.conf", "custom-value",
            "spark.kubernetes.operator.namespace", "default");
    Map<String, String> redacted = Utils.redactSensitiveInfo(props);
    assertEquals(REDACTED_TEXT, redacted.get("custom.confidential.conf"));
    assertEquals("default", redacted.get("spark.kubernetes.operator.namespace"));
  }

  @Test
  void redactSensitiveInfoIgnoresDynamicRedactionRegexOverride() {
    // spark.redaction.regex is intentionally not dynamically overridable: a runtime override
    // could weaken or disable redaction, so refresh() drops it and the startup default pattern
    // continues to apply.
    SparkOperatorConfManager.INSTANCE.refresh(Map.of(REDACTION_REGEX_KEY, "(?i)confidential"));
    Map<String, String> props =
        Map.of(
            "custom.confidential.conf", "custom-value",
            "spark.kubernetes.operator.namespace", "default");
    Map<String, String> redacted = Utils.redactSensitiveInfo(props);
    // The override is ignored, so "confidential" is not treated as sensitive.
    assertEquals("custom-value", redacted.get("custom.confidential.conf"));
    assertEquals("default", redacted.get("spark.kubernetes.operator.namespace"));
    assertEquals(props.keySet(), redacted.keySet());
  }
}
