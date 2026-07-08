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

package org.apache.spark.k8s.operator.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.utils.StringUtils;

/**
 * Loads ConfigOption from properties file. In addition, loads hot properties override from config
 * map if dynamic config is enabled.
 */
@Slf4j
public class SparkOperatorConfManager {
  public static final String BASE_CONFIG_DIR = "/opt/spark-operator/conf/";
  public static final String INITIAL_CONFIG_FILE_PATH =
      BASE_CONFIG_DIR + "spark-operator.properties";

  public static final String METRICS_CONFIG_FILE_PATH = BASE_CONFIG_DIR + "metrics.properties";

  public static final String INITIAL_CONFIG_FILE_PATH_PROPS_KEY =
      "spark.kubernetes.operator.basePropertyFileName";

  public static final String METRICS_CONFIG_FILE_PATH_PROPS_KEY =
      "spark.kubernetes.operator.metrics.propertyFileName";

  public static final SparkOperatorConfManager INSTANCE = new SparkOperatorConfManager();
  protected final Properties initialConfig;
  protected final Properties metricsConfig;
  protected Properties configOverrides;

  protected SparkOperatorConfManager() {
    this.initialConfig = new Properties();
    this.configOverrides = new Properties();
    this.metricsConfig = new Properties();
    initialize();
  }

  /**
   * Returns all properties.
   *
   * @return a Properties instance contains all properties.
   */
  public Properties getAll() {
    synchronized (this) {
      Properties properties = new Properties();
      properties.putAll(initialConfig);
      properties.putAll(metricsConfig);
      properties.putAll(configOverrides);
      return properties;
    }
  }

  /**
   * Returns the current value for a given configuration key, considering dynamic overrides.
   *
   * @param key The configuration key.
   * @return The resolved configuration value.
   */
  public String getValue(String key) {
    synchronized (this) {
      String currentValue = configOverrides.getProperty(key);
      return StringUtils.isEmpty(currentValue) ? getInitialValue(key) : currentValue;
    }
  }

  /**
   * Returns the initial value for a given configuration key, without considering dynamic overrides.
   *
   * @param key The configuration key.
   * @return The initial configuration value.
   */
  public String getInitialValue(String key) {
    return initialConfig.getProperty(key);
  }

  /**
   * Refreshes the configuration overrides with new values from a map. Only keys that are declared
   * as dynamically overridable options (see {@link ConfigOption#dynamicOverrideEnabledKeys()}) are
   * applied; unknown keys and keys whose option disables dynamic override are dropped and logged so
   * an ineffective update is not silently ignored.
   *
   * @param updatedConfig A map containing the updated configuration properties.
   */
  public void refresh(Map<String, String> updatedConfig) {
    // Ensure every ConfigOption has registered before we read the allow-list. The options are
    // static final fields on SparkOperatorConf and register lazily on class init, so without this
    // the allow-list could be empty or partial if refresh() runs before SparkOperatorConf is
    // touched, silently dropping every override. Forcing init here makes correctness independent
    // of boot ordering rather than relying on an earlier incidental read of SparkOperatorConf.
    SparkOperatorConf.ensureOptionsRegistered();
    Set<String> allowedKeys = ConfigOption.dynamicOverrideEnabledKeys();
    Properties filtered = new Properties();
    List<String> droppedKeys = new ArrayList<>();
    for (Map.Entry<String, String> entry : updatedConfig.entrySet()) {
      if (allowedKeys.contains(entry.getKey())) {
        filtered.put(entry.getKey(), entry.getValue());
      } else {
        droppedKeys.add(entry.getKey());
      }
    }
    if (!droppedKeys.isEmpty()) {
      log.warn(
          "Ignoring dynamic config override for unknown or non-overridable keys: {}", droppedKeys);
    }
    synchronized (this) {
      this.configOverrides = filtered;
    }
  }

  /**
   * Returns the properties related to metrics configuration.
   *
   * @return A Properties object containing metrics configuration.
   */
  public Properties getMetricsProperties() {
    return metricsConfig;
  }

  private void initialize() {
    initialConfig.putAll(System.getProperties());
    Properties properties =
        getProperties(
            System.getProperty(INITIAL_CONFIG_FILE_PATH_PROPS_KEY, INITIAL_CONFIG_FILE_PATH));
    initialConfig.putAll(properties);
    Properties metricsProperties =
        getProperties(
            System.getProperty(METRICS_CONFIG_FILE_PATH_PROPS_KEY, METRICS_CONFIG_FILE_PATH));
    metricsConfig.putAll(metricsProperties);
  }

  private Properties getProperties(String filePath) {
    Properties properties = new Properties();
    try (InputStream inputStream = new FileInputStream(filePath)) {
      properties.load(inputStream);
    } catch (FileNotFoundException e) {
      log.warn("File Not Found: {}", filePath);
    } catch (IOException e) {
      log.error("Failed to load properties from {}.", filePath, e);
    }
    return properties;
  }
}
