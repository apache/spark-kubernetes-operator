/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.kubernetes.operator.config;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * Loads ConfigOption from properties file. In addition, loads hot properties override
 * from config map if dynamic config is enabled.
 */
@Slf4j
public class SparkOperatorConfManager {
  public static final String INITIAL_CONFIG_FILE_PATH =
      "/opt/spark-operator/conf/spark-operator.properties";

  public static final String METRICS_CONFIG_FILE_PATH =
      "/opt/spark-operator/conf/metrics.properties";

  public static final String INITIAL_CONFIG_FILE_PATH_PROPS_KEY =
      "spark.operator.base.property.file.name";

  public static final String METRICS_CONFIG_FILE_PATH_PROPS_KEY =
      "spark.operator.metrics.property.file.name";

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

  public String getValue(String key) {
    String currentValue = configOverrides.getProperty(key);
    return StringUtils.isEmpty(currentValue) ? getInitialValue(key) : currentValue;
  }

  public String getInitialValue(String key) {
    return initialConfig.getProperty(key);
  }

  public void refresh(Map<String, String> updatedConfig) {
    synchronized (this) {
      this.configOverrides = new Properties();
      configOverrides.putAll(updatedConfig);
    }
  }

  public Properties getMetricsProperties() {
    return metricsConfig;
  }

  private void initialize() {
    initialConfig.putAll(System.getProperties());
    Properties properties = getProperties(
        System.getProperty(INITIAL_CONFIG_FILE_PATH_PROPS_KEY, INITIAL_CONFIG_FILE_PATH));
    initialConfig.putAll(properties);
    initializeMetricsProperties();
  }

  private void initializeMetricsProperties() {
    Properties properties = getProperties(
        System.getProperty(METRICS_CONFIG_FILE_PATH_PROPS_KEY, METRICS_CONFIG_FILE_PATH));
    metricsConfig.putAll(properties);
  }

  private Properties getProperties(String filePath) {
    Properties properties = new Properties();
    try (InputStream inputStream = new FileInputStream(filePath)) {
      properties.load(inputStream);
    } catch (Exception e) {
      log.error("Failed to load properties from {}.", filePath, e);
    }
    return properties;
  }
}
