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

package org.apache.spark.k8s.operator.metrics;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import org.apache.spark.k8s.operator.config.SparkOperatorConfManager;

/** Factory for MetricsSystem. */
public final class MetricsSystemFactory {
  public static final String METRIC_PREFIX = "spark.metrics.conf.operator.";
  public static final String SINK = "sink.";
  public static final String CLASS = "class";

  private MetricsSystemFactory() {}

  /**
   * Creates a MetricsSystem instance using properties from SparkOperatorConfManager.
   *
   * @return A new MetricsSystem instance.
   */
  public static MetricsSystem createMetricsSystem() {
    Properties properties =
        parseMetricsProperties(SparkOperatorConfManager.INSTANCE.getMetricsProperties());
    return new MetricsSystem(properties);
  }

  /**
   * Creates a MetricsSystem instance with the given properties.
   *
   * @param properties The properties to configure the metrics system.
   * @return A new MetricsSystem instance.
   */
  public static MetricsSystem createMetricsSystem(Properties properties) {
    return new MetricsSystem(properties);
  }

  private static Properties parseMetricsProperties(Properties userProperties) {
    Properties properties = new Properties();
    Enumeration<?> valueEnumeration = userProperties.propertyNames();
    while (valueEnumeration.hasMoreElements()) {
      String key = (String) valueEnumeration.nextElement();
      if (key.startsWith(METRIC_PREFIX)) {
        properties.put(key.substring(METRIC_PREFIX.length()), userProperties.getProperty(key));
      }
    }
    return properties;
  }

  /**
   * Parses the given properties to extract sink-related configurations.
   *
   * @param metricsProperties The properties containing metrics configurations.
   * @return A Map where keys are sink short names and values are SinkProperties objects.
   */
  public static Map<String, MetricsSystem.SinkProperties> parseSinkProperties(
      Properties metricsProperties) {
    Map<String, MetricsSystem.SinkProperties> propertiesMap = new HashMap<>();
    // e.g: "sink.graphite.class"="org.apache.spark.metrics.sink.ConsoleSink"
    Enumeration<?> valueEnumeration = metricsProperties.propertyNames();
    while (valueEnumeration.hasMoreElements()) {
      String key = (String) valueEnumeration.nextElement();
      int firstDotIndex = StringUtils.ordinalIndexOf(key, ".", 1);
      int secondDotIndex = StringUtils.ordinalIndexOf(key, ".", 2);
      if (key.startsWith(SINK)) {
        String shortName = key.substring(firstDotIndex + 1, secondDotIndex);
        MetricsSystem.SinkProperties sinkProperties =
            propertiesMap.getOrDefault(shortName, new MetricsSystem.SinkProperties());
        if (key.endsWith(CLASS)) {
          sinkProperties.setClassName(metricsProperties.getProperty(key));
        } else {
          sinkProperties
              .getProperties()
              .put(key.substring(secondDotIndex + 1), metricsProperties.getProperty(key));
        }
        propertiesMap.put(shortName, sinkProperties);
      }
    }
    sinkPropertiesSanityCheck(propertiesMap);
    return propertiesMap;
  }

  private static void sinkPropertiesSanityCheck(
      Map<String, MetricsSystem.SinkProperties> sinkPropsMap) {
    for (Map.Entry<String, MetricsSystem.SinkProperties> pair : sinkPropsMap.entrySet()) {
      // Each Sink should have mapping class full name
      if (StringUtils.isBlank(pair.getValue().className)) {
        String errorMessage =
            String.format(
                "%s provides properties, but does not provide full class name", pair.getKey());
        throw new IllegalStateException(errorMessage);
      }
      // Check the existence of each class full name
      try {
        Class.forName(pair.getValue().getClassName());
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException(
            String.format("Fail to find class %s", pair.getValue().getClassName()), e);
      }
    }
  }
}
