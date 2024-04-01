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

package org.apache.spark.kubernetes.operator.metrics;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.kubernetes.operator.config.SparkOperatorConfManager;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.spark.kubernetes.operator.config.SparkOperatorConf.CLASS;
import static org.apache.spark.kubernetes.operator.config.SparkOperatorConf.METRIC_PREFIX;
import static org.apache.spark.kubernetes.operator.config.SparkOperatorConf.SINK;

public class MetricsSystemFactory {
    public static MetricsSystem createMetricsSystem() {
        Properties properties =
                parseMetricsProperties(SparkOperatorConfManager.INSTANCE.getMetricsProperties());
        return new MetricsSystem(properties);
    }

    private static Properties parseMetricsProperties(Properties userProperties) {
        Properties properties = new Properties();
        Enumeration<?> valueEnumeration = userProperties.propertyNames();
        while (valueEnumeration.hasMoreElements()) {
            String key = (String) valueEnumeration.nextElement();
            if (key.startsWith(METRIC_PREFIX)) {
                properties.put(key.substring(METRIC_PREFIX.length()),
                        userProperties.getProperty(key));
            }
        }
        return properties;
    }

    public static Map<String, MetricsSystem.SinkProps> parseSinkProperties(
            Properties metricsProperties) {
        Map<String, MetricsSystem.SinkProps> propertiesMap = new HashMap<>();
        // e.g: "sink.graphite.class"="org.apache.spark.metrics.sink.GraphiteSink"
        Enumeration<?> valueEnumeration = metricsProperties.propertyNames();
        while (valueEnumeration.hasMoreElements()) {
            String key = (String) valueEnumeration.nextElement();
            int firstDotIndex = StringUtils.ordinalIndexOf(key, ".", 1);
            int secondDotIndex = StringUtils.ordinalIndexOf(key, ".", 2);
            if (key.startsWith(SINK)) {
                String shortName = key.substring(firstDotIndex + 1, secondDotIndex);
                MetricsSystem.SinkProps sinkProps =
                        propertiesMap.getOrDefault(shortName, new MetricsSystem.SinkProps());
                if (key.endsWith(CLASS)) {
                    sinkProps.setClassName(metricsProperties.getProperty(key));
                } else {
                    sinkProps.getProperties().put(key.substring(secondDotIndex + 1),
                            metricsProperties.getProperty(key));
                }
                propertiesMap.put(shortName, sinkProps);
            }
        }
        sinkPropertiesSanityCheck(propertiesMap);
        return propertiesMap;
    }

    private static void sinkPropertiesSanityCheck(
            Map<String, MetricsSystem.SinkProps> sinkPropsMap) {
        for (Map.Entry<String, MetricsSystem.SinkProps> pair : sinkPropsMap.entrySet()) {
            // Each Sink should have mapping class full name
            if (StringUtils.isBlank(pair.getValue().className)) {
                String errorMessage = String.format(
                        "%s provides properties, but does not provide full class name",
                        pair.getKey());
                throw new RuntimeException(errorMessage);
            }
            // Check the existence of each class full name
            try {
                Class.forName(pair.getValue().getClassName());
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(
                        String.format("Fail to find class %s", pair.getValue().getClassName()), e);
            }
        }
    }
}
