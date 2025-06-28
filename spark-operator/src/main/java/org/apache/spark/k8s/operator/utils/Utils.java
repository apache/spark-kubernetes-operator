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

import static org.apache.spark.k8s.operator.Constants.LABEL_RESOURCE_NAME;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_OPERATOR_NAME;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_ROLE_CLUSTER_VALUE;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_ROLE_DRIVER_VALUE;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_ROLE_EXECUTOR_VALUE;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_VERSION_NAME;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.OPERATOR_APP_NAME;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.OPERATOR_WATCHED_NAMESPACES;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.SPARK_APP_STATUS_LISTENER_CLASS_NAMES;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.SPARK_CLUSTER_STATUS_LISTENER_CLASS_NAMES;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.listeners.SparkAppStatusListener;
import org.apache.spark.k8s.operator.listeners.SparkClusterStatusListener;

/** Utility class for common operations. */
public final class Utils {

  private Utils() {}

  public static Set<String> sanitizeCommaSeparatedStrAsSet(String str) {
    if (StringUtils.isBlank(str)) {
      return Collections.emptySet();
    }
    if ("*".equals(str)) {
      return Collections.emptySet();
    }
    return Arrays.stream(str.split(","))
        .map(String::trim)
        .filter(StringUtils::isNotBlank)
        .collect(Collectors.toSet());
  }

  public static String labelsAsStr(Map<String, String> labels) {
    return labels.entrySet().stream()
        .map(e -> String.join("=", e.getKey(), e.getValue()))
        .collect(Collectors.joining(","));
  }

  public static Map<String, String> commonOperatorResourceLabels() {
    Map<String, String> labels = new HashMap<>();
    labels.put(LABEL_RESOURCE_NAME, OPERATOR_APP_NAME.getValue());
    return labels;
  }

  public static Map<String, String> defaultOperatorConfigLabels() {
    Map<String, String> labels = new HashMap<>(commonOperatorResourceLabels());
    labels.put("app.kubernetes.io/component", "operator-dynamic-config-overrides");
    return labels;
  }

  public static Map<String, String> commonManagedResourceLabels() {
    Map<String, String> labels = new HashMap<>();
    labels.put(LABEL_SPARK_OPERATOR_NAME, OPERATOR_APP_NAME.getValue());
    return labels;
  }

  public static Map<String, String> sparkAppResourceLabels(final SparkApplication app) {
    return sparkAppResourceLabels(app.getMetadata().getName());
  }

  public static Map<String, String> sparkAppResourceLabels(final String appName) {
    Map<String, String> labels = commonManagedResourceLabels();
    labels.put(Constants.LABEL_SPARK_APPLICATION_NAME, appName);
    return labels;
  }

  public static Map<String, String> driverLabels(final SparkApplication app) {
    Map<String, String> labels = sparkAppResourceLabels(app);
    labels.put(Constants.LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_DRIVER_VALUE);
    return labels;
  }

  public static Map<String, String> executorLabels(final SparkApplication app) {
    Map<String, String> labels = sparkAppResourceLabels(app);
    labels.put(Constants.LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_EXECUTOR_VALUE);
    return labels;
  }

  public static Map<String, String> sparkClusterResourceLabels(final SparkCluster cluster) {
    Map<String, String> labels = commonManagedResourceLabels();
    labels.put(Constants.LABEL_SPARK_CLUSTER_NAME, cluster.getMetadata().getName());
    labels.put(LABEL_SPARK_VERSION_NAME, cluster.getSpec().getRuntimeVersions().getSparkVersion());
    return labels;
  }

  public static Map<String, String> clusterLabels(final SparkCluster cluster) {
    Map<String, String> labels = sparkClusterResourceLabels(cluster);
    labels.put(Constants.LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_CLUSTER_VALUE);
    return labels;
  }

  public static Set<String> getWatchedNamespaces() {
    return sanitizeCommaSeparatedStrAsSet(OPERATOR_WATCHED_NAMESPACES.getValue());
  }

  public static List<SparkAppStatusListener> getAppStatusListener() {
    return ClassLoadingUtils.getStatusListener(
        SparkAppStatusListener.class, SPARK_APP_STATUS_LISTENER_CLASS_NAMES.getValue());
  }

  public static List<SparkClusterStatusListener> getClusterStatusListener() {
    return ClassLoadingUtils.getStatusListener(
        SparkClusterStatusListener.class, SPARK_CLUSTER_STATUS_LISTENER_CLASS_NAMES.getValue());
  }

  /**
   * Labels to be applied to all created resources, as a comma-separated string
   *
   * @return labels string
   */
  public static String commonResourceLabelsStr() {
    return labelsAsStr(commonManagedResourceLabels());
  }
}
