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

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;
import org.apache.commons.lang3.StringUtils;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.listeners.SparkAppStatusListener;
import org.apache.spark.k8s.operator.listeners.SparkClusterStatusListener;

/** Utility class for common operations. */
public final class Utils {

  private Utils() {}

  /**
   * Sanitizes a comma-separated string into a Set of trimmed, non-blank strings. If the input is
   * "*", an empty set is returned.
   *
   * @param str The comma-separated string.
   * @return A Set of sanitized strings.
   */
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

  /**
   * Converts a Map of labels to a comma-separated string of key=value pairs.
   *
   * @param labels The Map of labels.
   * @return A string representation of the labels.
   */
  public static String labelsAsStr(Map<String, String> labels) {
    return labels.entrySet().stream()
        .map(e -> String.join("=", e.getKey(), e.getValue()))
        .collect(Collectors.joining(","));
  }

  /**
   * Returns a Map of common labels for operator resources.
   *
   * @return A Map of common operator resource labels.
   */
  public static Map<String, String> commonOperatorResourceLabels() {
    Map<String, String> labels = new HashMap<>();
    labels.put(LABEL_RESOURCE_NAME, OPERATOR_APP_NAME.getValue());
    return labels;
  }

  /**
   * Returns a Map of default labels for operator configuration resources.
   *
   * @return A Map of default operator configuration labels.
   */
  public static Map<String, String> defaultOperatorConfigLabels() {
    Map<String, String> labels = new HashMap<>(commonOperatorResourceLabels());
    labels.put("app.kubernetes.io/component", "operator-dynamic-config-overrides");
    return labels;
  }

  /**
   * Returns a Map of common labels for resources managed by the operator.
   *
   * @return A Map of common managed resource labels.
   */
  public static Map<String, String> commonManagedResourceLabels() {
    Map<String, String> labels = new HashMap<>();
    labels.put(LABEL_SPARK_OPERATOR_NAME, OPERATOR_APP_NAME.getValue());
    return labels;
  }

  /**
   * Returns a Map of labels for a SparkApplication resource.
   *
   * @param app The SparkApplication object.
   * @return A Map of labels for the SparkApplication.
   */
  public static Map<String, String> sparkAppResourceLabels(final SparkApplication app) {
    return sparkAppResourceLabels(app.getMetadata().getName());
  }

  /**
   * Returns a Map of labels for a SparkApplication resource given its name.
   *
   * @param appName The name of the SparkApplication.
   * @return A Map of labels for the SparkApplication.
   */
  public static Map<String, String> sparkAppResourceLabels(final String appName) {
    Map<String, String> labels = commonManagedResourceLabels();
    labels.put(Constants.LABEL_SPARK_APPLICATION_NAME, appName);
    return labels;
  }

  /**
   * Returns a Map of labels for the driver pod of a SparkApplication.
   *
   * @param app The SparkApplication object.
   * @return A Map of labels for the driver pod.
   */
  public static Map<String, String> driverLabels(final SparkApplication app) {
    Map<String, String> labels = sparkAppResourceLabels(app);
    labels.put(Constants.LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_DRIVER_VALUE);
    return labels;
  }

  /**
   * Returns a Map of labels for executor pods of a SparkApplication.
   *
   * @param app The SparkApplication object.
   * @return A Map of labels for executor pods.
   */
  public static Map<String, String> executorLabels(final SparkApplication app) {
    Map<String, String> labels = sparkAppResourceLabels(app);
    labels.put(Constants.LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_EXECUTOR_VALUE);
    return labels;
  }

  /**
   * Returns a Map of labels for a SparkCluster resource.
   *
   * @param cluster The SparkCluster object.
   * @return A Map of labels for the SparkCluster.
   */
  public static Map<String, String> sparkClusterResourceLabels(final SparkCluster cluster) {
    Map<String, String> labels = commonManagedResourceLabels();
    labels.put(Constants.LABEL_SPARK_CLUSTER_NAME, cluster.getMetadata().getName());
    labels.put(LABEL_SPARK_VERSION_NAME, cluster.getSpec().getRuntimeVersions().getSparkVersion());
    return labels;
  }

  /**
   * Returns a Map of labels for a SparkCluster's components.
   *
   * @param cluster The SparkCluster object.
   * @return A Map of labels for the SparkCluster's components.
   */
  public static Map<String, String> clusterLabels(final SparkCluster cluster) {
    Map<String, String> labels = sparkClusterResourceLabels(cluster);
    labels.put(Constants.LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_CLUSTER_VALUE);
    return labels;
  }

  /**
   * Returns the set of namespaces that the operator is configured to watch.
   *
   * @return A Set of namespace names.
   */
  public static Set<String> getWatchedNamespaces() {
    return sanitizeCommaSeparatedStrAsSet(OPERATOR_WATCHED_NAMESPACES.getValue());
  }

  /**
   * Retrieves a list of SparkAppStatusListener instances based on configuration.
   *
   * @return A List of SparkAppStatusListener objects.
   */
  public static List<SparkAppStatusListener> getAppStatusListener() {
    return ClassLoadingUtils.getStatusListener(
        SparkAppStatusListener.class, SPARK_APP_STATUS_LISTENER_CLASS_NAMES.getValue());
  }

  /**
   * Retrieves a list of SparkClusterStatusListener instances based on configuration.
   *
   * @return A List of SparkClusterStatusListener objects.
   */
  public static List<SparkClusterStatusListener> getClusterStatusListener() {
    return ClassLoadingUtils.getStatusListener(
        SparkClusterStatusListener.class, SPARK_CLUSTER_STATUS_LISTENER_CLASS_NAMES.getValue());
  }

  /**
   * Returns a comma-separated string of common labels to be applied to all created resources.
   *
   * @return A string of common resource labels.
   */
  public static String commonResourceLabelsStr() {
    return labelsAsStr(commonManagedResourceLabels());
  }

  /**
   * Creates a SecondaryToPrimaryMapper that maps secondary resources to their primary resources
   * based on a common label.
   *
   * @param nameKey The label key used to identify the primary resource's name.
   * @param <T> The type of the secondary resource, extending HasMetadata.
   * @return A SecondaryToPrimaryMapper instance.
   */
  public static <T extends HasMetadata>
      SecondaryToPrimaryMapper<T> basicLabelSecondaryToPrimaryMapper(String nameKey) {
    return resource -> {
      final var metadata = resource.getMetadata();
      if (metadata == null) {
        return Collections.emptySet();
      } else {
        final var map = metadata.getLabels();
        if (map == null) {
          return Collections.emptySet();
        }
        var name = map.get(nameKey);
        if (name == null) {
          return Collections.emptySet();
        }
        var namespace = resource.getMetadata().getNamespace();
        return Set.of(new ResourceID(name, namespace));
      }
    };
  }
}
