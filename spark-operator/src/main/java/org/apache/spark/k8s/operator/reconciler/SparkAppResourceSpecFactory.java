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

package org.apache.spark.k8s.operator.reconciler;

import static org.apache.spark.k8s.operator.Constants.DRIVER_SPARK_TEMPLATE_FILE_PROP_KEY;
import static org.apache.spark.k8s.operator.Constants.EXECUTOR_SPARK_TEMPLATE_FILE_PROP_KEY;
import static org.apache.spark.k8s.operator.utils.ModelUtils.overrideDriverTemplateEnabled;
import static org.apache.spark.k8s.operator.utils.ModelUtils.overrideExecutorTemplateEnabled;
import static org.apache.spark.k8s.operator.utils.Utils.sparkAppResourceLabels;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.client.KubernetesClient;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.SparkAppResourceSpec;
import org.apache.spark.k8s.operator.SparkAppSubmissionWorker;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.decorators.DriverDecorator;
import org.apache.spark.k8s.operator.utils.ModelUtils;

@Slf4j
public class SparkAppResourceSpecFactory {
  public static SparkAppResourceSpec buildResourceSpec(
      final SparkApplication app,
      final KubernetesClient client,
      final SparkAppSubmissionWorker worker) {
    Map<String, String> confOverrides = overrideDependencyConf(app);
    SparkAppResourceSpec resourceSpec = worker.getResourceSpec(app, client, confOverrides);
    cleanUpTempResourcesForApp(app, confOverrides);
    DriverDecorator decorator = new DriverDecorator(app);
    decorator.decorate(resourceSpec.getConfiguredPod());
    return resourceSpec;
  }

  private static Map<String, String> overrideDependencyConf(final SparkApplication app) {
    Map<String, String> confOverrides = new HashMap<>();
    sparkAppResourceLabels(app)
        .forEach(
            (k, v) -> {
              confOverrides.put("spark.kubernetes.driver.label." + k, v);
              confOverrides.put("spark.kubernetes.driver.service.label." + k, v);
              confOverrides.put("spark.kubernetes.executor.label." + k, v);
            });
    confOverrides.put("spark.kubernetes.namespace", app.getMetadata().getNamespace());
    if (app.getSpec().getSparkConf().containsKey("spark.app.name")) {
      confOverrides.put("spark.app.name", app.getMetadata().getName());
    }
    // FIXME: avoid this file flushing
    confOverrides.putAll(getOrCreateLocalFileForDriverSpec(app, confOverrides));
    confOverrides.putAll(getOrCreateLocalFileForExecutorSpec(app, confOverrides));
    return confOverrides;
  }

  private static void cleanUpTempResourcesForApp(
      final SparkApplication app, Map<String, String> confOverrides) {
    if (overrideDriverTemplateEnabled(app.getSpec())) {
      deleteLocalFileFromPathKey(confOverrides, DRIVER_SPARK_TEMPLATE_FILE_PROP_KEY);
    }
    if (overrideExecutorTemplateEnabled(app.getSpec())) {
      deleteLocalFileFromPathKey(confOverrides, EXECUTOR_SPARK_TEMPLATE_FILE_PROP_KEY);
    }
  }

  private static Optional<File> getLocalFileFromPathKey(
      Map<String, String> confOverrides, String pathKey) {
    if (confOverrides.containsKey(pathKey)) {
      String filePath = confOverrides.get(pathKey);
      if (filePath.startsWith("local") || filePath.startsWith("file") || filePath.startsWith("/")) {
        return Optional.of(new File(filePath));
      }
    }
    return Optional.empty();
  }

  private static void deleteLocalFileFromPathKey(
      Map<String, String> confOverrides, String pathKey) {
    Optional<File> localFile = Optional.empty();
    boolean deleted = false;
    try {
      localFile = getLocalFileFromPathKey(confOverrides, pathKey);
      if (localFile.isPresent() && localFile.get().exists() && localFile.get().isFile()) {
        deleted = localFile.get().delete();
      } else {
        log.warn("Local temp file not found at {}", pathKey);
      }
    } catch (Throwable t) {
      log.error("Failed to delete temp file. Attempting delete upon exit.", t);
    } finally {
      if (!deleted && localFile.isPresent() && localFile.get().exists()) {
        localFile.get().deleteOnExit();
      }
    }
  }

  private static Map<String, String> getOrCreateLocalFileForDriverSpec(
      final SparkApplication app, final Map<String, String> confOverrides) {
    if (overrideDriverTemplateEnabled(app.getSpec())) {
      Optional<File> localFile =
          getLocalFileFromPathKey(confOverrides, DRIVER_SPARK_TEMPLATE_FILE_PROP_KEY);
      if (localFile.isEmpty() || !localFile.get().exists() || !localFile.get().isFile()) {
        String filePath =
            createLocalFileForPodTemplateSpec(
                app.getSpec().getDriverSpec().getPodTemplateSpec(),
                app.getMetadata().getUid() + "-driver-");
        return Collections.singletonMap(DRIVER_SPARK_TEMPLATE_FILE_PROP_KEY, filePath);
      }
    }
    return Collections.emptyMap();
  }

  private static Map<String, String> getOrCreateLocalFileForExecutorSpec(
      final SparkApplication app, final Map<String, String> confOverrides) {
    if (overrideExecutorTemplateEnabled(app.getSpec())) {
      Optional<File> localFile =
          getLocalFileFromPathKey(confOverrides, EXECUTOR_SPARK_TEMPLATE_FILE_PROP_KEY);
      if (localFile.isEmpty() || !localFile.get().exists() || !localFile.get().isFile()) {
        String filePath =
            createLocalFileForPodTemplateSpec(
                app.getSpec().getExecutorSpec().getPodTemplateSpec(),
                app.getMetadata().getUid() + "-executor-");
        return Collections.singletonMap(EXECUTOR_SPARK_TEMPLATE_FILE_PROP_KEY, filePath);
      }
    }
    return Collections.emptyMap();
  }

  /**
   * Flush driver pod template spec to a local file
   *
   * @return temp file path
   */
  private static String createLocalFileForPodTemplateSpec(
      final PodTemplateSpec podTemplateSpec, final String tempFilePrefix) {
    try {
      File tmpFile = File.createTempFile(tempFilePrefix, ".json");
      FileOutputStream fileStream = new FileOutputStream(tmpFile);
      OutputStreamWriter writer = new OutputStreamWriter(fileStream, "UTF-8");
      writer.write(ModelUtils.asJsonString(ModelUtils.getPodFromTemplateSpec(podTemplateSpec)));
      writer.close();
      String path = tmpFile.getAbsolutePath();
      if (log.isDebugEnabled()) {
        log.debug("Temp file wrote to {}", tmpFile.getAbsolutePath());
      }
      return path;
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
