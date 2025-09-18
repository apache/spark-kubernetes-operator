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

import static org.apache.spark.k8s.operator.Constants.DRIVER_SPARK_CONTAINER_PROP_KEY;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;

/** Utility class for Kubernetes model operations. */
public final class ModelUtils {

  public static final ObjectMapper objectMapper = new ObjectMapper();

  private ModelUtils() {}

  /**
   * Converts a PodTemplateSpec to a Pod object.
   *
   * @param podTemplateSpec The PodTemplateSpec to convert.
   * @return A new Pod object.
   */
  public static Pod getPodFromTemplateSpec(PodTemplateSpec podTemplateSpec) {
    if (podTemplateSpec == null) {
      return new PodBuilder().withNewMetadata().endMetadata().withNewSpec().endSpec().build();
    }
    return new PodBuilder()
        .withMetadata(podTemplateSpec.getMetadata())
        .withSpec(podTemplateSpec.getSpec())
        .withAdditionalProperties(podTemplateSpec.getAdditionalProperties())
        .build();
  }

  /**
   * Finds the Spark main container(s) in the driver pod based on the application specification. If
   * `spark.kubernetes.driver.podTemplateContainerName` is not set, all containers are considered as
   * main containers.
   *
   * @param appSpec The ApplicationSpec of the Spark application.
   * @param containerStatusList A list of ContainerStatus objects from the driver pod.
   * @return A List of ContainerStatus objects representing the main containers.
   */
  public static List<ContainerStatus> findDriverMainContainerStatus(
      final ApplicationSpec appSpec, final List<ContainerStatus> containerStatusList) {
    if (appSpec == null) {
      return containerStatusList;
    }
    Map<String, String> sparkConf = appSpec.getSparkConf();
    String key = sparkConf.get(DRIVER_SPARK_CONTAINER_PROP_KEY);
    if (key == null || key.isEmpty()) {
      return containerStatusList;
    }
    String mainContainerName = sparkConf.get(DRIVER_SPARK_CONTAINER_PROP_KEY);
    return containerStatusList.stream()
        .filter(c -> mainContainerName.equalsIgnoreCase(c.getName()))
        .collect(Collectors.toList());
  }

  /**
   * Builds an OwnerReference to the given resource.
   *
   * @param owner The owner resource.
   * @return An OwnerReference object to be used for subresources.
   */
  public static OwnerReference buildOwnerReferenceTo(HasMetadata owner) {
    return new OwnerReferenceBuilder()
        .withName(owner.getMetadata().getName())
        .withApiVersion(owner.getApiVersion())
        .withKind(owner.getKind())
        .withUid(owner.getMetadata().getUid())
        .withBlockOwnerDeletion(true)
        .build();
  }

  /**
   * Converts a Kubernetes resource to its JSON string representation.
   *
   * @param resource The resource to convert.
   * @param <T> The type of the resource, extending HasMetadata.
   * @return A JSON string representation of the resource.
   */
  public static <T extends HasMetadata> String asJsonString(T resource) {
    try {
      return objectMapper.writeValueAsString(resource);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Checks if overriding the driver template is enabled in the application specification.
   *
   * @param applicationSpec The ApplicationSpec to check.
   * @return True if driver template override is enabled, false otherwise.
   */
  public static boolean overrideDriverTemplateEnabled(ApplicationSpec applicationSpec) {
    return applicationSpec != null
        && applicationSpec.getDriverSpec() != null
        && applicationSpec.getDriverSpec().getPodTemplateSpec() != null;
  }

  /**
   * Checks if overriding the executor template is enabled in the application specification.
   *
   * @param applicationSpec The ApplicationSpec to check.
   * @return True if executor template override is enabled, false otherwise.
   */
  public static boolean overrideExecutorTemplateEnabled(ApplicationSpec applicationSpec) {
    return applicationSpec != null
        && applicationSpec.getExecutorSpec() != null
        && applicationSpec.getExecutorSpec().getPodTemplateSpec() != null;
  }

  /**
   * Retrieves the current attempt ID from a SparkApplication's status.
   *
   * @param app The SparkApplication to get the attempt ID from.
   * @return The current attempt ID, or 0L if not available.
   */
  public static long getAttemptId(final SparkApplication app) {
    long attemptId = 0L;
    if (app.getStatus() != null && app.getStatus().getCurrentAttemptSummary() != null) {
      attemptId = app.getStatus().getCurrentAttemptSummary().getAttemptInfo().getId();
    }
    return attemptId;
  }
}
