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

import java.util.List;
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
import org.apache.commons.lang3.StringUtils;

import org.apache.spark.k8s.operator.spec.ApplicationSpec;

public class ModelUtils {
  public static final String DRIVER_SPARK_CONTAINER_PROP_KEY =
      "spark.kubernetes.driver.podTemplateContainerName";
  public static final String DRIVER_SPARK_TEMPLATE_FILE_PROP_KEY =
      "spark.kubernetes.driver.podTemplateFile";
  public static final String EXECUTOR_SPARK_TEMPLATE_FILE_PROP_KEY =
      "spark.kubernetes.executor.podTemplateFile";
  public static final ObjectMapper objectMapper = new ObjectMapper();

  public static Pod getPodFromTemplateSpec(PodTemplateSpec podTemplateSpec) {
    if (podTemplateSpec != null) {
      return new PodBuilder()
          .withMetadata(podTemplateSpec.getMetadata())
          .withSpec(podTemplateSpec.getSpec())
          .withAdditionalProperties(podTemplateSpec.getAdditionalProperties())
          .build();
    } else {
      return new PodBuilder().withNewMetadata().endMetadata().withNewSpec().endSpec().build();
    }
  }

  /**
   * Find the Spark main container(s) in driver pod. If `spark.kubernetes.driver
   * .podTemplateContainerName` is not set, all containers are considered as main container from
   * health monitoring perspective
   */
  public static List<ContainerStatus> findDriverMainContainerStatus(
      final ApplicationSpec appSpec, final List<ContainerStatus> containerStatusList) {
    if (appSpec == null
        || appSpec.getSparkConf() == null
        || !appSpec.getSparkConf().containsKey(DRIVER_SPARK_CONTAINER_PROP_KEY)) {
      return containerStatusList;
    }
    String mainContainerName = appSpec.getSparkConf().get(DRIVER_SPARK_CONTAINER_PROP_KEY);
    if (StringUtils.isEmpty(mainContainerName)) {
      return containerStatusList;
    }
    return containerStatusList.stream()
        .filter(c -> mainContainerName.equalsIgnoreCase(c.getName()))
        .collect(Collectors.toList());
  }

  /**
   * Build OwnerReference to the given resource
   *
   * @param owner the owner
   * @return OwnerReference to be used for subresources
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

  public static <T extends HasMetadata> String asJsonString(T resource) {
    try {
      return objectMapper.writeValueAsString(resource);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static boolean overrideDriverTemplateEnabled(ApplicationSpec applicationSpec) {
    return applicationSpec != null
        && applicationSpec.getDriverSpec() != null
        && applicationSpec.getDriverSpec().getPodTemplateSpec() != null;
  }

  public static boolean overrideExecutorTemplateEnabled(ApplicationSpec applicationSpec) {
    return applicationSpec != null
        && applicationSpec.getExecutorSpec() != null
        && applicationSpec.getExecutorSpec().getPodTemplateSpec() != null;
  }
}
