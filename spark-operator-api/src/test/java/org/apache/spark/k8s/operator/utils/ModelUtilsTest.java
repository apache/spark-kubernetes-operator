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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.spec.BaseApplicationTemplateSpec;

class ModelUtilsTest {

  private PodTemplateSpec buildSamplePodTemplateSpec() {
    return new PodTemplateSpecBuilder()
        .withNewMetadata()
        .withName("foo-pod-name")
        .withNamespace("foo-namespace-name")
        .withLabels(Map.of("foo", "bar"))
        .withAnnotations(Map.of("foo", "bar"))
        .endMetadata()
        .withNewSpec()
        .addNewInitContainer()
        .withName("foo-init-container")
        .withImage("foo-image")
        .endInitContainer()
        .addNewContainer()
        .withName("foo-container-1")
        .withImage("foo-image")
        .endContainer()
        .addNewContainer()
        .withName("foo-container-2")
        .withImage("bar-image")
        .endContainer()
        .endSpec()
        .addToAdditionalProperties("foo", "bar")
        .build();
  }

  @Test
  void testGetPodFromTemplateSpec() {
    PodTemplateSpec podTemplateSpec = buildSamplePodTemplateSpec();
    Pod pod = ModelUtils.getPodFromTemplateSpec(podTemplateSpec);
    Assertions.assertEquals(podTemplateSpec.getMetadata(), pod.getMetadata());
    Assertions.assertEquals(podTemplateSpec.getSpec(), pod.getSpec());
    Assertions.assertEquals(
        podTemplateSpec.getAdditionalProperties(), pod.getAdditionalProperties());
  }

  @Test
  void testFindDriverMainContainerStatus() {
    ContainerStatus containerStatus1 =
        new ContainerStatusBuilder().withName("foo-container-1").build();
    ContainerStatus containerStatus2 =
        new ContainerStatusBuilder().withName("foo-container-2").build();
    ApplicationSpec specWithoutSparkContainerNameProp = new ApplicationSpec();
    List<ContainerStatus> sparkContainerStatusList =
        ModelUtils.findDriverMainContainerStatus(
            specWithoutSparkContainerNameProp, List.of(containerStatus1, containerStatus2));
    Assertions.assertEquals(List.of(containerStatus1, containerStatus2), sparkContainerStatusList);

    ApplicationSpec specWithSparkContainerNameProp = new ApplicationSpec();
    Map<String, String> sparkProps = new HashMap<>();
    sparkProps.put("spark.kubernetes.driver.podTemplateContainerName", "foo-container-2");
    specWithSparkContainerNameProp.setSparkConf(sparkProps);
    sparkContainerStatusList =
        ModelUtils.findDriverMainContainerStatus(
            specWithSparkContainerNameProp, List.of(containerStatus1, containerStatus2));
    Assertions.assertEquals(List.of(containerStatus2), sparkContainerStatusList);
  }

  @Test
  void testOverrideDriverTemplateEnabled() {
    ApplicationSpec applicationSpec = new ApplicationSpec();
    Assertions.assertFalse(ModelUtils.overrideDriverTemplateEnabled(applicationSpec));

    BaseApplicationTemplateSpec driverSpec = new BaseApplicationTemplateSpec();
    applicationSpec.setDriverSpec(driverSpec);
    Assertions.assertFalse(ModelUtils.overrideDriverTemplateEnabled(applicationSpec));

    driverSpec.setPodTemplateSpec(buildSamplePodTemplateSpec());
    applicationSpec.setDriverSpec(driverSpec);
    Assertions.assertTrue(ModelUtils.overrideDriverTemplateEnabled(applicationSpec));
  }

  @Test
  void testOverrideExecutorTemplateEnabled() {
    ApplicationSpec applicationSpec = new ApplicationSpec();
    Assertions.assertFalse(ModelUtils.overrideDriverTemplateEnabled(applicationSpec));

    BaseApplicationTemplateSpec executorSpec = new BaseApplicationTemplateSpec();
    applicationSpec.setExecutorSpec(executorSpec);
    Assertions.assertFalse(ModelUtils.overrideDriverTemplateEnabled(applicationSpec));

    executorSpec.setPodTemplateSpec(buildSamplePodTemplateSpec());
    applicationSpec.setDriverSpec(executorSpec);
    Assertions.assertTrue(ModelUtils.overrideDriverTemplateEnabled(applicationSpec));
  }
}
