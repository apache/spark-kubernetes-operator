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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.spec.BaseApplicationTemplateSpec;
import org.apache.spark.k8s.operator.spec.DeploymentMode;

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
    sparkProps.put("spark.kubernetes.driver.podTemplateFile", "s3a://bucket/driver.yaml");
    specWithSparkContainerNameProp.setSparkConf(sparkProps);
    sparkContainerStatusList =
        ModelUtils.findDriverMainContainerStatus(
            specWithSparkContainerNameProp, List.of(containerStatus1, containerStatus2));
    Assertions.assertEquals(List.of(containerStatus2), sparkContainerStatusList);
  }

  @Test
  void testFindDriverMainContainerStatusWithoutPodTemplate() {
    ContainerStatus sidecarStatus = new ContainerStatusBuilder().withName("istio-proxy").build();
    ContainerStatus driverStatus =
        new ContainerStatusBuilder().withName("spark-kubernetes-driver").build();
    Assertions.assertEquals(
        List.of(driverStatus),
        ModelUtils.findDriverMainContainerStatus(
            new ApplicationSpec(), List.of(sidecarStatus, driverStatus)));

    ApplicationSpec spec = new ApplicationSpec();
    spec.getSparkConf().put("spark.kubernetes.driver.podTemplateContainerName", "driver");
    Assertions.assertEquals(
        List.of(driverStatus),
        ModelUtils.findDriverMainContainerStatus(spec, List.of(sidecarStatus, driverStatus)));
  }

  @Test
  void testFindDriverMainContainerStatusWithPodTemplate() {
    ContainerStatus containerStatus1 =
        new ContainerStatusBuilder().withName("foo-container-1").build();
    ContainerStatus containerStatus2 =
        new ContainerStatusBuilder().withName("foo-container-2").build();
    List<ContainerStatus> containerStatusList = List.of(containerStatus2, containerStatus1);
    ApplicationSpec spec = new ApplicationSpec();
    spec.setDriverSpec(new BaseApplicationTemplateSpec(buildSamplePodTemplateSpec()));
    Assertions.assertEquals(
        List.of(containerStatus1),
        ModelUtils.findDriverMainContainerStatus(spec, containerStatusList));

    spec.getSparkConf().put("spark.kubernetes.driver.podTemplateContainerName", "foo-container-2");
    Assertions.assertEquals(
        List.of(containerStatus2),
        ModelUtils.findDriverMainContainerStatus(spec, containerStatusList));

    spec.getSparkConf().put("spark.kubernetes.driver.podTemplateContainerName", "bar-container");
    Assertions.assertEquals(
        List.of(containerStatus1),
        ModelUtils.findDriverMainContainerStatus(spec, containerStatusList));
  }

  @Test
  void testFindDriverMainContainerStatusWithUnnamedContainerInPodTemplate() {
    ApplicationSpec spec = new ApplicationSpec();
    spec.setDriverSpec(
        new BaseApplicationTemplateSpec(
            new PodTemplateSpecBuilder()
                .withNewSpec()
                .addNewContainer()
                .endContainer()
                .addNewContainer()
                .withName("sidecar")
                .endContainer()
                .endSpec()
                .build()));
    ContainerStatus sidecarStatus = new ContainerStatusBuilder().withName("sidecar").build();
    ContainerStatus driverStatus =
        new ContainerStatusBuilder().withName("spark-kubernetes-driver").build();
    Assertions.assertEquals(
        List.of(driverStatus),
        ModelUtils.findDriverMainContainerStatus(spec, List.of(sidecarStatus, driverStatus)));
  }

  @Test
  void testOverrideDriverTemplateEnabled() {
    ApplicationSpec applicationSpec = new ApplicationSpec();
    assertFalse(ModelUtils.overrideDriverTemplateEnabled(applicationSpec));

    BaseApplicationTemplateSpec driverSpec = new BaseApplicationTemplateSpec();
    applicationSpec.setDriverSpec(driverSpec);
    assertFalse(ModelUtils.overrideDriverTemplateEnabled(applicationSpec));

    driverSpec.setPodTemplateSpec(buildSamplePodTemplateSpec());
    applicationSpec.setDriverSpec(driverSpec);
    assertTrue(ModelUtils.overrideDriverTemplateEnabled(applicationSpec));
  }

  @Test
  void testOverrideExecutorTemplateEnabled() {
    ApplicationSpec applicationSpec = new ApplicationSpec();
    assertFalse(ModelUtils.overrideDriverTemplateEnabled(applicationSpec));

    BaseApplicationTemplateSpec executorSpec = new BaseApplicationTemplateSpec();
    applicationSpec.setExecutorSpec(executorSpec);
    assertFalse(ModelUtils.overrideDriverTemplateEnabled(applicationSpec));

    executorSpec.setPodTemplateSpec(buildSamplePodTemplateSpec());
    applicationSpec.setDriverSpec(executorSpec);
    assertTrue(ModelUtils.overrideDriverTemplateEnabled(applicationSpec));
  }

  @Test
  void testIsClientMode() {
    SparkApplication app = new SparkApplication();
    ApplicationSpec spec = new ApplicationSpec();

    spec.setDeploymentMode(DeploymentMode.ClientMode);
    app.setSpec(spec);
    assertTrue(ModelUtils.isClientMode(app));

    spec.setDeploymentMode(DeploymentMode.ClusterMode);
    app.setSpec(spec);
    assertFalse(ModelUtils.isClientMode(app));
  }
}
