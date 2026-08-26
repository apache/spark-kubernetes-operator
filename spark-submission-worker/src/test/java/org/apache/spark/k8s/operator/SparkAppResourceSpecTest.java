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

package org.apache.spark.k8s.operator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.deploy.k8s.Constants;
import org.apache.spark.deploy.k8s.KubernetesDriverSpec;
import org.apache.spark.deploy.k8s.SparkPod;

class SparkAppResourceSpecTest {

  @Test
  void testDriverResourceIncludesConfigMap() {
    SparkAppDriverConf mockConf = mock(SparkAppDriverConf.class);
    when(mockConf.configMapNameDriver()).thenReturn("foo-configmap");
    when(mockConf.sparkConf())
        .thenReturn(new SparkConf().set("spark.kubernetes.namespace", "foo-namespace"));
    when(mockConf.proxyUser()).thenReturn(scala.Option.empty());

    Pod driver = buildBasicPod("driver");
    SparkPod sparkPod = new SparkPod(driver, buildBasicContainer());

    // Add some mock resources and pre-resources
    Pod pod1 = buildBasicPod("pod-1");
    Pod pod2 = buildBasicPod("pod-2");
    List<HasMetadata> preResourceList = List.of(pod1);
    List<HasMetadata> resourceList = List.of(pod2);
    KubernetesDriverSpec spec =
        KubernetesDriverSpec.create(sparkPod, preResourceList, resourceList, Map.of());

    SparkAppResourceSpec appResourceSpec =
        new SparkAppResourceSpec(mockConf, spec, List.of(), List.of(), List.of(), List.of());

    Assertions.assertEquals(2, appResourceSpec.getDriverResources().size());
    Assertions.assertEquals(1, appResourceSpec.getDriverPreResources().size());
    Assertions.assertEquals(Pod.class, appResourceSpec.getDriverResources().get(0).getClass());
    Assertions.assertEquals(
        ConfigMap.class, appResourceSpec.getDriverResources().get(1).getClass());
    Assertions.assertEquals(pod1, appResourceSpec.getDriverPreResources().get(0));
    Assertions.assertEquals(pod2, appResourceSpec.getDriverResources().get(0));

    ConfigMap proposedConfigMap = (ConfigMap) appResourceSpec.getDriverResources().get(1);
    Assertions.assertEquals("foo-configmap", proposedConfigMap.getMetadata().getName());
    Assertions.assertEquals(
        "foo-namespace", proposedConfigMap.getData().get("spark.kubernetes.namespace"));
    Assertions.assertEquals("foo-namespace", proposedConfigMap.getMetadata().getNamespace());

    Assertions.assertEquals(2, appResourceSpec.getConfiguredPod().getSpec().getVolumes().size());
    Volume proposedConfigVolume = appResourceSpec.getConfiguredPod().getSpec().getVolumes().get(1);
    Assertions.assertEquals("foo-configmap", proposedConfigVolume.getConfigMap().getName());

    Assertions.assertEquals(2, appResourceSpec.getConfiguredPod().getSpec().getContainers().size());
    Assertions.assertEquals(
        2,
        appResourceSpec
            .getConfiguredPod()
            .getSpec()
            .getContainers()
            .get(1)
            .getVolumeMounts()
            .size());
    VolumeMount proposedConfigVolumeMount =
        appResourceSpec
            .getConfiguredPod()
            .getSpec()
            .getContainers()
            .get(1)
            .getVolumeMounts()
            .get(1);
    Assertions.assertEquals(proposedConfigVolume.getName(), proposedConfigVolumeMount.getName());
  }

  @Test
  void driverSparkUserShouldReflectProxyUserWhenSet() {
    SparkAppDriverConf mockConf = mock(SparkAppDriverConf.class);
    when(mockConf.configMapNameDriver()).thenReturn("foo-configmap");
    when(mockConf.sparkConf())
        .thenReturn(new SparkConf().set("spark.kubernetes.namespace", "foo-namespace"));
    when(mockConf.proxyUser()).thenReturn(scala.Option.apply("alice"));

    Container driverContainer =
        new ContainerBuilder(buildBasicContainer())
            .addNewEnv()
            .withName(Constants.ENV_SPARK_USER())
            .withValue("submitter")
            .endEnv()
            .build();
    SparkPod sparkPod = new SparkPod(buildBasicPod("driver"), driverContainer);
    KubernetesDriverSpec spec =
        KubernetesDriverSpec.create(sparkPod, List.of(), List.of(), Map.of());

    SparkAppResourceSpec appResourceSpec =
        new SparkAppResourceSpec(mockConf, spec, List.of(), List.of(), List.of(), List.of());

    Container configured =
        appResourceSpec.getConfiguredPod().getSpec().getContainers().get(1);
    List<EnvVar> sparkUserEnvs =
        configured.getEnv().stream()
            .filter(e -> Constants.ENV_SPARK_USER().equals(e.getName()))
            .collect(Collectors.toList());
    Assertions.assertEquals(1, sparkUserEnvs.size(), "SPARK_USER should appear exactly once");
    Assertions.assertEquals("alice", sparkUserEnvs.get(0).getValue());
  }

  @Test
  void driverSparkUserShouldBeUntouchedWhenProxyUserAbsent() {
    SparkAppDriverConf mockConf = mock(SparkAppDriverConf.class);
    when(mockConf.configMapNameDriver()).thenReturn("foo-configmap");
    when(mockConf.sparkConf())
        .thenReturn(new SparkConf().set("spark.kubernetes.namespace", "foo-namespace"));
    when(mockConf.proxyUser()).thenReturn(scala.Option.empty());

    Container driverContainer =
        new ContainerBuilder(buildBasicContainer())
            .addNewEnv()
            .withName(Constants.ENV_SPARK_USER())
            .withValue("submitter")
            .endEnv()
            .build();
    SparkPod sparkPod = new SparkPod(buildBasicPod("driver"), driverContainer);
    KubernetesDriverSpec spec =
        KubernetesDriverSpec.create(sparkPod, List.of(), List.of(), Map.of());

    SparkAppResourceSpec appResourceSpec =
        new SparkAppResourceSpec(mockConf, spec, List.of(), List.of(), List.of(), List.of());

    Container configured =
        appResourceSpec.getConfiguredPod().getSpec().getContainers().get(1);
    List<EnvVar> sparkUserEnvs =
        configured.getEnv().stream()
            .filter(e -> Constants.ENV_SPARK_USER().equals(e.getName()))
            .collect(Collectors.toList());
    Assertions.assertEquals(1, sparkUserEnvs.size());
    Assertions.assertEquals(
        "submitter",
        sparkUserEnvs.get(0).getValue(),
        "SPARK_USER should be untouched when no proxy user is configured");
  }

  protected Container buildBasicContainer() {
    return new ContainerBuilder()
        .withName("foo-container")
        .addNewVolumeMount()
        .withName("placeholder")
        .endVolumeMount()
        .build();
  }

  protected Pod buildBasicPod(String name) {
    return new PodBuilder()
        .withNewMetadata()
        .withName(name)
        .endMetadata()
        .withNewSpec()
        .addNewContainer()
        .withName("placeholder")
        .endContainer()
        .addNewVolume()
        .withName("placeholder")
        .endVolume()
        .endSpec()
        .build();
  }
}
