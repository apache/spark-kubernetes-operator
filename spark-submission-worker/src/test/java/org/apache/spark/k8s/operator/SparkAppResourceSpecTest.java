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

  private static final String DRIVER_CONTAINER_NAME = "foo-container";

  @Test
  void testDriverResourceIncludesConfigMap() {
    SparkAppDriverConf mockConf = mockDriverConf(null, Map.of());

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
    Container configured = buildDriverContainer("alice", Map.of(), "submitter");
    Assertions.assertEquals("alice", sparkUserValue(configured));
  }

  @Test
  void driverSparkUserShouldBeUntouchedWhenProxyUserAbsent() {
    Container configured = buildDriverContainer(null, Map.of(), "submitter");
    Assertions.assertEquals("submitter", sparkUserValue(configured));
  }

  @Test
  void driverSparkUserShouldRespectExplicitDriverEnvOverride() {
    // If the user explicitly set spark.kubernetes.driverEnv.SPARK_USER, honor it over
    // --proxy-user. Without this, driver identity would silently diverge from executor
    // identity (spark.executorEnv.SPARK_USER is not touched here).
    Container configured =
        buildDriverContainer("alice", Map.of(Constants.ENV_SPARK_USER(), "carol"), "carol");
    Assertions.assertEquals("carol", sparkUserValue(configured));
  }

  @Test
  void driverSparkUserOverrideShouldPreservePosition() {
    // BasicDriverFeatureStep puts SPARK_USER before driverCustomEnvs so that entries like
    // spark.kubernetes.driverEnv.HADOOP_USER_NAME=$(SPARK_USER) can reference it. kubelet
    // resolves $(VAR) only against variables earlier in the env list, so the override must
    // leave SPARK_USER at its original position.
    Container driverContainer =
        new ContainerBuilder()
            .withName(DRIVER_CONTAINER_NAME)
            .addNewVolumeMount()
            .withName("placeholder")
            .endVolumeMount()
            .addNewEnv()
            .withName(Constants.ENV_SPARK_USER())
            .withValue("submitter")
            .endEnv()
            .addNewEnv()
            .withName("HADOOP_USER_NAME")
            .withValue("$(SPARK_USER)")
            .endEnv()
            .build();
    SparkAppDriverConf mockConf = mockDriverConf("alice", Map.of());
    SparkPod sparkPod = new SparkPod(buildBasicPod("driver"), driverContainer);
    KubernetesDriverSpec spec =
        KubernetesDriverSpec.create(sparkPod, List.of(), List.of(), Map.of());

    SparkAppResourceSpec appResourceSpec =
        new SparkAppResourceSpec(mockConf, spec, List.of(), List.of(), List.of(), List.of());

    Container configured = driverContainer(appResourceSpec);
    List<String> envNames = configured.getEnv().stream().map(EnvVar::getName).toList();
    Assertions.assertTrue(
        envNames.indexOf(Constants.ENV_SPARK_USER()) < envNames.indexOf("HADOOP_USER_NAME"),
        "SPARK_USER must remain before HADOOP_USER_NAME so $(SPARK_USER) resolves: " + envNames);
    Assertions.assertEquals("alice", sparkUserValue(configured));
  }

  private Container buildDriverContainer(
      String proxyUser, Map<String, String> driverEnv, String initialSparkUser) {
    Container driverContainer =
        new ContainerBuilder(buildBasicContainer())
            .addNewEnv()
            .withName(Constants.ENV_SPARK_USER())
            .withValue(initialSparkUser)
            .endEnv()
            .build();
    SparkAppDriverConf mockConf = mockDriverConf(proxyUser, driverEnv);
    SparkPod sparkPod = new SparkPod(buildBasicPod("driver"), driverContainer);
    KubernetesDriverSpec spec =
        KubernetesDriverSpec.create(sparkPod, List.of(), List.of(), Map.of());
    SparkAppResourceSpec appResourceSpec =
        new SparkAppResourceSpec(mockConf, spec, List.of(), List.of(), List.of(), List.of());
    return driverContainer(appResourceSpec);
  }

  private static SparkAppDriverConf mockDriverConf(
      String proxyUser, Map<String, String> driverEnv) {
    SparkAppDriverConf mockConf = mock(SparkAppDriverConf.class);
    when(mockConf.configMapNameDriver()).thenReturn("foo-configmap");
    when(mockConf.sparkConf())
        .thenReturn(new SparkConf().set("spark.kubernetes.namespace", "foo-namespace"));
    when(mockConf.proxyUser()).thenReturn(scala.Option.apply(proxyUser));
    when(mockConf.environment()).thenReturn(toScalaMap(driverEnv));
    return mockConf;
  }

  private static scala.collection.immutable.Map<String, String> toScalaMap(
      Map<String, String> javaMap) {
    scala.collection.immutable.Map<String, String> scalaMap =
        scala.collection.immutable.Map$.MODULE$.empty();
    for (Map.Entry<String, String> entry : javaMap.entrySet()) {
      scalaMap = scalaMap.updated(entry.getKey(), entry.getValue());
    }
    return scalaMap;
  }

  private static Container driverContainer(SparkAppResourceSpec appResourceSpec) {
    return appResourceSpec.getConfiguredPod().getSpec().getContainers().stream()
        .filter(c -> DRIVER_CONTAINER_NAME.equals(c.getName()))
        .findFirst()
        .orElseThrow(() -> new AssertionError("driver container not found"));
  }

  private static String sparkUserValue(Container container) {
    List<EnvVar> matches =
        container.getEnv().stream()
            .filter(e -> Constants.ENV_SPARK_USER().equals(e.getName()))
            .toList();
    Assertions.assertEquals(1, matches.size(), "SPARK_USER should appear exactly once");
    return matches.get(0).getValue();
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
