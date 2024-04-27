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
 */

package org.apache.spark.kubernetes.operator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import scala.collection.Seq;
import scala.collection.immutable.HashMap;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.deploy.k8s.KubernetesDriverSpec;
import org.apache.spark.deploy.k8s.SparkPod;

class SparkAppResourceSpecTest {

  @Test
  void testDriverResourceIncludesConfigMap() {
    SparkAppDriverConf mockConf = mock(SparkAppDriverConf.class);
    when(mockConf.configMapNameDriver()).thenReturn("foo-configmap");
    when(mockConf.sparkConf())
        .thenReturn(new SparkConf().set("spark.kubernetes.namespace", "foo-namespace"));

    KubernetesDriverSpec mockSpec = mock(KubernetesDriverSpec.class);
    Container container =
        new ContainerBuilder()
            .withName("foo-container")
            .addNewVolumeMount()
            .withName("placeholder")
            .endVolumeMount()
            .build();
    Pod pod =
        new PodBuilder()
            .withNewMetadata()
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
    SparkPod sparkPod = new SparkPod(pod, container);
    List<HasMetadata> emptyList = Collections.emptyList();
    Seq<HasMetadata> emptySeq =
        scala.collection.JavaConverters.collectionAsScalaIterableConverter(emptyList)
            .asScala()
            .toSeq();
    when(mockSpec.driverKubernetesResources()).thenReturn(emptySeq);
    when(mockSpec.driverPreKubernetesResources()).thenReturn(emptySeq);
    when(mockSpec.pod()).thenReturn(sparkPod);
    when(mockSpec.systemProperties()).thenReturn(new HashMap<>());

    SparkAppResourceSpec appResourceSpec = new SparkAppResourceSpec(mockConf, mockSpec);

    Assertions.assertEquals(1, appResourceSpec.getDriverResources().size());
    Assertions.assertEquals(
        ConfigMap.class, appResourceSpec.getDriverResources().get(0).getClass());

    ConfigMap proposedConfigMap = (ConfigMap) appResourceSpec.getDriverResources().get(0);
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
}
