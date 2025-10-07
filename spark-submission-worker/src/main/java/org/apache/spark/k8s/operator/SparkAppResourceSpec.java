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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map;
import scala.jdk.CollectionConverters;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import org.apache.spark.deploy.k8s.Config;
import org.apache.spark.deploy.k8s.Constants;
import org.apache.spark.deploy.k8s.KubernetesDriverSpec;
import org.apache.spark.deploy.k8s.SparkPod;
import org.apache.spark.deploy.k8s.submit.KubernetesClientUtils;
import org.apache.spark.k8s.operator.spec.ConfigMapSpec;
import org.apache.spark.k8s.operator.spec.DriverServiceIngressSpec;
import org.apache.spark.k8s.operator.utils.ConfigMapSpecUtils;
import org.apache.spark.k8s.operator.utils.DriverServiceIngressUtils;

/**
 * Resembles resources that would be directly launched by operator. Based on resolved
 * org.apache.spark.deploy.k8s.KubernetesDriverSpec, it:
 *
 * <ul>
 *   <li>Add ConfigMap as a resource for driver
 *   <li>Converts scala types to Java for easier reference from operator
 * </ul>
 *
 * <p>This is not thread safe and not expected to be shared among reconciler threads.
 */
public class SparkAppResourceSpec {
  @Getter private final Pod configuredPod;
  @Getter private final List<HasMetadata> driverPreResources;
  @Getter private final List<HasMetadata> driverResources;
  private final SparkAppDriverConf kubernetesDriverConf;

  /**
   * Constructs a new SparkAppResourceSpec.
   *
   * @param kubernetesDriverConf The KubernetesDriverConf for the application.
   * @param kubernetesDriverSpec The KubernetesDriverSpec for the application.
   * @param driverServiceIngressList A list of DriverServiceIngressSpec for the driver service.
   * @param configMapSpecs A list of ConfigMapSpec for additional config maps.
   */
  public SparkAppResourceSpec(
      SparkAppDriverConf kubernetesDriverConf,
      KubernetesDriverSpec kubernetesDriverSpec,
      List<DriverServiceIngressSpec> driverServiceIngressList,
      List<ConfigMapSpec> configMapSpecs) {
    this.kubernetesDriverConf = kubernetesDriverConf;
    String namespace = kubernetesDriverConf.sparkConf().get(Config.KUBERNETES_NAMESPACE().key());
    Map<String, String> confFilesMap =
        KubernetesClientUtils.buildSparkConfDirFilesMap(
                kubernetesDriverConf.configMapNameDriver(),
                kubernetesDriverConf.sparkConf(),
                kubernetesDriverSpec.systemProperties())
            .$plus(new Tuple2<>(Config.KUBERNETES_NAMESPACE().key(), namespace));
    SparkPod sparkPod = addConfigMap(kubernetesDriverSpec.pod(), confFilesMap);
    this.configuredPod =
        new PodBuilder(sparkPod.pod())
            .editSpec()
            .addToContainers(sparkPod.container())
            .endSpec()
            .build();
    this.driverPreResources =
        new ArrayList<>(
            CollectionConverters.SeqHasAsJava(kubernetesDriverSpec.driverPreKubernetesResources())
                .asJava());
    this.driverResources =
        new ArrayList<>(
            CollectionConverters.SeqHasAsJava(kubernetesDriverSpec.driverKubernetesResources())
                .asJava());
    this.driverResources.add(
        KubernetesClientUtils.buildConfigMap(
            kubernetesDriverConf.configMapNameDriver(), confFilesMap, new HashMap<>()));
    this.driverPreResources.addAll(ConfigMapSpecUtils.buildConfigMaps(configMapSpecs));
    this.driverResources.addAll(configureDriverServerIngress(sparkPod, driverServiceIngressList));
    this.driverPreResources.forEach(r -> setNamespaceIfMissing(r, namespace));
    this.driverResources.forEach(r -> setNamespaceIfMissing(r, namespace));
  }

  /**
   * Sets the namespace for a given resource if it's not already set.
   *
   * @param resource The resource to set the namespace for.
   * @param namespace The namespace to set.
   */
  private void setNamespaceIfMissing(HasMetadata resource, String namespace) {
    if (StringUtils.isNotEmpty(resource.getMetadata().getNamespace())) {
      return;
    }
    resource.getMetadata().setNamespace(namespace);
  }

  /**
   * Adds a ConfigMap volume and mount to the SparkPod.
   *
   * @param pod The SparkPod to modify.
   * @param confFilesMap The map of configuration files.
   * @return The modified SparkPod.
   */
  private SparkPod addConfigMap(SparkPod pod, Map<String, String> confFilesMap) {
    Container containerWithConfigMapVolume =
        new ContainerBuilder(pod.container())
            .addNewEnv()
            .withName(Constants.ENV_SPARK_CONF_DIR())
            .withValue(Constants.SPARK_CONF_DIR_INTERNAL())
            .endEnv()
            .addNewVolumeMount()
            .withName(Constants.SPARK_CONF_VOLUME_DRIVER())
            .withMountPath(Constants.SPARK_CONF_DIR_INTERNAL())
            .endVolumeMount()
            .build();
    Pod podWithConfigMapVolume =
        new PodBuilder(pod.pod())
            .editSpec()
            .addNewVolume()
            .withName(Constants.SPARK_CONF_VOLUME_DRIVER())
            .withNewConfigMap()
            .withItems(
                CollectionConverters.SeqHasAsJava(
                        KubernetesClientUtils.buildKeyToPathObjects(confFilesMap))
                    .asJava())
            .withName(kubernetesDriverConf.configMapNameDriver())
            .endConfigMap()
            .endVolume()
            .endSpec()
            .build();
    return new SparkPod(podWithConfigMapVolume, containerWithConfigMapVolume);
  }

  /**
   * Configures driver service ingress resources.
   *
   * @param pod The SparkPod for the driver.
   * @param driverServiceIngressList A list of DriverServiceIngressSpec.
   * @return A List of HasMetadata objects representing the ingress resources.
   */
  private List<HasMetadata> configureDriverServerIngress(
      SparkPod pod, List<DriverServiceIngressSpec> driverServiceIngressList) {
    if (driverServiceIngressList == null || driverServiceIngressList.isEmpty()) {
      return List.of();
    }
    return driverServiceIngressList.stream()
        .map(spec -> DriverServiceIngressUtils.buildIngressService(spec, pod.pod().getMetadata()))
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }
}
