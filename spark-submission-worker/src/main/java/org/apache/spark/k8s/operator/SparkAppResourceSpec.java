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

import java.util.*;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import lombok.Getter;

import org.apache.spark.deploy.k8s.Config;
import org.apache.spark.deploy.k8s.Constants;
import org.apache.spark.deploy.k8s.KubernetesDriverSpec;
import org.apache.spark.deploy.k8s.SparkPod;
import org.apache.spark.deploy.k8s.submit.KubernetesClientUtils;
import org.apache.spark.k8s.operator.spec.ConfigMapSpec;
import org.apache.spark.k8s.operator.spec.DriverGrpcRouteSpec;
import org.apache.spark.k8s.operator.spec.DriverHttpRouteSpec;
import org.apache.spark.k8s.operator.spec.DriverServiceIngressSpec;
import org.apache.spark.k8s.operator.utils.ConfigMapSpecUtils;
import org.apache.spark.k8s.operator.utils.DriverGatewayRouteUtils;
import org.apache.spark.k8s.operator.utils.DriverServiceIngressUtils;
import org.apache.spark.k8s.operator.utils.StringUtils;

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
   * @param driverHttpRouteList A list of DriverHttpRouteSpec for Gateway API HTTPRoutes.
   * @param driverGrpcRouteList A list of DriverGrpcRouteSpec for Gateway API GRPCRoutes.
   * @param configMapSpecs A list of ConfigMapSpec for additional config maps.
   */
  public SparkAppResourceSpec(
      SparkAppDriverConf kubernetesDriverConf,
      KubernetesDriverSpec kubernetesDriverSpec,
      List<DriverServiceIngressSpec> driverServiceIngressList,
      List<DriverHttpRouteSpec> driverHttpRouteList,
      List<DriverGrpcRouteSpec> driverGrpcRouteList,
      List<ConfigMapSpec> configMapSpecs) {
    this.kubernetesDriverConf = kubernetesDriverConf;
    String namespace = kubernetesDriverConf.sparkConf().get(Config.KUBERNETES_NAMESPACE().key());
    Map<String, String> originalConfFilesMap =
        KubernetesClientUtils.buildSparkConfDirFilesMapJava(
                kubernetesDriverConf.configMapNameDriver(),
                kubernetesDriverConf.sparkConf(),
                kubernetesDriverSpec.getSystemPropertiesAsJavaMap());
    Map<String, String> confFilesMap = new HashMap<>(originalConfFilesMap);
    confFilesMap.put(Config.KUBERNETES_NAMESPACE().key(), namespace);
    SparkPod sparkPod = addConfigMap(kubernetesDriverSpec.pod(), confFilesMap);
    sparkPod = overrideSparkUserForProxyUser(sparkPod);
    this.configuredPod =
        new PodBuilder(sparkPod.pod())
            .editSpec()
            .addToContainers(sparkPod.container())
            .endSpec()
            .build();
    this.driverPreResources =
        new ArrayList<>(kubernetesDriverSpec.getDriverPreKubernetesResourcesAsJavaList());
    this.driverResources =
        new ArrayList<>(kubernetesDriverSpec.getDriverKubernetesResourcesAsJavaList());
    this.driverResources.add(
        KubernetesClientUtils.buildConfigMapJava(
            kubernetesDriverConf.configMapNameDriver(), confFilesMap, Map.of()));
    this.driverPreResources.addAll(ConfigMapSpecUtils.buildConfigMaps(configMapSpecs));
    this.driverResources.addAll(configureDriverServerIngress(sparkPod, driverServiceIngressList));
    this.driverResources.addAll(configureDriverHttpRoutes(sparkPod, driverHttpRouteList));
    this.driverResources.addAll(configureDriverGrpcRoutes(sparkPod, driverGrpcRouteList));
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
            .withItems(KubernetesClientUtils.buildKeyToPathObjectsJava(confFilesMap))
            .withName(kubernetesDriverConf.configMapNameDriver())
            .endConfigMap()
            .endVolume()
            .endSpec()
            .build();
    return new SparkPod(podWithConfigMapVolume, containerWithConfigMapVolume);
  }

  /**
   * Overrides the {@code SPARK_USER} environment variable on the driver container with the
   * proxy user when one is configured.
   *
   * <p>Background: In {@code spark-submit}, {@code SparkSubmit} wraps {@code runMain} in
   * {@code proxyUser.doAs(...)}, so {@code Utils.getCurrentUserName()} inside
   * {@link org.apache.spark.deploy.k8s.features.BasicDriverFeatureStep} returns the proxy user.
   * The operator builds the driver pod spec by invoking the feature steps directly, without an
   * equivalent {@code doAs}, so {@code SPARK_USER} would otherwise be the operator's identity
   * rather than the effective one. Note that {@code Utils.getCurrentUserName()} reads the
   * {@code SPARK_USER} environment variable in preference to the UGI short user name, and the
   * default Helm chart exports {@code SPARK_USER=spark} into the operator container; a
   * {@code doAs} wrapper alone would therefore not correct the driver env, which is why the
   * override is applied on the container spec here.
   *
   * <p>Replaces the entry in place so ordering established by {@code BasicDriverFeatureStep} is
   * preserved. Ordering matters because kubelet resolves {@code $(VAR)} references only against
   * variables defined earlier in the env list; moving {@code SPARK_USER} after
   * {@code driverCustomEnvs} would break any {@code spark.kubernetes.driverEnv.*} value that
   * uses {@code $(SPARK_USER)}.
   *
   * <p>If the user has explicitly set {@code spark.kubernetes.driverEnv.SPARK_USER}, the
   * explicit value wins and the proxy user is not applied to the driver container. The
   * override is a fallback for the default case where {@code SPARK_USER} is stamped by the
   * feature step.
   *
   * <p>Known limitation: this only patches the driver container env. Kerberos delegation
   * tokens minted by {@code KerberosConfDriverFeatureStep} are still obtained from the
   * operator's UGI, i.e. as the operator principal rather than the proxy user. Applications
   * that need proxy-user delegation tokens on kerberized clusters must supply the tokens via
   * an existing secret.
   */
  private SparkPod overrideSparkUserForProxyUser(SparkPod pod) {
    scala.Option<String> proxyUser = kubernetesDriverConf.proxyUser();
    if (proxyUser.isEmpty()) {
      return pod;
    }
    if (kubernetesDriverConf.environment().contains(Constants.ENV_SPARK_USER())) {
      return pod;
    }
    List<EnvVar> patchedEnv = new ArrayList<>(pod.container().getEnv());
    boolean replaced = false;
    for (int i = 0; i < patchedEnv.size(); i++) {
      if (Constants.ENV_SPARK_USER().equals(patchedEnv.get(i).getName())) {
        patchedEnv.set(
            i, new EnvVarBuilder(patchedEnv.get(i)).withValue(proxyUser.get()).build());
        replaced = true;
        break;
      }
    }
    if (!replaced) {
      patchedEnv.add(
          0,
          new EnvVarBuilder()
              .withName(Constants.ENV_SPARK_USER())
              .withValue(proxyUser.get())
              .build());
    }
    Container containerWithSparkUser =
        new ContainerBuilder(pod.container()).withEnv(patchedEnv).build();
    return new SparkPod(pod.pod(), containerWithSparkUser);
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

  /**
   * Configures driver Gateway API HTTPRoute resources.
   *
   * @param pod The SparkPod for the driver.
   * @param driverHttpRouteList A list of DriverHttpRouteSpec.
   * @return A List of HasMetadata objects representing the HTTPRoute + Service resources.
   */
  private List<HasMetadata> configureDriverHttpRoutes(
      SparkPod pod, List<DriverHttpRouteSpec> driverHttpRouteList) {
    if (driverHttpRouteList == null || driverHttpRouteList.isEmpty()) {
      return List.of();
    }
    return driverHttpRouteList.stream()
        .map(spec -> DriverGatewayRouteUtils.buildHttpRouteService(spec, pod.pod().getMetadata()))
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  /**
   * Configures driver Gateway API GRPCRoute resources.
   *
   * @param pod The SparkPod for the driver.
   * @param driverGrpcRouteList A list of DriverGrpcRouteSpec.
   * @return A List of HasMetadata objects representing the GRPCRoute + Service resources.
   */
  private List<HasMetadata> configureDriverGrpcRoutes(
      SparkPod pod, List<DriverGrpcRouteSpec> driverGrpcRouteList) {
    if (driverGrpcRouteList == null || driverGrpcRouteList.isEmpty()) {
      return List.of();
    }
    return driverGrpcRouteList.stream()
        .map(spec -> DriverGatewayRouteUtils.buildGrpcRouteService(spec, pod.pod().getMetadata()))
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }
}
