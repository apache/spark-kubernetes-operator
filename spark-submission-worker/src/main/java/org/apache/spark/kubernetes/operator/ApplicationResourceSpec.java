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
 *
 */

package org.apache.spark.kubernetes.operator;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import lombok.Getter;
import org.apache.spark.deploy.k8s.Config;
import org.apache.spark.deploy.k8s.Constants;
import org.apache.spark.deploy.k8s.KubernetesDriverSpec;
import org.apache.spark.deploy.k8s.SparkPod;
import org.apache.spark.deploy.k8s.submit.KubernetesClientUtils;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map;

import java.util.ArrayList;
import java.util.List;

/**
 * Resembles resources that would be directly launched by operator.
 * Operator would later create
 * This includes below task:
 * + Add ConfigMap as a pre-resource for driver
 * + Converts scala types to Java for easier reference
 * <p>
 * This is not thread safe
 */
public class ApplicationResourceSpec {
    @Getter
    private final Pod configuredPod;
    @Getter
    private final List<HasMetadata> driverPreResources;
    @Getter
    private final List<HasMetadata> driverResources;
    private final ApplicationDriverConf kubernetesDriverConf;

    public ApplicationResourceSpec(ApplicationDriverConf kubernetesDriverConf,
                                   KubernetesDriverSpec kubernetesDriverSpec) {
        this.kubernetesDriverConf = kubernetesDriverConf;
        String namespace =
                kubernetesDriverConf.sparkConf().get(Config.KUBERNETES_NAMESPACE().key());
        Map<String, String> confFilesMap = KubernetesClientUtils.buildSparkConfDirFilesMap(
                        kubernetesDriverConf.configMapNameDriver(),
                        kubernetesDriverConf.sparkConf(), kubernetesDriverSpec.systemProperties())
                .$plus(new Tuple2<>(Config.KUBERNETES_NAMESPACE().key(), namespace));
        SparkPod sparkPod = addConfigMap(kubernetesDriverSpec.pod(), confFilesMap);
        this.configuredPod = new PodBuilder(sparkPod.pod())
                .editSpec()
                .addToContainers(sparkPod.container())
                .endSpec()
                .build();
        this.driverPreResources = new ArrayList<>(
                JavaConverters.seqAsJavaList(kubernetesDriverSpec.driverPreKubernetesResources()));
        this.driverResources = new ArrayList<>(
                JavaConverters.seqAsJavaList(kubernetesDriverSpec.driverKubernetesResources()));
        this.driverResources.add(
                KubernetesClientUtils.buildConfigMap(kubernetesDriverConf.configMapNameDriver(),
                        confFilesMap, new HashMap<>()));
        this.driverPreResources.forEach(r -> r.getMetadata().setNamespace(namespace));
        this.driverResources.forEach(r -> r.getMetadata().setNamespace(namespace));
    }

    private SparkPod addConfigMap(SparkPod pod, Map<String, String> confFilesMap) {
        Container containerWithVolume = new ContainerBuilder(pod.container())
                .addNewEnv()
                .withName(org.apache.spark.deploy.k8s.Constants.ENV_SPARK_CONF_DIR())
                .withValue(org.apache.spark.deploy.k8s.Constants.SPARK_CONF_DIR_INTERNAL())
                .endEnv()
                .addNewVolumeMount()
                .withName(org.apache.spark.deploy.k8s.Constants.SPARK_CONF_VOLUME_DRIVER())
                .withMountPath(org.apache.spark.deploy.k8s.Constants.SPARK_CONF_DIR_INTERNAL())
                .endVolumeMount()
                .build();
        Pod podWithVolume = new PodBuilder(pod.pod())
                .editSpec()
                .addNewVolume()
                .withName(Constants.SPARK_CONF_VOLUME_DRIVER())
                .withNewConfigMap()
                .withItems(JavaConverters.seqAsJavaList(
                        KubernetesClientUtils.buildKeyToPathObjects(confFilesMap)))
                .withName(kubernetesDriverConf.configMapNameDriver())
                .endConfigMap()
                .endVolume()
                .endSpec()
                .build();
        return new SparkPod(podWithVolume, containerWithVolume);
    }
}
