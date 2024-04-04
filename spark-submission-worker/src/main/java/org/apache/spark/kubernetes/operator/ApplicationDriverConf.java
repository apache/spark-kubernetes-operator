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

import scala.Option;

import org.apache.spark.SparkConf;
import org.apache.spark.deploy.k8s.Config;
import org.apache.spark.deploy.k8s.KubernetesDriverConf;
import org.apache.spark.deploy.k8s.KubernetesVolumeUtils;
import org.apache.spark.deploy.k8s.submit.KubernetesClientUtils;
import org.apache.spark.deploy.k8s.submit.MainAppResource;

public class ApplicationDriverConf extends KubernetesDriverConf {
  private ApplicationDriverConf(SparkConf sparkConf,
                                String appId,
                                MainAppResource mainAppResource,
                                String mainClass,
                                String[] appArgs,
                                Option<String> proxyUser) {
    super(sparkConf, appId, mainAppResource, mainClass, appArgs, proxyUser);
  }

  public static ApplicationDriverConf create(SparkConf sparkConf,
                                             String appId,
                                             MainAppResource mainAppResource,
                                             String mainClass,
                                             String[] appArgs,
                                             Option<String> proxyUser) {
    // pre-create check only
    KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf,
        Config.KUBERNETES_EXECUTOR_VOLUMES_PREFIX());
    return new ApplicationDriverConf(sparkConf, appId, mainAppResource, mainClass, appArgs,
        proxyUser);
  }

  /**
   * Application managed by operator has a deterministic prefix
   */
  @Override
  public String resourceNamePrefix() {
    return sparkConf().getOption(Config.KUBERNETES_DRIVER_POD_NAME_PREFIX().key()).isEmpty()
        ? appId() : sparkConf().get(Config.KUBERNETES_DRIVER_POD_NAME_PREFIX().key());
  }

  public String configMapNameDriver() {
    return KubernetesClientUtils.configMapName(
        String.format("spark-drv-%s", resourceNamePrefix()));
  }
}
