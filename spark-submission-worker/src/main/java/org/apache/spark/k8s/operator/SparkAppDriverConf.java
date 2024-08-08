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

import scala.Option;

import org.apache.spark.SparkConf;
import org.apache.spark.deploy.k8s.Config;
import org.apache.spark.deploy.k8s.KubernetesDriverConf;
import org.apache.spark.deploy.k8s.KubernetesVolumeUtils;
import org.apache.spark.deploy.k8s.submit.KubernetesClientUtils;
import org.apache.spark.deploy.k8s.submit.MainAppResource;

public final class SparkAppDriverConf extends KubernetesDriverConf {
  private SparkAppDriverConf(
      SparkConf sparkConf,
      String appId,
      MainAppResource mainAppResource,
      String mainClass,
      String[] appArgs,
      Option<String> proxyUser) {
    super(sparkConf, appId, mainAppResource, mainClass, appArgs, proxyUser, null);
  }

  public static SparkAppDriverConf create(
      SparkConf sparkConf,
      String appId,
      MainAppResource mainAppResource,
      String mainClass,
      String[] appArgs,
      Option<String> proxyUser) {
    // pre-create check only
    KubernetesVolumeUtils.parseVolumesWithPrefix(
        sparkConf, Config.KUBERNETES_EXECUTOR_VOLUMES_PREFIX());
    return new SparkAppDriverConf(sparkConf, appId, mainAppResource, mainClass, appArgs, proxyUser);
  }

  /** Application managed by operator has a deterministic prefix */
  @Override
  public String resourceNamePrefix() {
    return appId();
  }

  /**
   * Create the name to be used by driver config map. The consists of `resourceNamePrefix` and Spark
   * instance type (driver). Operator proposes `resourceNamePrefix` with leaves naming length margin
   * for sub-resources to be qualified as DNS subdomain or label. In addition, the overall config
   * name length is governed by `KubernetesClientUtils.configMapName` - which ensures the name
   * length meets requirements as DNS subdomain name.
   *
   * @return proposed name to be used by driver config map
   */
  public String configMapNameDriver() {
    return KubernetesClientUtils.configMapName(String.format("%s-spark-drv", resourceNamePrefix()));
  }
}
