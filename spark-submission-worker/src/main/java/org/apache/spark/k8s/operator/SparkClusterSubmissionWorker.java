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

import java.util.Map;

import org.apache.commons.collections4.MapUtils;

import org.apache.spark.SparkConf;

public class SparkClusterSubmissionWorker {
  public SparkClusterResourceSpec getResourceSpec(
      SparkCluster cluster, Map<String, String> confOverrides) {
    SparkConf effectiveSparkConf = new SparkConf();

    Map<String, String> confFromSpec = cluster.getSpec().getSparkConf();
    if (MapUtils.isNotEmpty(confFromSpec)) {
      for (Map.Entry<String, String> entry : confFromSpec.entrySet()) {
        effectiveSparkConf.set(entry.getKey(), entry.getValue());
      }
    }

    if (MapUtils.isNotEmpty(confOverrides)) {
      for (Map.Entry<String, String> entry : confOverrides.entrySet()) {
        effectiveSparkConf.set(entry.getKey(), entry.getValue());
      }
    }

    effectiveSparkConf.set("spark.kubernetes.namespace", cluster.getMetadata().getNamespace());

    return new SparkClusterResourceSpec(cluster, effectiveSparkConf);
  }
}
