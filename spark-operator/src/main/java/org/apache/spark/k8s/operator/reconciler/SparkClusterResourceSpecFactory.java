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

package org.apache.spark.k8s.operator.reconciler;

import java.util.HashMap;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.SparkClusterResourceSpec;
import org.apache.spark.k8s.operator.SparkClusterSubmissionWorker;
import org.apache.spark.k8s.operator.decorators.ClusterDecorator;

/** Factory for creating SparkClusterResourceSpec objects. */
@Slf4j
public final class SparkClusterResourceSpecFactory {

  private SparkClusterResourceSpecFactory() {}

  /**
   * Builds a SparkClusterResourceSpec for the given SparkCluster.
   *
   * @param cluster The SparkCluster.
   * @param worker The SparkClusterSubmissionWorker.
   * @return A SparkClusterResourceSpec containing the configured resources.
   */
  public static SparkClusterResourceSpec buildResourceSpec(
      final SparkCluster cluster, final SparkClusterSubmissionWorker worker) {
    Map<String, String> confOverrides = new HashMap<>();
    SparkClusterResourceSpec spec = worker.getResourceSpec(cluster, confOverrides);
    ClusterDecorator decorator = new ClusterDecorator(cluster);
    decorator.decorate(spec.getMasterService());
    decorator.decorate(spec.getWorkerService());
    decorator.decorate(spec.getMasterStatefulSet());
    decorator.decorate(spec.getWorkerStatefulSet());
    if (spec.getHorizontalPodAutoscaler().isPresent()) {
      decorator.decorate(spec.getHorizontalPodAutoscaler().get());
    }
    if (spec.getPodDisruptionBudget().isPresent()) {
      decorator.decorate(spec.getPodDisruptionBudget().get());
    }
    return spec;
  }
}
