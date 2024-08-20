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

package org.apache.spark.k8s.operator.decorators;

import static org.apache.spark.k8s.operator.utils.ModelUtils.buildOwnerReferenceTo;
import static org.apache.spark.k8s.operator.utils.Utils.sparkClusterResourceLabels;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import lombok.RequiredArgsConstructor;

import org.apache.spark.k8s.operator.SparkCluster;

/** Decorates Spark cluster resources like statefulsets. */
@RequiredArgsConstructor
public class ClusterDecorator implements ResourceDecorator {

  private final SparkCluster cluster;

  /** Add labels and owner references to the cluster for all secondary resources */
  @Override
  public <T extends HasMetadata> T decorate(T resource) {
    ObjectMeta metaData =
        new ObjectMetaBuilder(resource.getMetadata())
            .addToOwnerReferences(buildOwnerReferenceTo(cluster))
            .addToLabels(sparkClusterResourceLabels(cluster))
            .withNamespace(cluster.getMetadata().getNamespace())
            .build();
    resource.setMetadata(metaData);
    return resource;
  }
}
