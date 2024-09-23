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

package org.apache.spark.k8s.operator.context;

import java.util.Optional;

import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.autoscaling.v2.HorizontalPodAutoscaler;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import lombok.RequiredArgsConstructor;

import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.SparkClusterResourceSpec;
import org.apache.spark.k8s.operator.SparkClusterSubmissionWorker;
import org.apache.spark.k8s.operator.reconciler.SparkClusterResourceSpecFactory;

/**
 * Context for {@link SparkCluster} resource, including secondary resource(s) and desired secondary
 * resource spec
 */
@RequiredArgsConstructor
public class SparkClusterContext extends BaseContext<SparkCluster> {
  private final SparkCluster sparkCluster;
  private final Context<?> josdkContext;
  private final SparkClusterSubmissionWorker submissionWorker;

  /** secondaryResourceSpec is initialized in a lazy fashion - built upon the first attempt */
  private SparkClusterResourceSpec secondaryResourceSpec;

  private SparkClusterResourceSpec getSecondaryResourceSpec() {
    synchronized (this) {
      if (secondaryResourceSpec == null) {
        secondaryResourceSpec =
            SparkClusterResourceSpecFactory.buildResourceSpec(sparkCluster, submissionWorker);
      }
      return secondaryResourceSpec;
    }
  }

  @Override
  public SparkCluster getResource() {
    return sparkCluster;
  }

  public Service getMasterServiceSpec() {
    return getSecondaryResourceSpec().getMasterService();
  }

  public Service getWorkerServiceSpec() {
    return getSecondaryResourceSpec().getWorkerService();
  }

  public StatefulSet getMasterStatefulSetSpec() {
    return getSecondaryResourceSpec().getMasterStatefulSet();
  }

  public StatefulSet getWorkerStatefulSetSpec() {
    return getSecondaryResourceSpec().getWorkerStatefulSet();
  }

  public Optional<HorizontalPodAutoscaler> getHorizontalPodAutoscalerSpec() {
    return getSecondaryResourceSpec().getHorizontalPodAutoscaler();
  }

  @Override
  public KubernetesClient getClient() {
    return josdkContext.getClient();
  }
}
