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

import static org.apache.spark.k8s.operator.utils.Utils.driverLabels;
import static org.apache.spark.k8s.operator.utils.Utils.executorLabels;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.SparkAppResourceSpec;
import org.apache.spark.k8s.operator.SparkAppSubmissionWorker;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.reconciler.SparkAppResourceSpecFactory;

/**
 * Context for {@link org.apache.spark.k8s.operator.SparkApplication} resource, including secondary
 * resource(s) and desired secondary resource spec
 */
@RequiredArgsConstructor
@Slf4j
public class SparkAppContext extends BaseContext<SparkApplication> {
  private final SparkApplication sparkApplication;
  private final Context<?> josdkContext;
  private final SparkAppSubmissionWorker submissionWorker;

  /** secondaryResourceSpec is initialized in a lazy fashion - built upon the first attempt */
  private SparkAppResourceSpec secondaryResourceSpec;

  public Optional<Pod> getDriverPod() {
    return josdkContext
        .getSecondaryResourcesAsStream(Pod.class)
        .filter(
            p ->
                p.getMetadata()
                    .getLabels()
                    .entrySet()
                    .containsAll(driverLabels(sparkApplication).entrySet()))
        .findAny();
  }

  public Set<Pod> getExecutorsForApplication() {
    return josdkContext
        .getSecondaryResourcesAsStream(Pod.class)
        .filter(
            p ->
                p.getMetadata()
                    .getLabels()
                    .entrySet()
                    .containsAll(executorLabels(sparkApplication).entrySet()))
        .collect(Collectors.toSet());
  }

  private SparkAppResourceSpec getSecondaryResourceSpec() {
    synchronized (this) {
      if (secondaryResourceSpec == null) {
        secondaryResourceSpec =
            SparkAppResourceSpecFactory.buildResourceSpec(
                sparkApplication, josdkContext.getClient(), submissionWorker);
      }
      return secondaryResourceSpec;
    }
  }

  @Override
  public SparkApplication getResource() {
    return sparkApplication;
  }

  @Override
  public KubernetesClient getClient() {
    return josdkContext.getClient();
  }

  public List<HasMetadata> getDriverPreResourcesSpec() {
    return getSecondaryResourceSpec().getDriverPreResources();
  }

  public Pod getDriverPodSpec() {
    return getSecondaryResourceSpec().getConfiguredPod();
  }

  public List<HasMetadata> getDriverResourcesSpec() {
    return getSecondaryResourceSpec().getDriverResources();
  }
}
