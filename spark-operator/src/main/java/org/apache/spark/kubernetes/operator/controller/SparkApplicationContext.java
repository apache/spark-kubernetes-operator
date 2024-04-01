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

package org.apache.spark.kubernetes.operator.controller;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.kubernetes.operator.ApplicationResourceSpec;
import org.apache.spark.kubernetes.operator.SparkApplication;
import org.apache.spark.kubernetes.operator.reconciler.SparkApplicationReconcileUtils;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.spark.kubernetes.operator.reconciler.SparkReconcilerUtils.driverLabels;
import static org.apache.spark.kubernetes.operator.reconciler.SparkReconcilerUtils.executorLabels;

/**
 * Context for {@link org.apache.spark.kubernetes.operator.SparkApplication} resource
 * Includes secondary resource(s) and desired secondary resource spec
 */
@RequiredArgsConstructor
@Slf4j
public class SparkApplicationContext {
    @Getter
    private final SparkApplication sparkApplication;
    private final Context<?> josdkContext;
    private ApplicationResourceSpec secondaryResourceSpec;

    public Optional<Pod> getDriverPod() {
        return josdkContext.getSecondaryResourcesAsStream(Pod.class)
                .filter(p -> p.getMetadata().getLabels().entrySet()
                        .containsAll(driverLabels(sparkApplication).entrySet()))
                .findAny();
    }

    public Set<Pod> getExecutorsForApplication() {
        return josdkContext.getSecondaryResourcesAsStream(Pod.class)
                .filter(p -> p.getMetadata().getLabels().entrySet()
                        .containsAll(executorLabels(sparkApplication).entrySet()))
                .collect(Collectors.toSet());
    }

    private ApplicationResourceSpec getSecondaryResourceSpec() {
        synchronized (this) {
            if (secondaryResourceSpec == null) {
                secondaryResourceSpec = SparkApplicationReconcileUtils.buildResourceSpec(
                        sparkApplication, josdkContext.getClient());
            }
            return secondaryResourceSpec;
        }
    }

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
