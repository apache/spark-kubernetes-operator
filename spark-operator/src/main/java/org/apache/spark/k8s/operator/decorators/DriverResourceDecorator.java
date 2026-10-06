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

import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_APPLICATION_NAME;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_APP_NAME;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_APP_SELECTOR;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_OPERATOR_NAME;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_ROLE_NAME;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_VERSION_NAME;
import static org.apache.spark.k8s.operator.utils.ModelUtils.buildOwnerReferenceTo;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Decorates Driver resources (except the pod). This makes sure all resources have owner reference
 * to the driver pod, so they can be garbage collected upon termination. Secondary resources would
 * be garbage-collected if ALL owners are deleted. Therefore, operator makes only driver pod has
 * owned by the SparkApplication while all other secondary resources are owned by the driver. In
 * this way, after driver pod is deleted at the end of each attempt, all other resources would be
 * garbage collected automatically. If given secondary resource already has owner reference to
 * additional resources, it's reference to driver pod would be added to the list. Note - this is
 * uncommon as additional owner reference might impact the garbage collection at the end of each
 * attempt
 */
@RequiredArgsConstructor
@Slf4j
public class DriverResourceDecorator implements ResourceDecorator {
  private final Pod driverPod;

  /**
   * Operator- and Spark-managed identity labels whose driver-pod value must always propagate to
   * secondary resources unchanged. A resource's own label of the same key
   * must never override them. Any label key under the {@value #MANAGED_LABEL_PREFIX} prefix is
   * protected the same way (which already covers the operator-namespaced keys listed here).
   */
  private static final Set<String> MANAGED_LABEL_KEYS =
      Set.of(
          LABEL_SPARK_OPERATOR_NAME,
          LABEL_SPARK_APPLICATION_NAME,
          LABEL_SPARK_ROLE_NAME,
          LABEL_SPARK_VERSION_NAME,
          LABEL_SPARK_APP_SELECTOR,
          LABEL_SPARK_APP_NAME);

  /** Any label key starting with this prefix is treated as operator-managed and protected. */
  private static final String MANAGED_LABEL_PREFIX = "spark.operator/";

  /**
   * Decorates a Kubernetes resource by adding an owner reference to the driver pod. This ensures
   * that secondary resources are garbage collected when the driver pod is deleted.
   *
   * <p>Label precedence: driver-pod labels are copied onto the resource; a label already set on the
   * resource takes precedence on key collision, except for operator- and Spark-managed identity
   * labels (see {@link #MANAGED_LABEL_KEYS} and {@link #MANAGED_LABEL_PREFIX}), for which the
   * driver-pod value always wins.
   *
   * @param resource The resource to decorate.
   * @param <T> The type of the resource, extending HasMetadata.
   * @return The decorated resource.
   */
  @Override
  public <T extends HasMetadata> T decorate(T resource) {
    boolean ownerReferenceExists = false;
    if (resource.getMetadata().getOwnerReferences() != null
        && !resource.getMetadata().getOwnerReferences().isEmpty()) {
      for (OwnerReference o : resource.getMetadata().getOwnerReferences()) {
        if (driverPod.getKind().equals(o.getKind())
            && driverPod.getMetadata().getName().equals(o.getName())
            && driverPod.getMetadata().getUid().equals(o.getUid())) {
          ownerReferenceExists = true;
          break;
        }
      }
    }
    if (!ownerReferenceExists) {
      log.debug("Adding OwnerReference to driver for secondary resource");
      ObjectMeta metaData =
          new ObjectMetaBuilder(resource.getMetadata())
              .addToOwnerReferences(buildOwnerReferenceTo(driverPod))
              .addToLabels(driverPod.getMetadata().getLabels())
              .addToLabels(overridableLabels(resource.getMetadata().getLabels()))
              .build();
      resource.setMetadata(metaData);
    }
    return resource;
  }

  /**
   * Filters the resource's own labels down to the ones that are allowed to override a driver-pod
   * label of the same key, dropping operator- and Spark-managed identity labels (see {@link
   * #MANAGED_LABEL_KEYS} and {@link #MANAGED_LABEL_PREFIX}).
   *
   * @param resourceLabels the resource's own labels (may be {@code null}).
   * @return the subset of labels safe to re-apply on top of the driver-pod labels.
   */
  private static Map<String, String> overridableLabels(Map<String, String> resourceLabels) {
    if (resourceLabels == null || resourceLabels.isEmpty()) {
      return Map.of();
    }
    return resourceLabels.entrySet().stream()
        .filter(entry -> !isManagedLabel(entry.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Returns whether the given label key is operator- or Spark-managed and must therefore keep the
   * driver-pod value rather than be overridden by the resource.
   *
   * @param key the label key to check.
   * @return {@code true} if the key is managed and must not be overridden.
   */
  private static boolean isManagedLabel(String key) {
    return MANAGED_LABEL_KEYS.contains(key) || key.startsWith(MANAGED_LABEL_PREFIX);
  }
}
