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

package org.apache.spark.k8s.operator.metrics.healthcheck;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.ToStringBuilder;

import org.apache.spark.k8s.operator.BaseResource;
import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.config.SparkOperatorConf;
import org.apache.spark.k8s.operator.utils.ReconcilerUtils;
import org.apache.spark.k8s.operator.utils.Utils;

/**
 * Sentinel manager monitors dedicated sentinel resources to make sure the operator is healthy.
 *
 * @param <CR> custom resource type
 */
@RequiredArgsConstructor
@Slf4j
public class SentinelManager<CR extends BaseResource<?, ?, ?, ?, ?>> {

  private final ConcurrentHashMap<ResourceID, SentinelResourceState> sentinelResources =
      new ConcurrentHashMap<>();

  private final ScheduledExecutorService executorService =
      Executors.newScheduledThreadPool(
          SparkOperatorConf.SENTINEL_EXECUTOR_SERVICE_POOL_SIZE.getValue());

  public static boolean isSentinelResource(HasMetadata resource) {
    Map<String, String> labels = resource.getMetadata().getLabels();
    if (labels == null) {
      return false;
    }
    String namespace = resource.getMetadata().getNamespace();
    return shouldSentinelWatchGivenNamespace(namespace)
        && Boolean.TRUE
            .toString()
            .equalsIgnoreCase(
                labels.getOrDefault(Constants.LABEL_SENTINEL_RESOURCE, Boolean.FALSE.toString()));
  }

  private static boolean shouldSentinelWatchGivenNamespace(String namespace) {
    if (!Utils.getWatchedNamespaces().isEmpty()
        && !Utils.getWatchedNamespaces().contains(namespace)) {
      if (log.isErrorEnabled()) {
        log.error("Skip watching sentinel resource in namespace {}", namespace);
      }
      return false;
    }
    return true;
  }

  public boolean allSentinelsAreHealthy() {
    Set<ResourceID> unWatchedKey = new HashSet<>();
    boolean result =
        sentinelResources.entrySet().stream()
            .filter(
                x -> {
                  if (x.getKey().getNamespace().isPresent()
                      && shouldSentinelWatchGivenNamespace(x.getKey().getNamespace().get())) {
                    return true;
                  }
                  unWatchedKey.add(x.getKey());
                  return false;
                })
            .map(Map.Entry::getValue)
            .allMatch(SentinelResourceState::isHealthy);
    sentinelResources.keySet().removeAll(unWatchedKey);
    return result;
  }

  public void checkHealth(ResourceID resourceID, KubernetesClient client) {
    SentinelResourceState sentinelResourceState = sentinelResources.get(resourceID);
    if (sentinelResourceState == null) {
      if (log.isErrorEnabled()) {
        log.error("Sentinel resources {} not found. Stopping sentinel health checks", resourceID);
      }
      return;
    }

    if (sentinelResourceState.reconciledSinceUpdate()) {
      log.info("Sentinel reports healthy state globally");
      sentinelResourceState.isHealthy = true;
    } else {
      if (log.isErrorEnabled()) {
        log.error(
            "Sentinel deployment {} latest spec not was reconciled. Expected "
                + "generation larger than {}, received {}",
            resourceID,
            sentinelResourceState.previousGeneration,
            sentinelResourceState.resource.getMetadata().getGeneration());
      }
      sentinelResourceState.isHealthy = false;
    }

    updateSpecAndScheduleHealthCheck(resourceID, sentinelResourceState, client);
  }

  public boolean handleSentinelResourceReconciliation(CR resource, KubernetesClient client) {
    if (!isSentinelResource(resource)) {
      return false;
    }

    ResourceID resourceId = ResourceID.fromResource(resource);
    sentinelResources.compute(
        resourceId,
        (id, previousState) -> {
          boolean firstReconcile = false;
          if (previousState == null) {
            firstReconcile = true;
            previousState = new SentinelResourceState();
          }
          previousState.onReconcile(resource);
          if (firstReconcile) {
            updateSpecAndScheduleHealthCheck(resourceId, previousState, client);
          }
          return previousState;
        });
    return true;
  }

  private void updateSpecAndScheduleHealthCheck(
      ResourceID resourceID, SentinelResourceState sentinelResourceState, KubernetesClient client) {
    Map<String, String> sparkConf = sentinelResourceState.resource.getSpec().getSparkConf();
    sparkConf.compute(
        Constants.SENTINEL_RESOURCE_DUMMY_FIELD,
        (key, value) -> {
          if (value == null) {
            return "1";
          } else {
            return String.valueOf(Long.parseLong(value) + 1);
          }
        });
    sentinelResourceState.previousGeneration =
        sentinelResourceState.resource.getMetadata().getGeneration();
    try {
      if (log.isDebugEnabled()) {
        log.debug("Update the sentinel kubernetes resource spec {}", sentinelResourceState);
      }
      client.resource(ReconcilerUtils.clone(sentinelResourceState.resource)).replace();
    } catch (Throwable t) {
      if (log.isWarnEnabled()) {
        log.warn(
            "Could not replace the sentinel deployment spark conf {}",
            Constants.SENTINEL_RESOURCE_DUMMY_FIELD,
            t);
      }
    }
    int delay = SparkOperatorConf.SENTINEL_RESOURCE_RECONCILIATION_DELAY.getValue();
    if (log.isInfoEnabled()) {
      log.info("Scheduling sentinel check for {} in {} seconds", resourceID, delay);
    }
    executorService.schedule(() -> checkHealth(resourceID, client), delay, TimeUnit.SECONDS);
  }

  public class SentinelResourceState {
    CR resource;
    long previousGeneration;

    @Getter boolean isHealthy = true;

    void onReconcile(CR cr) {
      resource = cr;
    }

    boolean reconciledSinceUpdate() {
      return resource.getMetadata().getGeneration() > previousGeneration;
    }

    @Override
    public String toString() {
      return new ToStringBuilder(this)
          .append("resource", resource)
          .append("previousGeneration", previousGeneration)
          .append("isHealthy", isHealthy)
          .toString();
    }
  }

  @VisibleForTesting
  public ConcurrentHashMap<ResourceID, SentinelResourceState> getSentinelResources() {
    return sentinelResources;
  }
}
