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

package org.apache.spark.k8s.operator.utils;

import static org.apache.spark.k8s.operator.config.SparkOperatorConf.API_RETRY_ATTEMPT_AFTER_SECONDS;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.API_STATUS_PATCH_MAX_ATTEMPTS;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.BaseResource;
import org.apache.spark.k8s.operator.context.BaseContext;
import org.apache.spark.k8s.operator.listeners.BaseStatusListener;
import org.apache.spark.k8s.operator.status.BaseStatus;

/**
 * Enables additional (extendable) observers for Spark App status. Cache and version locking might
 * be removed in future version as batch app does not expect spec change after submitted.
 */
@Slf4j
public class StatusRecorder<
    STATUS extends BaseStatus<?, ?, ?>,
    CR extends BaseResource<?, ?, ?, ?, STATUS>,
    LISTENER extends BaseStatusListener<STATUS, CR>> {
  protected final List<LISTENER> statusListeners;
  protected final ObjectMapper objectMapper = ModelUtils.objectMapper;
  protected final Class<STATUS> statusClass;
  protected final Class<CR> resourceClass;
  protected final ConcurrentHashMap<ResourceID, ObjectNode> statusCache;

  protected StatusRecorder(
      List<LISTENER> statusListeners, Class<STATUS> statusClass, Class<CR> resourceClass) {
    this.statusListeners = statusListeners;
    this.statusClass = statusClass;
    this.resourceClass = resourceClass;
    this.statusCache = new ConcurrentHashMap<>();
  }

  /**
   * Update the status of the provided kubernetes resource on the k8s cluster. We use patch together
   * with null resourceVersion to try to guarantee that the status update succeeds even if the
   * underlying resource spec was update in the meantime. This is necessary for the correct operator
   * behavior.
   *
   * @param resource Resource for which status update should be performed
   */
  @SneakyThrows
  private void patchAndStatusWithVersionLocked(CR resource, KubernetesClient client) {
    ObjectNode newStatusNode = objectMapper.convertValue(resource.getStatus(), ObjectNode.class);
    ResourceID resourceId = ResourceID.fromResource(resource);
    ObjectNode previousStatusNode = statusCache.get(resourceId);

    if (newStatusNode.equals(previousStatusNode)) {
      log.debug("No status change.");
      return;
    }

    Exception err = null;
    long maxRetry = API_STATUS_PATCH_MAX_ATTEMPTS.getValue();
    for (long i = 0; i < maxRetry; i++) {
      // We retry the status update 3 times to avoid some intermittent connectivity errors
      try {
        CR updated = client.resource(resource).lockResourceVersion().updateStatus();
        resource.getMetadata().setResourceVersion(updated.getMetadata().getResourceVersion());
        err = null;
      } catch (KubernetesClientException e) {
        log.warn("Error while patching status, retrying {}/{}...", i + 1, maxRetry, e);
        Thread.sleep(TimeUnit.SECONDS.toMillis(API_RETRY_ATTEMPT_AFTER_SECONDS.getValue()));
        err = e;
      }
    }

    if (err != null) {
      log.error("Fail to patch status.", err);
      throw err;
    }

    statusCache.put(resourceId, newStatusNode);
    STATUS prevStatus = objectMapper.convertValue(previousStatusNode, statusClass);
    statusListeners.forEach(
        listener -> {
          listener.listenStatus(resource, prevStatus, resource.getStatus());
        });
  }

  public void persistStatus(BaseContext<CR> context, STATUS newStatus) {
    context.getResource().setStatus(newStatus);
    patchAndStatusWithVersionLocked(context.getResource(), context.getClient());
  }

  /**
   * Update the custom resource status based on the in-memory cached to ensure that any status
   * updates that we made previously are always visible in the reconciliation loop. This is required
   * due to our custom status patching logic.
   *
   * <p>If the cache doesn't have a status stored, we do no update. This happens when the operator
   * reconciles a resource for the first time after a restart.
   *
   * @param resource Resource for which the status should be updated from the cache
   */
  public void updateStatusFromCache(CR resource) {
    ResourceID key = ResourceID.fromResource(resource);
    Object cachedStatus = statusCache.get(key);
    if (cachedStatus != null) {
      resource.setStatus(objectMapper.convertValue(cachedStatus, statusClass));
    } else {
      // Initialize cache with current status copy
      statusCache.put(key, objectMapper.convertValue(resource.getStatus(), ObjectNode.class));
    }
  }

  /** Remove cached status */
  public void removeCachedStatus(CR resource) {
    statusCache.remove(ResourceID.fromResource(resource));
  }
}
