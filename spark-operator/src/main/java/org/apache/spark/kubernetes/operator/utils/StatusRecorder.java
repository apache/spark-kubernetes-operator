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

package org.apache.spark.kubernetes.operator.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.kubernetes.operator.SparkApplication;
import org.apache.spark.kubernetes.operator.controller.SparkApplicationContext;
import org.apache.spark.kubernetes.operator.listeners.ApplicationStatusListener;
import org.apache.spark.kubernetes.operator.status.ApplicationStatus;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.apache.spark.kubernetes.operator.config.SparkOperatorConf.StatusPatchFailureBackoffSeconds;
import static org.apache.spark.kubernetes.operator.config.SparkOperatorConf.StatusPatchMaxRetry;

/**
 * <pre>
 * Note - this is inspired by
 * <a href="https://github.com/apache/flink-kubernetes-operator/blob/main/flink-kubernetes-operator/src/main/java/org/apache/flink/kubernetes/operator/utils/StatusRecorder.java">Flink Operator Status Recorder</a>
 * </pre>
 *  Enables additional (extendable) observers for Spark App status.
 *  Cache & version locking might be removed in future version as batch app does not expect
 *  spec change after submitted.
 */
@Slf4j
public class StatusRecorder {
    protected final List<ApplicationStatusListener> appStatusListeners;
    protected final ObjectMapper objectMapper = new ObjectMapper();
    protected final ConcurrentHashMap<ResourceID, ObjectNode> statusCache;

    public StatusRecorder(List<ApplicationStatusListener> appStatusListeners) {
        this.appStatusListeners = appStatusListeners;
        this.statusCache = new ConcurrentHashMap<>();
    }

    /**
     * Update the status of the provided kubernetes resource on the k8s cluster. We use patch
     * together with null resourceVersion to try to guarantee that the status update succeeds even
     * if the underlying resource spec was update in the meantime. This is necessary for the correct
     * operator behavior.
     *
     * @param resource Resource for which status update should be performed
     */
    @SneakyThrows
    private void patchAndCacheStatus(SparkApplication resource, KubernetesClient client) {
        ObjectNode newStatusNode =
                objectMapper.convertValue(resource.getStatus(), ObjectNode.class);
        ResourceID resourceId = ResourceID.fromResource(resource);
        ObjectNode previousStatusNode = statusCache.get(resourceId);

        if (newStatusNode.equals(previousStatusNode)) {
            log.debug("No status change.");
            return;
        }

        ApplicationStatus prevStatus =
                objectMapper.convertValue(previousStatusNode, ApplicationStatus.class);

        Exception err = null;
        for (long i = 0; i < StatusPatchMaxRetry.getValue(); i++) {
            // We retry the status update 3 times to avoid some intermittent connectivity errors
            try {
                replaceStatus(resource, prevStatus, client);
                err = null;
            } catch (KubernetesClientException e) {
                log.error("Error while patching status, retrying {}/3...", (i + 1), e);
                Thread.sleep(
                        TimeUnit.SECONDS.toMillis(StatusPatchFailureBackoffSeconds.getValue()));
                err = e;
            }
        }

        if (err != null) {
            throw err;
        }

        statusCache.put(resourceId, newStatusNode);
        appStatusListeners.forEach(listener -> {
            listener.listenStatus(resource, prevStatus, resource.getStatus());
        });
    }

    public void persistStatus(SparkApplicationContext context,
                              ApplicationStatus newStatus) {
        context.getSparkApplication().setStatus(newStatus);
        patchAndCacheStatus(context.getSparkApplication(), context.getClient());
    }

    private void replaceStatus(SparkApplication resource, ApplicationStatus prevStatus,
                               KubernetesClient client)
            throws JsonProcessingException {
        int retries = 0;
        while (true) {
            try {
                var updated = client.resource(resource).lockResourceVersion().updateStatus();

                // If we successfully replaced the status, update the resource version so we know
                // what to lock next in the same reconciliation loop
                resource.getMetadata()
                        .setResourceVersion(updated.getMetadata().getResourceVersion());
                return;
            } catch (KubernetesClientException kce) {
                // 409 is the error code for conflicts resulting from the locking
                if (kce.getCode() == 409) {
                    var currentVersion = resource.getMetadata().getResourceVersion();
                    log.debug(
                            "Could not apply status update for resource version {}",
                            currentVersion);

                    var latest = client.resource(resource).get();
                    var latestVersion = latest.getMetadata().getResourceVersion();

                    if (latestVersion.equals(currentVersion)) {
                        // This should not happen as long as the client works consistently
                        log.error("Unable to fetch latest resource version");
                        throw kce;
                    }

                    if (latest.getStatus().equals(prevStatus)) {
                        if (retries++ < 3) {
                            log.debug(
                                    "Retrying status update for latest version {}", latestVersion);
                            resource.getMetadata().setResourceVersion(latestVersion);
                        } else {
                            // If we cannot get the latest version in 3 tries we throw the error to
                            // retry with delay
                            throw kce;
                        }
                    } else {
                        throw new RuntimeException(
                                "Status have been modified externally in version "
                                        + latestVersion
                                        + " Previous: "
                                        + objectMapper.writeValueAsString(prevStatus)
                                        + " Latest: "
                                        + objectMapper.writeValueAsString(latest.getStatus()), kce);
                    }
                } else {
                    // We simply throw non conflict errors, to trigger retry with delay
                    throw kce;
                }
            }
        }
    }

    /**
     * Update the custom resource status based on the in-memory cached to ensure that any status
     * updates that we made previously are always visible in the reconciliation loop. This is
     * required due to our custom status patching logic.
     *
     * <p>If the cache doesn't have a status stored, we do no update. This happens when the operator
     * reconciles a resource for the first time after a restart.
     *
     * @param resource Resource for which the status should be updated from the cache
     */
    public void updateStatusFromCache(SparkApplication resource) {
        var key = ResourceID.fromResource(resource);
        var cachedStatus = statusCache.get(key);
        if (cachedStatus != null) {
            resource.setStatus(
                    objectMapper.convertValue(
                            cachedStatus, resource.getStatus().getClass()));
        } else {
            // Initialize cache with current status copy
            statusCache.put(key, objectMapper.convertValue(resource.getStatus(), ObjectNode.class));
        }
    }

    /**
     * Remove cached status
     */
    public void removeCachedStatus(SparkApplication resource) {
        statusCache.remove(ResourceID.fromResource(resource));
    }
}
