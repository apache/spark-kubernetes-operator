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

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.API_SECONDARY_RESOURCE_CREATE_MAX_ATTEMPTS;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.RECONCILER_FOREGROUND_REQUEST_TIMEOUT_SECONDS;
import static org.apache.spark.k8s.operator.utils.ModelUtils.buildOwnerReferenceTo;
import static org.apache.spark.k8s.operator.utils.SparkExceptionUtils.isConflictForExistingResource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.DeletionPropagation;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.javaoperatorsdk.operator.api.reconciler.DeleteControl;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.BaseResource;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;

@Slf4j
public final class ReconcilerUtils {

  private ReconcilerUtils() {}

  public static <S, T, O extends BaseResource<?, ?, ?, ?, ?>> UpdateControl<O> toUpdateControl(
      O resource, ReconcileProgress reconcileProgress) {
    // reconciler already handled resource and status update, skip update at lower level
    UpdateControl<O> updateControl = UpdateControl.noUpdate();
    if (reconcileProgress.isRequeue()) {
      return updateControl.rescheduleAfter(reconcileProgress.getRequeueAfterDuration());
    } else {
      return updateControl;
    }
  }

  public static <S, T, O extends BaseResource<?, ?, ?, ?, ?>> DeleteControl toDeleteControl(
      O resource, ReconcileProgress reconcileProgress) {
    if (reconcileProgress.isRequeue()) {
      return DeleteControl.noFinalizerRemoval()
          .rescheduleAfter(reconcileProgress.getRequeueAfterDuration());
    } else {
      return DeleteControl.defaultDelete();
    }
  }

  public static <T extends HasMetadata> Optional<T> getOrCreateSecondaryResource(
      final KubernetesClient client, final T resource) {
    Optional<T> current = getResource(client, resource);
    if (current.isEmpty()) {
      // Adding retry logic to overcome known k8s issue:
      // https://github.com/kubernetes/kubernetes/issues/67761
      long maxAttempts = API_SECONDARY_RESOURCE_CREATE_MAX_ATTEMPTS.getValue();
      long attemptCount = 1;
      while (true) {
        try {
          current = Optional.ofNullable(client.resource(resource).create());
          break;
        } catch (KubernetesClientException e) {
          if (log.isErrorEnabled()) {
            log.error(
                "Failed to request resource with responseCode={} attemptCount={}/{}",
                e.getCode(),
                attemptCount,
                maxAttempts);
          }
          if (e.getCode() == HTTP_CONFLICT) {
            if (isConflictForExistingResource(e)) {
              current = getResource(client, resource);
              if (current.isPresent()) {
                return current;
              }
            }
            if (++attemptCount > maxAttempts) {
              log.error("Max Retries exceeded while trying to create resource");
              throw e;
            }
          } else {
            throw e;
          }
        }
      }
    }
    return current;
  }

  public static void addOwnerReferenceSecondaryResource(
      final KubernetesClient client, final List<HasMetadata> resources, final HasMetadata owner) {

    resources.forEach(
        r -> {
          ObjectMeta metaData =
              new ObjectMetaBuilder(r.getMetadata())
                  .addToOwnerReferences(buildOwnerReferenceTo(owner))
                  .build();
          r.setMetadata(metaData);
        });
    client.resourceList(resources).forceConflicts().serverSideApply();
  }

  public static <T extends HasMetadata> Optional<T> getResource(
      final KubernetesClient client, final T desired) {
    T resource = null;
    try {
      resource = client.resource(desired).get();
    } catch (KubernetesClientException e) {
      if (e.getCode() == HTTP_NOT_FOUND) {
        return Optional.empty();
      }
    }
    return Optional.ofNullable(resource);
  }

  public static <T extends HasMetadata> void deleteResourceIfExists(
      final KubernetesClient client, final T resource, boolean forceDelete) {
    try {
      if (forceDelete) {
        client.resource(resource).withGracePeriod(0L).delete();
      } else {
        client
            .resource(resource)
            .withPropagationPolicy(DeletionPropagation.FOREGROUND)
            .withTimeout(RECONCILER_FOREGROUND_REQUEST_TIMEOUT_SECONDS.getValue(), TimeUnit.SECONDS)
            .delete();
      }
    } catch (KubernetesClientException e) {
      if (e.getCode() == HTTP_NOT_FOUND) {
        log.info("Pod to delete does not exist, proceeding...");
      } else {
        throw e;
      }
    }
  }

  public static <T> T clone(T object) {
    if (object == null) {
      return null;
    }
    try {
      ObjectMapper mapper = ModelUtils.objectMapper;
      return (T) mapper.readValue(mapper.writeValueAsString(object), object.getClass());
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }
}
