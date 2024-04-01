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

package org.apache.spark.kubernetes.operator.reconciler;

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
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.kubernetes.operator.BaseResource;
import org.apache.spark.kubernetes.operator.Constants;
import org.apache.spark.kubernetes.operator.SparkApplication;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.spark.kubernetes.operator.Constants.LABEL_RESOURCE_NAME;
import static org.apache.spark.kubernetes.operator.Constants.LABEL_SPARK_OPERATOR_NAME;
import static org.apache.spark.kubernetes.operator.Constants.LABEL_SPARK_ROLE_DRIVER_VALUE;
import static org.apache.spark.kubernetes.operator.Constants.LABEL_SPARK_ROLE_EXECUTOR_VALUE;
import static org.apache.spark.kubernetes.operator.config.SparkOperatorConf.DriverCreateMaxAttempts;
import static org.apache.spark.kubernetes.operator.config.SparkOperatorConf.ForegroundRequestTimeoutSeconds;
import static org.apache.spark.kubernetes.operator.config.SparkOperatorConf.OperatorAppName;
import static org.apache.spark.kubernetes.operator.config.SparkOperatorConf.OperatorWatchedNamespaces;
import static org.apache.spark.kubernetes.operator.utils.ModelUtils.buildOwnerReferenceTo;
import static org.apache.spark.kubernetes.operator.utils.SparkExceptionUtils.isConflictForExistingResource;

@Slf4j
public class SparkReconcilerUtils {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static Map<String, String> commonOperatorResourceLabels() {
        Map<String, String> labels = new HashMap<>();
        labels.put(LABEL_RESOURCE_NAME, OperatorAppName.getValue());
        return labels;
    }

    public static Map<String, String> defaultOperatorConfigLabels() {
        Map<String, String> labels = new HashMap<>(commonOperatorResourceLabels());
        labels.put("app.kubernetes.io/component", "operator-dynamic-config-overrides");
        return labels;
    }

    public static Map<String, String> commonManagedResourceLabels() {
        Map<String, String> labels = new HashMap<>();
        labels.put(LABEL_SPARK_OPERATOR_NAME, OperatorAppName.getValue());
        return labels;
    }

    public static Map<String, String> sparkAppResourceLabels(final SparkApplication app) {
        return sparkAppResourceLabels(app.getMetadata().getName());
    }

    public static Map<String, String> sparkAppResourceLabels(final String appName) {
        Map<String, String> labels = commonManagedResourceLabels();
        labels.put(Constants.LABEL_SPARK_APPLICATION_NAME, appName);
        return labels;
    }

    public static Map<String, String> driverLabels(final SparkApplication sparkApplication) {
        Map<String, String> labels = sparkAppResourceLabels(sparkApplication);
        labels.put(Constants.LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_DRIVER_VALUE);
        return labels;
    }

    public static Map<String, String> executorLabels(final SparkApplication sparkApplication) {
        Map<String, String> labels = sparkAppResourceLabels(sparkApplication);
        labels.put(Constants.LABEL_SPARK_ROLE_NAME, LABEL_SPARK_ROLE_EXECUTOR_VALUE);
        return labels;
    }

    public static Set<String> getWatchedNamespaces() {
        String namespaces = OperatorWatchedNamespaces.getValue();
        if (StringUtils.isNotEmpty(namespaces)) {
            return Arrays.stream(namespaces.split(",")).map(String::trim)
                    .collect(Collectors.toSet());
        }
        return Collections.emptySet();
    }

    /**
     * Labels to be applied to all created resources, as a comma-separated string
     *
     * @return labels string
     */
    public static String commonResourceLabelsStr() {
        return labelsAsStr(commonManagedResourceLabels());
    }

    public static String labelsAsStr(Map<String, String> labels) {
        return labels
                .entrySet()
                .stream()
                .map(e -> String.join("=", e.getKey(), e.getValue()))
                .collect(Collectors.joining(","));
    }

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
            return DeleteControl.noFinalizerRemoval().rescheduleAfter(
                    reconcileProgress.getRequeueAfterDuration());
        } else {
            return DeleteControl.defaultDelete();
        }
    }

    public static <T extends HasMetadata> Optional<T> getOrCreateSecondaryResource(
            final KubernetesClient client,
            final T resource) {
        Optional<T> current = getResource(client, resource);
        if (current.isEmpty()) {
            // Adding retry logic to overcome a k8s bug:
            // https://github.com/kubernetes/kubernetes/issues/67761
            long maxAttempts = DriverCreateMaxAttempts.getValue();
            long attemptCount = 1;
            while (true) {
                try {
                    current = Optional.ofNullable(client.resource(resource).create());
                    break;
                } catch (KubernetesClientException e) {
                    if (log.isErrorEnabled()) {
                        log.error(
                                "Failed to request resource with responseCode={} " +
                                        "attemptCount={}/{}",
                                e.getCode(), attemptCount, maxAttempts);
                    }
                    // retry only on 409 Conflict
                    if (e.getCode() != 409) {
                        throw e;
                    } else {
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
                    }
                }
            }
        }
        return current;
    }

    public static void addOwnerReferenceSecondaryResource(final KubernetesClient client,
                                                          final List<HasMetadata> resources,
                                                          final HasMetadata owner) {

        resources.forEach(r -> {
            ObjectMeta metaData = new ObjectMetaBuilder(r.getMetadata())
                    .addToOwnerReferences(buildOwnerReferenceTo(owner))
                    .build();
            r.setMetadata(metaData);
        });
        client.resourceList(resources).forceConflicts().serverSideApply();
    }

    public static <T extends HasMetadata> Optional<T> getResource(final KubernetesClient client,
                                                                  final T desired) {
        T resource = null;
        try {
            resource = client.resource(desired).get();
        } catch (KubernetesClientException e) {
            if (e.getCode() == 404) {
                return Optional.empty();
            }
        }
        return Optional.ofNullable(resource);
    }

    public static <T extends HasMetadata> void deleteResourceIfExists(final KubernetesClient client,
                                                                      final T resource,
                                                                      boolean forceDelete) {
        try {
            if (forceDelete) {
                client.resource(resource)
                        .withGracePeriod(0L)
                        .delete();
            } else {
                client.resource(resource)
                        .withPropagationPolicy(DeletionPropagation.FOREGROUND)
                        .withTimeout(ForegroundRequestTimeoutSeconds.getValue(), TimeUnit.SECONDS)
                        .delete();
            }
        } catch (KubernetesClientException e) {
            if (e.getCode() != 404) {
                throw e;
            } else {
                log.info("Pod to delete does not exist, proceeding...");
            }
        }
    }

    public static <T> T clone(T object) {
        if (object == null) {
            return null;
        }
        try {
            return (T)
                    objectMapper.readValue(
                            objectMapper.writeValueAsString(object), object.getClass());
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }
}
