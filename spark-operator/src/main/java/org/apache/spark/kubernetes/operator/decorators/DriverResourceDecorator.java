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

package org.apache.spark.kubernetes.operator.decorators;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import lombok.RequiredArgsConstructor;
import org.apache.commons.collections.CollectionUtils;

import static org.apache.spark.kubernetes.operator.utils.ModelUtils.buildOwnerReferenceTo;

/**
 * Decorates Driver resources (except the pod).
 * This makes sure all resources have owner reference to the driver pod, so they can
 * be garbage collected upon termination.
 * Secondary resources would be garbage-collected if ALL owners are deleted. Therefore,
 * operator makes only driver pod has owned by the SparkApplication while all other
 * secondary resources are owned by the driver. In this way, after driver pod is deleted
 * at the end of each attempt, all other resources would be garbage collected automatically.
 */
@RequiredArgsConstructor
public class DriverResourceDecorator implements ResourceDecorator {
    private final Pod driverPod;

    @Override
    public <T extends HasMetadata> T decorate(T resource) {
        boolean ownerReferenceExists = false;
        if (CollectionUtils.isNotEmpty(resource.getMetadata().getOwnerReferences())) {
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
            ObjectMeta metaData = new ObjectMetaBuilder(resource.getMetadata())
                    .addToOwnerReferences(buildOwnerReferenceTo(driverPod))
                    .build();
            resource.setMetadata(metaData);
        }
        return resource;
    }
}
