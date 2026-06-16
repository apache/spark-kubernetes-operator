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

import java.util.List;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;

/**
 * Defaults each container's {@code securityContext.allowPrivilegeEscalation} to {@code false} when
 * it is not set. Containers that already specify a value are left untouched. Handles both a {@link
 * Pod} directly and the pod template of a {@link StatefulSet}; other resource kinds are a no-op.
 */
public class SecurityContextDecorator implements ResourceDecorator {

  @Override
  public <T extends HasMetadata> T decorate(T resource) {
    PodSpec podSpec = extractPodSpec(resource);
    if (podSpec != null) {
      disallowPrivilegeEscalationIfUnset(podSpec.getInitContainers());
      disallowPrivilegeEscalationIfUnset(podSpec.getContainers());
    }
    return resource;
  }

  private static PodSpec extractPodSpec(HasMetadata resource) {
    if (resource instanceof Pod pod) {
      return pod.getSpec();
    } else if (resource instanceof StatefulSet statefulSet) {
      PodTemplateSpec template = statefulSet.getSpec().getTemplate();
      return template == null ? null : template.getSpec();
    }
    return null;
  }

  private static void disallowPrivilegeEscalationIfUnset(List<Container> containers) {
    if (containers == null) {
      return;
    }
    for (Container container : containers) {
      SecurityContext securityContext = container.getSecurityContext();
      if (securityContext == null) {
        securityContext = new SecurityContext();
        container.setSecurityContext(securityContext);
      }
      if (securityContext.getAllowPrivilegeEscalation() == null) {
        securityContext.setAllowPrivilegeEscalation(false);
      }
    }
  }
}
