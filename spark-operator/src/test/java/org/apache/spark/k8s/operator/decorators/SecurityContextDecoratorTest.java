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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import org.junit.jupiter.api.Test;

class SecurityContextDecoratorTest {

  @Test
  void defaultsAllowPrivilegeEscalationWhenSecurityContextMissing() {
    Pod pod =
        new PodBuilder()
            .withNewMetadata()
            .withName("driver")
            .endMetadata()
            .withNewSpec()
            .addNewContainer()
            .withName("spark-kubernetes-driver")
            .endContainer()
            .endSpec()
            .build();

    new SecurityContextDecorator().decorate(pod);

    Container container = pod.getSpec().getContainers().get(0);
    assertNotNull(container.getSecurityContext());
    assertFalse(container.getSecurityContext().getAllowPrivilegeEscalation());
  }

  @Test
  void defaultsAllowPrivilegeEscalationButKeepsOtherFields() {
    Pod pod =
        new PodBuilder()
            .withNewMetadata()
            .withName("driver")
            .endMetadata()
            .withNewSpec()
            .addNewContainer()
            .withName("driver")
            .withNewSecurityContext()
            .withReadOnlyRootFilesystem(true)
            .endSecurityContext()
            .endContainer()
            .endSpec()
            .build();

    new SecurityContextDecorator().decorate(pod);

    SecurityContext securityContext = pod.getSpec().getContainers().get(0).getSecurityContext();
    assertFalse(securityContext.getAllowPrivilegeEscalation());
    assertTrue(securityContext.getReadOnlyRootFilesystem());
  }

  @Test
  void preservesExplicitAllowPrivilegeEscalation() {
    Pod pod =
        new PodBuilder()
            .withNewMetadata()
            .withName("driver")
            .endMetadata()
            .withNewSpec()
            .addNewContainer()
            .withName("driver")
            .withNewSecurityContext()
            .withAllowPrivilegeEscalation(true)
            .endSecurityContext()
            .endContainer()
            .endSpec()
            .build();

    new SecurityContextDecorator().decorate(pod);

    assertTrue(
        pod.getSpec().getContainers().get(0).getSecurityContext().getAllowPrivilegeEscalation());
  }

  @Test
  void defaultsInitContainers() {
    Pod pod =
        new PodBuilder()
            .withNewMetadata()
            .withName("driver")
            .endMetadata()
            .withNewSpec()
            .addNewInitContainer()
            .withName("init")
            .endInitContainer()
            .addNewContainer()
            .withName("driver")
            .endContainer()
            .endSpec()
            .build();

    new SecurityContextDecorator().decorate(pod);

    Container initContainer = pod.getSpec().getInitContainers().get(0);
    Container container = pod.getSpec().getContainers().get(0);
    assertFalse(initContainer.getSecurityContext().getAllowPrivilegeEscalation());
    assertFalse(container.getSecurityContext().getAllowPrivilegeEscalation());
  }

  @Test
  void defaultsStatefulSetTemplateContainers() {
    StatefulSet statefulSet =
        new StatefulSetBuilder()
            .withNewMetadata()
            .withName("worker")
            .endMetadata()
            .withNewSpec()
            .withNewTemplate()
            .withNewSpec()
            .addNewContainer()
            .withName("worker")
            .endContainer()
            .endSpec()
            .endTemplate()
            .endSpec()
            .build();

    new SecurityContextDecorator().decorate(statefulSet);

    Container container = statefulSet.getSpec().getTemplate().getSpec().getContainers().get(0);
    assertFalse(container.getSecurityContext().getAllowPrivilegeEscalation());
  }

  @Test
  void ignoresOtherResourceKinds() {
    Service service =
        new ServiceBuilder()
            .withNewMetadata()
            .withName("svc")
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();

    Service result = new SecurityContextDecorator().decorate(service);

    assertSame(service, result);
  }
}
