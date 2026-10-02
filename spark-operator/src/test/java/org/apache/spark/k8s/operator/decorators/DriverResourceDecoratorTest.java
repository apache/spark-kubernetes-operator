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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import org.junit.jupiter.api.Test;

class DriverResourceDecoratorTest {

  private static Pod driverPod() {
    return new PodBuilder()
        .withKind("Pod")
        .withApiVersion("v1")
        .withNewMetadata()
        .withName("spark-driver")
        .withUid("driver-uid")
        .addToLabels("spark-app-name", "app1")
        .addToLabels("istio.io/use-waypoint", "none")
        .endMetadata()
        .build();
  }

  /**
   * Regression: a label set explicitly on the resource (e.g. a per-service waypoint label) must win
   * over a driver-pod label of the same key, not be overwritten by it.
   */
  @Test
  void perServiceLabelWinsOverDriverPodLabel() {
    Service service =
        new ServiceBuilder()
            .withNewMetadata()
            .withName("app1-connect-svc")
            .addToLabels("istio.io/use-waypoint", "waypoint-ns1")
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();

    new DriverResourceDecorator(driverPod()).decorate(service);

    assertEquals(
        "waypoint-ns1", service.getMetadata().getLabels().get("istio.io/use-waypoint"));
  }

  /** Driver-pod labels whose key is absent on the resource are still propagated (GC identity). */
  @Test
  void driverPodOnlyLabelsArePropagated() {
    Service service =
        new ServiceBuilder()
            .withNewMetadata()
            .withName("app1-connect-svc")
            .addToLabels("istio.io/use-waypoint", "waypoint-ns1")
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();

    new DriverResourceDecorator(driverPod()).decorate(service);

    Map<String, String> labels = service.getMetadata().getLabels();
    assertEquals("app1", labels.get("spark-app-name"));
    assertEquals("waypoint-ns1", labels.get("istio.io/use-waypoint"));
  }

  /** A resource with no labels of its own simply inherits the driver-pod labels (no NPE). */
  @Test
  void resourceWithoutLabelsInheritsDriverPodLabels() {
    Service service =
        new ServiceBuilder()
            .withNewMetadata()
            .withName("app1-driver-svc")
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();

    new DriverResourceDecorator(driverPod()).decorate(service);

    Map<String, String> labels = service.getMetadata().getLabels();
    assertEquals("none", labels.get("istio.io/use-waypoint"));
    assertEquals("app1", labels.get("spark-app-name"));
  }

  @Test
  void addsOwnerReferenceToDriver() {
    Service service =
        new ServiceBuilder()
            .withNewMetadata()
            .withName("app1-connect-svc")
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();

    new DriverResourceDecorator(driverPod()).decorate(service);

    assertNotNull(service.getMetadata().getOwnerReferences());
    assertEquals(1, service.getMetadata().getOwnerReferences().size());
    OwnerReference ref = service.getMetadata().getOwnerReferences().get(0);
    assertEquals("spark-driver", ref.getName());
    assertEquals("driver-uid", ref.getUid());
    assertTrue(ref.getBlockOwnerDeletion());
  }

  /**
   * When the resource already carries the driver-pod owner reference, decoration is a no-op, so an
   * existing per-service label is left untouched (and no duplicate owner reference is added).
   */
  @Test
  void skipsWhenOwnerReferenceAlreadyExists() {
    Service service =
        new ServiceBuilder()
            .withNewMetadata()
            .withName("app1-connect-svc")
            .addToLabels("istio.io/use-waypoint", "waypoint-ns1")
            .addNewOwnerReference()
            .withKind("Pod")
            .withName("spark-driver")
            .withUid("driver-uid")
            .endOwnerReference()
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();

    new DriverResourceDecorator(driverPod()).decorate(service);

    assertEquals(1, service.getMetadata().getOwnerReferences().size());
    assertEquals(
        "waypoint-ns1", service.getMetadata().getLabels().get("istio.io/use-waypoint"));
  }
}
