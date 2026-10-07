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
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_OPERATOR_NAME;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_ROLE_NAME;
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
        .addToLabels(LABEL_SPARK_OPERATOR_NAME, "spark-operator")
        .addToLabels(LABEL_SPARK_APPLICATION_NAME, "app1")
        // Prefix-matched managed label that is not one of the exact-match keys.
        .addToLabels("spark.operator/submission-id", "sub-1")
        .addToLabels(LABEL_SPARK_ROLE_NAME, "driver")
        .addToLabels("spark-app-selector", "spark-app-selector-1")
        .addToLabels("spark-version", "4.2.0")
        .addToLabels("spark-app-name", "app1-name")
        .addToLabels("driver-only-label", "driver-value")   // neutral driver only label
        .addToLabels("overlapping-label", "driver-value")
        .endMetadata()
        .build();
  }

  /** Builds a Service carrying only the given metadata labels (decorator reads metadata only). */
  private static Service service(Map<String, String> labels) {
    return new ServiceBuilder()
        .withNewMetadata()
        .withName("app1-connect-svc")
        .addToLabels(labels)
        .endMetadata()
        .build();
  }

  /**
   * Driver-pod labels whose key is absent on the resource are propagated onto the resource
   * (including a neutral driver-only label), a label the resource sets itself wins on a key
   * collision, and a neutral label only the resource sets is
   * kept.
   */
  @Test
  void driverLabelsPropagatedAndResourceLabelWinsOnCollision() {
    Service service =
        service(
            Map.of(
                "overlapping-label", "resource-value",
                "resource-only-label", "resource-value"));

    new DriverResourceDecorator(driverPod()).decorate(service);

    assertEquals(
        Map.of(
            LABEL_SPARK_OPERATOR_NAME, "spark-operator",
            LABEL_SPARK_APPLICATION_NAME, "app1",
            "spark.operator/submission-id", "sub-1",
            LABEL_SPARK_ROLE_NAME, "driver",
            "spark-app-selector", "spark-app-selector-1",
            "spark-version", "4.2.0",
            "spark-app-name", "app1-name",
            "driver-only-label", "driver-value",
            "overlapping-label", "resource-value",
            "resource-only-label", "resource-value"),
        service.getMetadata().getLabels());
  }

  /** A resource whose label map is null inherits the driver-pod labels without an NPE. */
  @Test
  void resourceWithoutLabelsInheritsDriverPodLabels() {
    Service service = service(Map.of());
    // withNewMetadata() seeds an empty map, so clear it to exercise the null-labels path.
    service.getMetadata().setLabels(null);

    new DriverResourceDecorator(driverPod()).decorate(service);

    Map<String, String> labels = service.getMetadata().getLabels();
    assertEquals("app1", labels.get(LABEL_SPARK_APPLICATION_NAME));
    assertEquals("driver", labels.get(LABEL_SPARK_ROLE_NAME));
  }

  /**
   * Managed identity labels (exact-match keys and anything under the {@code spark.operator} prefix)
   * must keep the driver-pod value even when the resource sets a different value, while a
   * non-managed label is still allowed to win.
   */
  @Test
  void managedLabelsAreNotOverriddenByResource() {
    Service service =
        service(
            Map.of(
                LABEL_SPARK_OPERATOR_NAME, "rogue-operator",
                LABEL_SPARK_APPLICATION_NAME, "rogue-app",
                "spark.operator/submission-id", "rogue-sub",
                LABEL_SPARK_ROLE_NAME, "rogue-role",
                "spark-app-selector", "rogue-selector",
                "spark-version", "rogue-version",
                "spark-app-name", "rogue-app-name",
                "overlapping-label", "resource-value"));

    new DriverResourceDecorator(driverPod()).decorate(service);

    assertEquals(
        Map.of(
            LABEL_SPARK_OPERATOR_NAME, "spark-operator",
            LABEL_SPARK_APPLICATION_NAME, "app1",
            "spark.operator/submission-id", "sub-1",
            LABEL_SPARK_ROLE_NAME, "driver",
            "spark-app-selector", "spark-app-selector-1",
            "spark-version", "4.2.0",
            "spark-app-name", "app1-name",
            "driver-only-label", "driver-value",
            "overlapping-label", "resource-value"),
        service.getMetadata().getLabels());
  }

  /**
   * A resource with no existing owner reference gets exactly one pointing at the driver pod (by
   * name and uid, with blockOwnerDeletion), which is what drives cascading garbage collection when
   * the driver pod is deleted.
   */
  @Test
  void addsOwnerReferenceToDriver() {
    Service service = service(Map.of());

    new DriverResourceDecorator(driverPod()).decorate(service);

    assertNotNull(service.getMetadata().getOwnerReferences());
    assertEquals(1, service.getMetadata().getOwnerReferences().size());
    OwnerReference ref = service.getMetadata().getOwnerReferences().get(0);
    assertEquals("spark-driver", ref.getName());
    assertEquals("driver-uid", ref.getUid());
    assertTrue(ref.getBlockOwnerDeletion());
  }

  /**
   * When the resource already carries the driver-pod owner reference, decoration is a no-op: the
   * labels are left exactly as they were (no driver-pod labels added) and no duplicate owner
   * reference is appended.
   */
  @Test
  void skipsWhenOwnerReferenceAlreadyExists() {
    Service service =
        new ServiceBuilder()
            .withNewMetadata()
            .withName("app1-connect-svc")
            .addToLabels("overlapping-label", "resource-value")
            .addNewOwnerReference()
            .withKind("Pod")
            .withName("spark-driver")
            .withUid("driver-uid")
            .endOwnerReference()
            .endMetadata()
            .build();

    new DriverResourceDecorator(driverPod()).decorate(service);

    // Labels are untouched: no driver-pod labels were merged in.
    assertEquals(
        Map.of("overlapping-label", "resource-value"), service.getMetadata().getLabels());
    // And no duplicate owner reference was appended.
    assertEquals(1, service.getMetadata().getOwnerReferences().size());
  }
}
