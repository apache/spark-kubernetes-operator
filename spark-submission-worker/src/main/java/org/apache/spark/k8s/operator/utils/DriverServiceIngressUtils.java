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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressBuilder;
import io.fabric8.kubernetes.api.model.networking.v1.IngressSpec;
import io.fabric8.kubernetes.api.model.networking.v1.IngressSpecBuilder;

import org.apache.spark.k8s.operator.spec.DriverServiceIngressSpec;

/** Utility class for driver service ingress. */
public final class DriverServiceIngressUtils {
  private DriverServiceIngressUtils() {}

  /**
   * Builds the full specification for ingress and service resources for a driver.
   *
   * @param spec The DriverServiceIngressSpec.
   * @param driverPodMetaData The ObjectMeta of the driver pod.
   * @return A List of HasMetadata objects representing the ingress and service.
   */
  public static List<HasMetadata> buildIngressService(
      DriverServiceIngressSpec spec, ObjectMeta driverPodMetaData) {
    List<HasMetadata> resources = new ArrayList<>(2);
    Service service = buildService(spec, driverPodMetaData);
    resources.add(service);
    resources.add(buildIngress(spec, service));
    return resources;
  }

  /**
   * Builds a Kubernetes Service object based on the provided specifications.
   *
   * @param spec The DriverServiceIngressSpec.
   * @param driverPodMetaData The ObjectMeta of the driver pod, used for namespace and default
   *     selectors.
   * @return A Service object.
   */
  private static Service buildService(DriverServiceIngressSpec spec, ObjectMeta driverPodMetaData) {
    ObjectMeta serviceMeta = new ObjectMetaBuilder(spec.getServiceMetadata()).build();
    serviceMeta.setNamespace(driverPodMetaData.getNamespace());
    Map<String, String> selectors = spec.getServiceSpec().getSelector();
    if (selectors == null || selectors.isEmpty()) {
      selectors = driverPodMetaData.getLabels();
    }
    return new ServiceBuilder()
        .withMetadata(serviceMeta)
        .withNewSpecLike(spec.getServiceSpec())
        .withSelector(selectors)
        .endSpec()
        .build();
  }

  /**
   * Builds a Kubernetes Ingress object based on the provided specifications and associated Service.
   *
   * @param spec The DriverServiceIngressSpec.
   * @param service The Service object that the Ingress will route to.
   * @return An Ingress object.
   */
  private static Ingress buildIngress(DriverServiceIngressSpec spec, Service service) {
    ObjectMeta metadata = new ObjectMetaBuilder(spec.getIngressMetadata()).build();
    IngressSpec ingressSpec = new IngressSpecBuilder(spec.getIngressSpec()).build();
    if ((ingressSpec.getRules() == null || ingressSpec.getRules().isEmpty())
        && service.getSpec().getPorts() != null
        && !service.getSpec().getPorts().isEmpty()) {
      // if no rule is provided, populate default path with backend to the associated service
      ingressSpec =
          new IngressSpecBuilder()
              .addNewRule()
              .withNewHttp()
              .addNewPath()
              .withPath("/")
              .withPathType("ImplementationSpecific")
              .withNewBackend()
              .withNewService()
              .withName(service.getMetadata().getName())
              .withNewPort()
              .withNumber(service.getSpec().getPorts().get(0).getPort())
              .endPort()
              .endService()
              .endBackend()
              .endPath()
              .endHttp()
              .endRule()
              .build();
    }
    return new IngressBuilder().withMetadata(metadata).withSpec(ingressSpec).build();
  }
}
