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
import io.fabric8.kubernetes.api.model.gatewayapi.v1.GRPCRoute;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.GRPCRouteBuilder;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.GRPCRouteSpec;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.GRPCRouteSpecBuilder;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.HTTPRoute;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.HTTPRouteBuilder;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.HTTPRouteSpec;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.HTTPRouteSpecBuilder;

import org.apache.spark.k8s.operator.spec.DriverGrpcRouteSpec;
import org.apache.spark.k8s.operator.spec.DriverHttpRouteSpec;

/**
 * Utility class for building Gateway API {@code HTTPRoute} / {@code GRPCRoute} resources for a
 * driver.
 */
public final class DriverGatewayRouteUtils {
  private DriverGatewayRouteUtils() {}

  /**
   * Builds the Service + {@code HTTPRoute} resources for a driver.
   *
   * @param spec the {@link DriverHttpRouteSpec}.
   * @param driverPodMetaData the {@link ObjectMeta} of the driver pod.
   * @return a {@link List} containing the Service followed by the HTTPRoute.
   */
  public static List<HasMetadata> buildHttpRouteService(
      DriverHttpRouteSpec spec, ObjectMeta driverPodMetaData) {
    List<HasMetadata> resources = new ArrayList<>(2);
    Service service =
        buildService(spec.getServiceMetadata(), spec.getServiceSpec(), driverPodMetaData);
    resources.add(service);
    resources.add(buildHttpRoute(spec, service));
    return resources;
  }

  /**
   * Builds the Service + {@code GRPCRoute} resources for a driver.
   *
   * @param spec the {@link DriverGrpcRouteSpec}.
   * @param driverPodMetaData the {@link ObjectMeta} of the driver pod.
   * @return a {@link List} containing the Service followed by the GRPCRoute.
   */
  public static List<HasMetadata> buildGrpcRouteService(
      DriverGrpcRouteSpec spec, ObjectMeta driverPodMetaData) {
    List<HasMetadata> resources = new ArrayList<>(2);
    Service service =
        buildService(spec.getServiceMetadata(), spec.getServiceSpec(), driverPodMetaData);
    resources.add(service);
    resources.add(buildGrpcRoute(spec, service));
    return resources;
  }

  private static Service buildService(
      ObjectMeta serviceMetadata,
      io.fabric8.kubernetes.api.model.ServiceSpec serviceSpec,
      ObjectMeta driverPodMetaData) {
    ObjectMeta serviceMeta = new ObjectMetaBuilder(serviceMetadata).build();
    serviceMeta.setNamespace(driverPodMetaData.getNamespace());
    Map<String, String> selectors = serviceSpec.getSelector();
    if (selectors == null || selectors.isEmpty()) {
      selectors = driverPodMetaData.getLabels();
    }
    return new ServiceBuilder()
        .withMetadata(serviceMeta)
        .withNewSpecLike(serviceSpec)
        .withSelector(selectors)
        .endSpec()
        .build();
  }

  private static HTTPRoute buildHttpRoute(DriverHttpRouteSpec spec, Service service) {
    ObjectMeta metadata = new ObjectMetaBuilder(spec.getHttpRouteMetadata()).build();
    HTTPRouteSpec httpRouteSpec =
        spec.getHttpRouteSpec() == null
            ? new HTTPRouteSpec()
            : new HTTPRouteSpecBuilder(spec.getHttpRouteSpec()).build();
    if ((httpRouteSpec.getRules() == null || httpRouteSpec.getRules().isEmpty())
        && service.getSpec().getPorts() != null
        && !service.getSpec().getPorts().isEmpty()) {
      httpRouteSpec =
          new HTTPRouteSpecBuilder(httpRouteSpec)
              .addNewRule()
              .addNewMatch()
              .withNewPath()
              .withType("PathPrefix")
              .withValue("/")
              .endPath()
              .endMatch()
              .addNewBackendRef()
              .withGroup("")
              .withKind("Service")
              .withName(service.getMetadata().getName())
              .withPort(service.getSpec().getPorts().get(0).getPort())
              .endBackendRef()
              .endRule()
              .build();
    }
    return new HTTPRouteBuilder().withMetadata(metadata).withSpec(httpRouteSpec).build();
  }

  private static GRPCRoute buildGrpcRoute(DriverGrpcRouteSpec spec, Service service) {
    ObjectMeta metadata = new ObjectMetaBuilder(spec.getGrpcRouteMetadata()).build();
    GRPCRouteSpec grpcRouteSpec =
        spec.getGrpcRouteSpec() == null
            ? new GRPCRouteSpec()
            : new GRPCRouteSpecBuilder(spec.getGrpcRouteSpec()).build();
    if ((grpcRouteSpec.getRules() == null || grpcRouteSpec.getRules().isEmpty())
        && service.getSpec().getPorts() != null
        && !service.getSpec().getPorts().isEmpty()) {
      grpcRouteSpec =
          new GRPCRouteSpecBuilder(grpcRouteSpec)
              .addNewRule()
              .addNewBackendRef()
              .withGroup("")
              .withKind("Service")
              .withName(service.getMetadata().getName())
              .withPort(service.getSpec().getPorts().get(0).getPort())
              .endBackendRef()
              .endRule()
              .build();
    }
    return new GRPCRouteBuilder().withMetadata(metadata).withSpec(grpcRouteSpec).build();
  }
}
