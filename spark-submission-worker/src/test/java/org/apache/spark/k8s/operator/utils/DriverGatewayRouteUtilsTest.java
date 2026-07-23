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

import java.util.List;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceSpec;
import io.fabric8.kubernetes.api.model.ServiceSpecBuilder;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.GRPCBackendRef;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.GRPCRoute;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.GRPCRouteRule;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.GRPCRouteSpec;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.GRPCRouteSpecBuilder;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.HTTPBackendRef;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.HTTPRoute;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.HTTPRouteRule;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.HTTPRouteSpec;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.HTTPRouteSpecBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.spec.DriverGrpcRouteSpec;
import org.apache.spark.k8s.operator.spec.DriverHttpRouteSpec;

class DriverGatewayRouteUtilsTest {

  @Test
  void buildHttpRouteServiceWithDefaultRule() {
    // SPARK-58160: default rule targets the Service's first port when user omits rules.
    ObjectMeta driverMeta =
        new ObjectMetaBuilder()
            .withName("foo-driver")
            .withNamespace("foo-ns")
            .addToLabels("foo", "bar")
            .build();
    DriverHttpRouteSpec spec = buildHttpSpecWithSinglePortService();

    List<HasMetadata> resources = DriverGatewayRouteUtils.buildHttpRouteService(spec, driverMeta);

    Assertions.assertEquals(2, resources.size());
    Assertions.assertInstanceOf(Service.class, resources.get(0));
    Assertions.assertInstanceOf(HTTPRoute.class, resources.get(1));

    Service service = (Service) resources.get(0);
    Assertions.assertEquals(driverMeta.getLabels(), service.getSpec().getSelector());
    Assertions.assertEquals(driverMeta.getNamespace(), service.getMetadata().getNamespace());

    HTTPRoute route = (HTTPRoute) resources.get(1);
    Assertions.assertEquals(1, route.getSpec().getRules().size());
    HTTPRouteRule rule = route.getSpec().getRules().get(0);
    Assertions.assertEquals(1, rule.getMatches().size());
    Assertions.assertEquals("PathPrefix", rule.getMatches().get(0).getPath().getType());
    Assertions.assertEquals("/", rule.getMatches().get(0).getPath().getValue());
    Assertions.assertEquals(1, rule.getBackendRefs().size());
    HTTPBackendRef backend = rule.getBackendRefs().get(0);
    Assertions.assertEquals(service.getMetadata().getName(), backend.getName());
    Assertions.assertEquals("Service", backend.getKind());
    Assertions.assertEquals(service.getSpec().getPorts().get(0).getPort(), backend.getPort());
  }

  @Test
  void buildHttpRouteServiceWithUserProvidedRules() {
    // SPARK-58160: user-provided HTTPRouteSpec rules pass through unchanged.
    ObjectMeta driverMeta =
        new ObjectMetaBuilder().withName("foo-driver").withNamespace("foo-ns").build();
    DriverHttpRouteSpec spec = buildHttpSpecWithRules();

    List<HasMetadata> resources = DriverGatewayRouteUtils.buildHttpRouteService(spec, driverMeta);

    Assertions.assertEquals(2, resources.size());
    HTTPRoute route = (HTTPRoute) resources.get(1);
    // User-provided rules pass through unchanged.
    Assertions.assertEquals(spec.getHttpRouteSpec().getRules(), route.getSpec().getRules());
    Assertions.assertEquals(spec.getHttpRouteSpec().getHostnames(), route.getSpec().getHostnames());
    Assertions.assertEquals(
        spec.getHttpRouteSpec().getParentRefs(), route.getSpec().getParentRefs());
  }

  @Test
  void buildGrpcRouteServiceWithDefaultRule() {
    // SPARK-58160: GRPCRoute default rule targets the Service's first port.
    ObjectMeta driverMeta =
        new ObjectMetaBuilder()
            .withName("foo-driver")
            .withNamespace("foo-ns")
            .addToLabels("foo", "bar")
            .build();
    DriverGrpcRouteSpec spec = buildGrpcSpecWithSinglePortService();

    List<HasMetadata> resources = DriverGatewayRouteUtils.buildGrpcRouteService(spec, driverMeta);

    Assertions.assertEquals(2, resources.size());
    Assertions.assertInstanceOf(Service.class, resources.get(0));
    Assertions.assertInstanceOf(GRPCRoute.class, resources.get(1));

    Service service = (Service) resources.get(0);
    Assertions.assertEquals(driverMeta.getLabels(), service.getSpec().getSelector());
    Assertions.assertEquals(driverMeta.getNamespace(), service.getMetadata().getNamespace());

    GRPCRoute route = (GRPCRoute) resources.get(1);
    Assertions.assertEquals(1, route.getSpec().getRules().size());
    GRPCRouteRule rule = route.getSpec().getRules().get(0);
    Assertions.assertEquals(1, rule.getBackendRefs().size());
    GRPCBackendRef backend = rule.getBackendRefs().get(0);
    Assertions.assertEquals(service.getMetadata().getName(), backend.getName());
    Assertions.assertEquals("Service", backend.getKind());
    Assertions.assertEquals(service.getSpec().getPorts().get(0).getPort(), backend.getPort());
  }

  @Test
  void buildGrpcRouteServiceWithUserProvidedRules() {
    // SPARK-58160: user-provided GRPCRouteSpec rules pass through unchanged.
    ObjectMeta driverMeta =
        new ObjectMetaBuilder().withName("foo-driver").withNamespace("foo-ns").build();
    DriverGrpcRouteSpec spec = buildGrpcSpecWithRules();

    List<HasMetadata> resources = DriverGatewayRouteUtils.buildGrpcRouteService(spec, driverMeta);

    Assertions.assertEquals(2, resources.size());
    GRPCRoute route = (GRPCRoute) resources.get(1);
    Assertions.assertEquals(spec.getGrpcRouteSpec().getRules(), route.getSpec().getRules());
    Assertions.assertEquals(
        spec.getGrpcRouteSpec().getParentRefs(), route.getSpec().getParentRefs());
  }

  private DriverHttpRouteSpec buildHttpSpecWithSinglePortService() {
    ServiceSpec serviceSpec =
        new ServiceSpecBuilder()
            .addNewPort()
            .withPort(80)
            .withProtocol("TCP")
            .withTargetPort(new IntOrString(4040))
            .endPort()
            .withType("ClusterIP")
            .build();
    ObjectMeta serviceMeta = new ObjectMetaBuilder().withName("foo-ui-svc").build();
    ObjectMeta routeMeta = new ObjectMetaBuilder().withName("foo-ui-route").build();
    return DriverHttpRouteSpec.builder()
        .serviceMetadata(serviceMeta)
        .serviceSpec(serviceSpec)
        .httpRouteMetadata(routeMeta)
        .build();
  }

  private DriverHttpRouteSpec buildHttpSpecWithRules() {
    ServiceSpec serviceSpec =
        new ServiceSpecBuilder()
            .addNewPort()
            .withPort(80)
            .withProtocol("TCP")
            .withTargetPort(new IntOrString(4040))
            .endPort()
            .withType("ClusterIP")
            .build();
    HTTPRouteSpec httpRouteSpec =
        new HTTPRouteSpecBuilder()
            .withHostnames("spark.example.com")
            .addNewParentRef()
            .withName("shared-gateway")
            .withNamespace("gateway-system")
            .withSectionName("https")
            .endParentRef()
            .addNewRule()
            .addNewMatch()
            .withNewPath()
            .withType("PathPrefix")
            .withValue("/spark-app")
            .endPath()
            .endMatch()
            .addNewBackendRef()
            .withGroup("")
            .withKind("Service")
            .withName("foo-ui-svc")
            .withPort(80)
            .endBackendRef()
            .endRule()
            .build();
    ObjectMeta serviceMeta = new ObjectMetaBuilder().withName("foo-ui-svc").build();
    ObjectMeta routeMeta = new ObjectMetaBuilder().withName("foo-ui-route").build();
    return DriverHttpRouteSpec.builder()
        .serviceMetadata(serviceMeta)
        .serviceSpec(serviceSpec)
        .httpRouteMetadata(routeMeta)
        .httpRouteSpec(httpRouteSpec)
        .build();
  }

  private DriverGrpcRouteSpec buildGrpcSpecWithSinglePortService() {
    ServiceSpec serviceSpec =
        new ServiceSpecBuilder()
            .addNewPort()
            .withPort(15002)
            .withProtocol("TCP")
            .withTargetPort(new IntOrString(15002))
            .endPort()
            .withType("ClusterIP")
            .build();
    ObjectMeta serviceMeta = new ObjectMetaBuilder().withName("foo-connect-svc").build();
    ObjectMeta routeMeta = new ObjectMetaBuilder().withName("foo-connect-route").build();
    return DriverGrpcRouteSpec.builder()
        .serviceMetadata(serviceMeta)
        .serviceSpec(serviceSpec)
        .grpcRouteMetadata(routeMeta)
        .build();
  }

  private DriverGrpcRouteSpec buildGrpcSpecWithRules() {
    ServiceSpec serviceSpec =
        new ServiceSpecBuilder()
            .addNewPort()
            .withPort(15002)
            .withProtocol("TCP")
            .withTargetPort(new IntOrString(15002))
            .endPort()
            .withType("ClusterIP")
            .build();
    GRPCRouteSpec grpcRouteSpec =
        new GRPCRouteSpecBuilder()
            .addNewParentRef()
            .withName("shared-gateway")
            .withNamespace("gateway-system")
            .withSectionName("grpc")
            .endParentRef()
            .addNewRule()
            .addNewMatch()
            .editOrNewMethod()
            .withService("spark.connect.SparkConnectService")
            .endMethod()
            .endMatch()
            .addNewBackendRef()
            .withGroup("")
            .withKind("Service")
            .withName("foo-connect-svc")
            .withPort(15002)
            .endBackendRef()
            .endRule()
            .build();
    ObjectMeta serviceMeta = new ObjectMetaBuilder().withName("foo-connect-svc").build();
    ObjectMeta routeMeta = new ObjectMetaBuilder().withName("foo-connect-route").build();
    return DriverGrpcRouteSpec.builder()
        .serviceMetadata(serviceMeta)
        .serviceSpec(serviceSpec)
        .grpcRouteMetadata(routeMeta)
        .grpcRouteSpec(grpcRouteSpec)
        .build();
  }
}
