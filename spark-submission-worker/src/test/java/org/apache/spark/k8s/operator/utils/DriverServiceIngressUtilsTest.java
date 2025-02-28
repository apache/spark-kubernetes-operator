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
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.kubernetes.api.model.networking.v1.IngressRule;
import io.fabric8.kubernetes.api.model.networking.v1.IngressSpec;
import io.fabric8.kubernetes.api.model.networking.v1.IngressSpecBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.spec.DriverServiceIngressSpec;

class DriverServiceIngressUtilsTest {
  @Test
  void buildIngressService() {
    ObjectMeta driverMeta =
        new ObjectMetaBuilder().withName("foo-driver").addToLabels("foo", "bar").build();
    DriverServiceIngressSpec spec1 = buildSpecWithSinglePortService();
    List<HasMetadata> resources1 = DriverServiceIngressUtils.buildIngressService(spec1, driverMeta);
    Assertions.assertEquals(2, resources1.size());
    Assertions.assertInstanceOf(Service.class, resources1.get(0));
    Assertions.assertInstanceOf(Ingress.class, resources1.get(1));
    Service service1 = (Service) resources1.get(0);
    Assertions.assertEquals(driverMeta.getLabels(), service1.getSpec().getSelector());
    Ingress ingress1 = (Ingress) resources1.get(1);
    Assertions.assertEquals(1, ingress1.getSpec().getRules().size());
    IngressRule ingressRule1 = ingress1.getSpec().getRules().get(0);
    Assertions.assertEquals(1, ingressRule1.getHttp().getPaths().size());
    Assertions.assertEquals(
        service1.getMetadata().getName(),
        ingressRule1.getHttp().getPaths().get(0).getBackend().getService().getName());
    Assertions.assertEquals(
        service1.getSpec().getPorts().get(0).getPort(),
        ingressRule1.getHttp().getPaths().get(0).getBackend().getService().getPort().getNumber());

    DriverServiceIngressSpec spec2 = buildSpecWithIngressSpec();
    List<HasMetadata> resources2 = DriverServiceIngressUtils.buildIngressService(spec2, driverMeta);
    Assertions.assertEquals(2, resources2.size());
    Assertions.assertInstanceOf(Service.class, resources2.get(0));
    Assertions.assertInstanceOf(Ingress.class, resources2.get(1));
    Service service2 = (Service) resources2.get(0);
    Assertions.assertEquals(driverMeta.getLabels(), service2.getSpec().getSelector());
    Ingress ingress2 = (Ingress) resources2.get(1);
    Assertions.assertEquals(spec2.getIngressSpec(), ingress2.getSpec());
  }

  private DriverServiceIngressSpec buildSpecWithSinglePortService() {
    ServiceSpec serviceSpec1 =
        new ServiceSpecBuilder()
            .addNewPort()
            .withPort(80)
            .withProtocol("TCP")
            .withTargetPort(new IntOrString(4040))
            .endPort()
            .withType("LoadBalancer")
            .build();
    ObjectMeta serviceMeta1 = new ObjectMetaBuilder().withName("foo-service-1").build();
    ObjectMeta ingressMeta1 = new ObjectMetaBuilder().withName("foo-ingress-1").build();
    return DriverServiceIngressSpec.builder()
        .serviceMetadata(serviceMeta1)
        .serviceSpec(serviceSpec1)
        .ingressMetadata(ingressMeta1)
        .build();
  }

  private DriverServiceIngressSpec buildSpecWithIngressSpec() {
    ServiceSpec serviceSpec2 =
        new ServiceSpecBuilder()
            .addNewPort()
            .withPort(19098)
            .withProtocol("TCP")
            .withTargetPort(new IntOrString(19098))
            .endPort()
            .addNewPort()
            .withPort(19099)
            .withProtocol("TCP")
            .withTargetPort(new IntOrString(19099))
            .endPort()
            .withType("ClusterIP")
            .build();
    IngressSpec ingressSpec2 =
        new IngressSpecBuilder()
            .withIngressClassName("nginx-example")
            .addNewRule()
            .withNewHttp()
            .addNewPath()
            .withPath("/testpath1")
            .withPathType("Prefix")
            .withNewBackend()
            .withNewService()
            .withName("foo-service-2")
            .withNewPort()
            .withNumber(19098)
            .endPort()
            .endService()
            .endBackend()
            .endPath()
            .endHttp()
            .endRule()
            .addNewRule()
            .withNewHttp()
            .addNewPath()
            .withPath("/testpath2")
            .withPathType("Prefix")
            .withNewBackend()
            .withNewService()
            .withName("foo-service-2")
            .withNewPort()
            .withNumber(19099)
            .endPort()
            .endService()
            .endBackend()
            .endPath()
            .endHttp()
            .endRule()
            .build();
    ObjectMeta serviceMeta2 = new ObjectMetaBuilder().withName("foo-service-2").build();
    ObjectMeta ingressMeta2 =
        new ObjectMetaBuilder()
            .withName("foo-ingress-2")
            .addToAnnotations("nginx.ingress.kubernetes.io/rewrite-target", "/")
            .build();
    return DriverServiceIngressSpec.builder()
        .serviceMetadata(serviceMeta2)
        .serviceSpec(serviceSpec2)
        .ingressMetadata(ingressMeta2)
        .ingressSpec(ingressSpec2)
        .build();
  }
}
