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

package org.apache.spark.k8s.operator.reconciler.reconcilesteps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.ServiceSpec;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.GRPCRouteSpec;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.GRPCRouteSpecBuilder;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.HTTPRouteSpec;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.HTTPRouteSpecBuilder;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.ParentReference;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.ParentReferenceBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.spec.DriverGrpcRouteSpec;
import org.apache.spark.k8s.operator.spec.DriverHttpRouteSpec;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;

class AppValidateStepTest {

  private static final ParentReference PARENT_REF =
      new ParentReferenceBuilder().withGroup("gateway.networking.k8s.io").withKind("Gateway")
          .withName("dex-gw").build();

  private static SparkApplication appWith(ApplicationSpec spec) {
    SparkApplication app = new SparkApplication();
    ObjectMeta meta = new ObjectMeta();
    meta.setName("app1");
    meta.setNamespace("default");
    app.setMetadata(meta);
    app.setSpec(spec);
    ApplicationStatus status =
        new ApplicationStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.Submitted, "ok"));
    app.setStatus(status);
    return app;
  }

  private static DriverHttpRouteSpec httpEntry(HTTPRouteSpec routeSpec) {
    DriverHttpRouteSpec s = new DriverHttpRouteSpec();
    s.setServiceMetadata(new ObjectMetaBuilder().withName("svc").build());
    s.setServiceSpec(new ServiceSpec());
    s.setHttpRouteMetadata(new ObjectMetaBuilder().withName("route").build());
    s.setHttpRouteSpec(routeSpec);
    return s;
  }

  private static DriverGrpcRouteSpec grpcEntry(GRPCRouteSpec routeSpec) {
    DriverGrpcRouteSpec s = new DriverGrpcRouteSpec();
    s.setServiceMetadata(new ObjectMetaBuilder().withName("svc").build());
    s.setServiceSpec(new ServiceSpec());
    s.setGrpcRouteMetadata(new ObjectMetaBuilder().withName("route").build());
    s.setGrpcRouteSpec(routeSpec);
    return s;
  }

  // Tests below cover SPARK-58160 Gateway API HTTPRoute/GRPCRoute validation.

  @Test
  void nullSpecReturnsNoError() {
    Assertions.assertNull(AppValidateStep.validateGatewayRoutes(null));
  }

  @Test
  void emptyRouteListsReturnNoError() {
    Assertions.assertNull(AppValidateStep.validateGatewayRoutes(new ApplicationSpec()));
  }

  @Test
  void httpRouteWithParentRefsPasses() {
    HTTPRouteSpec routeSpec = new HTTPRouteSpecBuilder().addToParentRefs(PARENT_REF).build();
    ApplicationSpec spec = new ApplicationSpec();
    spec.setDriverHttpRouteList(List.of(httpEntry(routeSpec)));
    Assertions.assertNull(AppValidateStep.validateGatewayRoutes(spec));
  }

  @Test
  void grpcRouteWithParentRefsPasses() {
    GRPCRouteSpec routeSpec = new GRPCRouteSpecBuilder().addToParentRefs(PARENT_REF).build();
    ApplicationSpec spec = new ApplicationSpec();
    spec.setDriverGrpcRouteList(List.of(grpcEntry(routeSpec)));
    Assertions.assertNull(AppValidateStep.validateGatewayRoutes(spec));
  }

  @Test
  void httpRouteMissingParentRefsIsRejected() {
    ApplicationSpec spec = new ApplicationSpec();
    spec.setDriverHttpRouteList(List.of(httpEntry(new HTTPRouteSpec())));
    String err = AppValidateStep.validateGatewayRoutes(spec);
    Assertions.assertNotNull(err);
    Assertions.assertTrue(err.contains("driverHttpRouteList[0].httpRouteSpec.parentRefs"), err);
  }

  @Test
  void httpRouteNullSpecIsRejected() {
    ApplicationSpec spec = new ApplicationSpec();
    spec.setDriverHttpRouteList(List.of(httpEntry(null)));
    String err = AppValidateStep.validateGatewayRoutes(spec);
    Assertions.assertNotNull(err);
    Assertions.assertTrue(err.contains("driverHttpRouteList[0]"), err);
  }

  @Test
  void httpEntryMissingServiceSpecIsRejected() {
    HTTPRouteSpec routeSpec = new HTTPRouteSpecBuilder().addToParentRefs(PARENT_REF).build();
    DriverHttpRouteSpec entry = httpEntry(routeSpec);
    entry.setServiceSpec(null);
    ApplicationSpec spec = new ApplicationSpec();
    spec.setDriverHttpRouteList(List.of(entry));
    String err = AppValidateStep.validateGatewayRoutes(spec);
    Assertions.assertNotNull(err);
    Assertions.assertTrue(err.contains("driverHttpRouteList[0].serviceSpec"), err);
  }

  @Test
  void httpEntryMissingServiceMetadataNameIsRejected() {
    HTTPRouteSpec routeSpec = new HTTPRouteSpecBuilder().addToParentRefs(PARENT_REF).build();
    DriverHttpRouteSpec entry = httpEntry(routeSpec);
    entry.setServiceMetadata(new ObjectMeta());
    ApplicationSpec spec = new ApplicationSpec();
    spec.setDriverHttpRouteList(List.of(entry));
    String err = AppValidateStep.validateGatewayRoutes(spec);
    Assertions.assertNotNull(err);
    Assertions.assertTrue(err.contains("driverHttpRouteList[0].serviceMetadata.name"), err);
  }

  @Test
  void httpEntryMissingRouteMetadataNameIsRejected() {
    HTTPRouteSpec routeSpec = new HTTPRouteSpecBuilder().addToParentRefs(PARENT_REF).build();
    DriverHttpRouteSpec entry = httpEntry(routeSpec);
    entry.setHttpRouteMetadata(new ObjectMeta());
    ApplicationSpec spec = new ApplicationSpec();
    spec.setDriverHttpRouteList(List.of(entry));
    String err = AppValidateStep.validateGatewayRoutes(spec);
    Assertions.assertNotNull(err);
    Assertions.assertTrue(err.contains("driverHttpRouteList[0].httpRouteMetadata.name"), err);
  }

  @Test
  void grpcEntryMissingServiceSpecIsRejected() {
    GRPCRouteSpec routeSpec = new GRPCRouteSpecBuilder().addToParentRefs(PARENT_REF).build();
    DriverGrpcRouteSpec entry = grpcEntry(routeSpec);
    entry.setServiceSpec(null);
    ApplicationSpec spec = new ApplicationSpec();
    spec.setDriverGrpcRouteList(List.of(entry));
    String err = AppValidateStep.validateGatewayRoutes(spec);
    Assertions.assertNotNull(err);
    Assertions.assertTrue(err.contains("driverGrpcRouteList[0].serviceSpec"), err);
  }

  @Test
  void grpcEntryMissingRouteMetadataNameIsRejected() {
    GRPCRouteSpec routeSpec = new GRPCRouteSpecBuilder().addToParentRefs(PARENT_REF).build();
    DriverGrpcRouteSpec entry = grpcEntry(routeSpec);
    entry.setGrpcRouteMetadata(new ObjectMeta());
    ApplicationSpec spec = new ApplicationSpec();
    spec.setDriverGrpcRouteList(List.of(entry));
    String err = AppValidateStep.validateGatewayRoutes(spec);
    Assertions.assertNotNull(err);
    Assertions.assertTrue(err.contains("driverGrpcRouteList[0].grpcRouteMetadata.name"), err);
  }

  @Test
  void grpcRouteMissingParentRefsIsRejected() {
    HTTPRouteSpec http = new HTTPRouteSpecBuilder().addToParentRefs(PARENT_REF).build();
    ApplicationSpec spec = new ApplicationSpec();
    spec.setDriverHttpRouteList(List.of(httpEntry(http)));
    spec.setDriverGrpcRouteList(List.of(grpcEntry(new GRPCRouteSpec())));
    String err = AppValidateStep.validateGatewayRoutes(spec);
    Assertions.assertNotNull(err);
    Assertions.assertTrue(err.contains("driverGrpcRouteList[0].grpcRouteSpec.parentRefs"), err);
  }

  @Test
  void reconcileMovesAppToFailedWhenParentRefsMissing() {
    AppValidateStep step = new AppValidateStep();
    ApplicationSpec spec = new ApplicationSpec();
    spec.setDriverHttpRouteList(List.of(httpEntry(new HTTPRouteSpec())));
    SparkApplication app = appWith(spec);

    SparkAppContext ctx = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    when(ctx.getResource()).thenReturn(app);
    when(recorder.persistStatus(any(), any())).thenReturn(true);

    ReconcileProgress progress = step.reconcile(ctx, recorder);
    Assertions.assertEquals(ReconcileProgress.completeAndImmediateRequeue(), progress);

    ArgumentCaptor<ApplicationStatus> captor = ArgumentCaptor.forClass(ApplicationStatus.class);
    Mockito.verify(recorder).persistStatus(any(), captor.capture());
    ApplicationStatus persisted = captor.getValue();
    Assertions.assertEquals(
        ApplicationStateSummary.Failed, persisted.getCurrentState().getCurrentStateSummary());
    Assertions.assertTrue(
        persisted.getCurrentState().getMessage().contains("parentRefs"),
        persisted.getCurrentState().getMessage());
  }

  @Test
  void reconcileSkipsRouteValidationForRunningApp() {
    AppValidateStep step = new AppValidateStep();
    ApplicationSpec spec = new ApplicationSpec();
    spec.setDriverHttpRouteList(List.of(httpEntry(new HTTPRouteSpec())));
    SparkApplication app = appWith(spec);
    app.setStatus(
        app.getStatus()
            .appendNewState(new ApplicationState(ApplicationStateSummary.RunningHealthy, "ok")));

    SparkAppContext ctx = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    when(ctx.getResource()).thenReturn(app);

    ReconcileProgress progress = step.reconcile(ctx, recorder);
    Assertions.assertEquals(ReconcileProgress.proceed(), progress);
    Mockito.verifyNoInteractions(recorder);
  }

  @Test
  void reconcileProceedsWhenRoutesValid() {
    AppValidateStep step = new AppValidateStep();
    HTTPRouteSpec routeSpec = new HTTPRouteSpecBuilder().addToParentRefs(PARENT_REF).build();
    ApplicationSpec spec = new ApplicationSpec();
    spec.setDriverHttpRouteList(List.of(httpEntry(routeSpec)));
    SparkApplication app = appWith(spec);

    SparkAppContext ctx = mock(SparkAppContext.class);
    SparkAppStatusRecorder recorder = mock(SparkAppStatusRecorder.class);
    when(ctx.getResource()).thenReturn(app);

    ReconcileProgress progress = step.reconcile(ctx, recorder);
    Assertions.assertEquals(ReconcileProgress.proceed(), progress);
  }

}
