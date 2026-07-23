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

import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.completeAndImmediateRequeue;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.proceed;
import static org.apache.spark.k8s.operator.utils.ModelUtils.isClientMode;
import static org.apache.spark.k8s.operator.utils.SparkAppStatusUtils.isValidApplicationStatus;

import java.util.List;

import io.fabric8.kubernetes.api.model.gatewayapi.v1.GRPCRouteSpec;
import io.fabric8.kubernetes.api.model.gatewayapi.v1.HTTPRouteSpec;
import lombok.extern.slf4j.Slf4j;

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

/** Validates the submitted app. This can be re-factored into webhook in the future. */
@Slf4j
public final class AppValidateStep extends AppReconcileStep {
  /**
   * Reconciles the application by validating its configuration and status.
   *
   * @param context The SparkAppContext for the application.
   * @param statusRecorder The SparkAppStatusRecorder for recording status updates.
   * @return The ReconcileProgress indicating the next step.
   */
  @Override
  public ReconcileProgress reconcile(
      SparkAppContext context, SparkAppStatusRecorder statusRecorder) {
    SparkApplication app = context.getResource();
    if (!isValidApplicationStatus(app)) {
      log.warn("Spark application found with empty status. Resetting to initial state.");
      return attemptStatusUpdate(context, statusRecorder, new ApplicationStatus(), proceed());
    }
    if (isClientMode(app)) {
      return failWith(context, statusRecorder, "Client mode is not supported yet.");
    }
    if (app.getStatus().getCurrentState().getCurrentStateSummary().isInitializing()) {
      String routeFailure = validateGatewayRoutes(app.getSpec());
      if (routeFailure != null) {
        return failWith(context, statusRecorder, routeFailure);
      }
    }
    return proceed();
  }

  private ReconcileProgress failWith(
      SparkAppContext context, SparkAppStatusRecorder statusRecorder, String message) {
    ApplicationState failure = new ApplicationState(ApplicationStateSummary.Failed, message);
    return attemptStatusUpdate(
        context,
        statusRecorder,
        context.getResource().getStatus().appendNewState(failure),
        completeAndImmediateRequeue());
  }

  static String validateGatewayRoutes(ApplicationSpec spec) {
    if (spec == null) {
      return null;
    }
    List<DriverHttpRouteSpec> httpList = spec.getDriverHttpRouteList();
    if (httpList != null) {
      for (int i = 0; i < httpList.size(); i++) {
        DriverHttpRouteSpec entry = httpList.get(i);
        String prefix = "driverHttpRouteList[" + i + "]";
        if (entry.getServiceSpec() == null) {
          return prefix + ".serviceSpec is required.";
        }
        if (entry.getServiceMetadata() == null
            || entry.getServiceMetadata().getName() == null
            || entry.getServiceMetadata().getName().isEmpty()) {
          return prefix + ".serviceMetadata.name is required.";
        }
        if (entry.getHttpRouteMetadata() == null
            || entry.getHttpRouteMetadata().getName() == null
            || entry.getHttpRouteMetadata().getName().isEmpty()) {
          return prefix + ".httpRouteMetadata.name is required.";
        }
        HTTPRouteSpec routeSpec = entry.getHttpRouteSpec();
        if (routeSpec == null
            || routeSpec.getParentRefs() == null
            || routeSpec.getParentRefs().isEmpty()) {
          return prefix + ".httpRouteSpec.parentRefs is required.";
        }
      }
    }
    List<DriverGrpcRouteSpec> grpcList = spec.getDriverGrpcRouteList();
    if (grpcList != null) {
      for (int i = 0; i < grpcList.size(); i++) {
        DriverGrpcRouteSpec entry = grpcList.get(i);
        String prefix = "driverGrpcRouteList[" + i + "]";
        if (entry.getServiceSpec() == null) {
          return prefix + ".serviceSpec is required.";
        }
        if (entry.getServiceMetadata() == null
            || entry.getServiceMetadata().getName() == null
            || entry.getServiceMetadata().getName().isEmpty()) {
          return prefix + ".serviceMetadata.name is required.";
        }
        if (entry.getGrpcRouteMetadata() == null
            || entry.getGrpcRouteMetadata().getName() == null
            || entry.getGrpcRouteMetadata().getName().isEmpty()) {
          return prefix + ".grpcRouteMetadata.name is required.";
        }
        GRPCRouteSpec routeSpec = entry.getGrpcRouteSpec();
        if (routeSpec == null
            || routeSpec.getParentRefs() == null
            || routeSpec.getParentRefs().isEmpty()) {
          return prefix + ".grpcRouteSpec.parentRefs is required.";
        }
      }
    }
    return null;
  }
}
