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

package org.apache.spark.k8s.operator.config;

import java.util.Set;
import java.util.function.Function;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * This serves dynamic configuration for Spark Operator. When enabled, Operator assumes config file
 * is located in given config map. It would keep watch the config map and apply changes when update
 * is detected.
 */
@ControllerConfiguration
@RequiredArgsConstructor
@Slf4j
public class SparkOperatorConfigMapReconciler implements Reconciler<ConfigMap> {
  private final Function<Set<String>, Boolean> namespaceUpdater;
  private final Function<Void, Set<String>> watchedNamespacesGetter;

  /**
   * Updates the error status of the ConfigMap reconciliation.
   *
   * @param resource The ConfigMap resource.
   * @param context The reconciliation context.
   * @param e The exception that occurred.
   * @return An ErrorStatusUpdateControl indicating no status update.
   */
  @Override
  public ErrorStatusUpdateControl<ConfigMap> updateErrorStatus(
      ConfigMap resource, Context<ConfigMap> context, Exception e) {
    log.error("Failed to reconcile dynamic config change.");
    return ErrorStatusUpdateControl.noStatusUpdate();
  }

  /**
   * Reconciles the ConfigMap resource, refreshing the Spark Operator configuration.
   *
   * @param resource The ConfigMap resource to reconcile.
   * @param context The reconciliation context.
   * @return An UpdateControl indicating no update.
   * @throws Exception if an error occurs during reconciliation.
   */
  @Override
  public UpdateControl<ConfigMap> reconcile(ConfigMap resource, Context<ConfigMap> context)
      throws Exception {
    SparkOperatorConfManager.INSTANCE.refresh(resource.getData());
    namespaceUpdater.apply(watchedNamespacesGetter.apply(null));
    return UpdateControl.noUpdate();
  }
}
