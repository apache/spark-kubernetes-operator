/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.kubernetes.operator.config;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusHandler;
import io.javaoperatorsdk.operator.api.reconciler.ErrorStatusUpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.rate.RateLimited;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.kubernetes.operator.reconciler.SparkReconcilerUtils;

import static org.apache.spark.kubernetes.operator.config.SparkOperatorConf.OperatorNamespace;

/**
 * This serves dynamic configuration for Spark Operator.
 * When enabled, Operator assumes config file is located in given config map
 * It would keep watch the config map & apply changes when update is detected.
 */
@ControllerConfiguration
@RateLimited(maxReconciliations = 1, within = 30)
@RequiredArgsConstructor
@Slf4j
public class SparkOperatorConfigMapReconciler implements Reconciler<ConfigMap>,
    ErrorStatusHandler<ConfigMap>, EventSourceInitializer<ConfigMap> {
  private final Function<Set<String>, Boolean> namespaceUpdater;

  @Override
  public ErrorStatusUpdateControl<ConfigMap> updateErrorStatus(ConfigMap resource,
                                                               Context<ConfigMap> context,
                                                               Exception e) {
    log.error("Failed to reconcile dynamic config change.");
    return ErrorStatusUpdateControl.noStatusUpdate();
  }

  @Override
  public Map<String, EventSource> prepareEventSources(EventSourceContext<ConfigMap> context) {
    var configMapEventSource =
        new InformerEventSource<>(InformerConfiguration.from(ConfigMap.class, context)
            .withNamespaces(OperatorNamespace.getValue())
            .build(), context);
    return EventSourceInitializer.nameEventSources(configMapEventSource);
  }

  @Override
  public UpdateControl<ConfigMap> reconcile(ConfigMap resource, Context<ConfigMap> context)
      throws Exception {
    SparkOperatorConfManager.INSTANCE.refresh(resource.getData());
    namespaceUpdater.apply(SparkReconcilerUtils.getWatchedNamespaces());
    return UpdateControl.noUpdate();
  }
}
