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

package org.apache.spark.k8s.operator.metrics.source;

import static io.javaoperatorsdk.operator.api.reconciler.Constants.CONTROLLER_NAME;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.monitoring.Metrics;
import io.javaoperatorsdk.operator.api.reconciler.Constants;
import io.javaoperatorsdk.operator.api.reconciler.RetryInfo;
import io.javaoperatorsdk.operator.processing.Controller;
import io.javaoperatorsdk.operator.processing.GroupVersionKind;
import io.javaoperatorsdk.operator.processing.event.Event;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceAction;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceEvent;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.BaseResource;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.metrics.BaseOperatorSource;
import org.apache.spark.metrics.source.Source;
import org.apache.spark.util.Clock;
import org.apache.spark.util.SystemClock;

/** Metrics for the Java Operator SDK. */
@Slf4j
public class OperatorJosdkMetrics extends BaseOperatorSource implements Source, Metrics {
  public static final String FINISHED = "finished";
  public static final String CLEANUP = "cleanup";
  public static final String FAILED = "failed";
  public static final String RETRIES = "retries";
  private static final String RECONCILIATION = "reconciliation";
  private static final String RESOURCE = "resource";
  private static final String EVENT = "event";
  private static final String SUCCESS = "success";
  private static final String FAILURE = "failure";
  private static final String EXCEPTION = "exception";
  private static final String PREFIX = "operator.sdk";
  private static final String RECONCILIATIONS = "reconciliations";
  private static final String RECONCILIATIONS_EXECUTIONS = RECONCILIATIONS + ".executions";
  private static final String RECONCILIATIONS_QUEUE_SIZE = RECONCILIATIONS + ".queue.size";
  private static final String SIZE = "size";

  private final Clock clock;

  public OperatorJosdkMetrics() {
    super(new MetricRegistry());
    this.clock = new SystemClock();
  }

  @Override
  public String sourceName() {
    return PREFIX;
  }

  @Override
  public MetricRegistry metricRegistry() {
    return metricRegistry;
  }

  @Override
  public void controllerRegistered(Controller<? extends HasMetadata> controller) {
    // no-op
    log.debug("Controller has been registered");
  }

  @Override
  public void receivedEvent(Event event, Map<String, Object> metadata) {
    log.debug("received event {}, metadata {}", event, metadata);
    if (event instanceof ResourceEvent) {
      final ResourceAction action = ((ResourceEvent) event).getAction();
      final Optional<Class<? extends BaseResource<?, ?, ?, ?, ?>>> resource =
          getResourceClass(metadata);
      final Optional<String> namespaceOptional = event.getRelatedCustomResourceID().getNamespace();
      resource.ifPresent(
          aClass ->
              getCounter(getMetricNamePrefix(aClass), action.name().toLowerCase(), RESOURCE, EVENT)
                  .inc());
      if (resource.isPresent() && namespaceOptional.isPresent()) {
        getCounter(
                getMetricNamePrefix(resource.get()),
                namespaceOptional.get(),
                action.name().toLowerCase(),
                RESOURCE,
                EVENT)
            .inc();
      }
    }
  }

  @Override
  public <T> T timeControllerExecution(ControllerExecution<T> execution) throws Exception {
    log.debug("Time controller execution");
    final String name = execution.controllerName();
    final ResourceID resourceID = execution.resourceID();
    final Optional<String> namespaceOptional = resourceID.getNamespace();
    final Map<String, Object> metadata = execution.metadata();
    final Optional<Class<? extends BaseResource<?, ?, ?, ?, ?>>> resourceClass =
        getResourceClass(metadata);
    final String execName = execution.name();

    long startTime = clock.getTimeMillis();
    try {
      T result = execution.execute();
      final String successType = execution.successTypeName(result);
      if (resourceClass.isPresent()) {
        String metricsPrefix = getMetricNamePrefix(resourceClass.get());
        getHistogram(metricsPrefix, name, execName, successType).update(toSeconds(startTime));
        getCounter(metricsPrefix, name, execName, SUCCESS, successType).inc();
        if (namespaceOptional.isPresent()) {
          getHistogram(metricsPrefix, namespaceOptional.get(), name, execName, successType)
              .update(toSeconds(startTime));
          getCounter(metricsPrefix, namespaceOptional.get(), name, execName, SUCCESS, successType)
              .inc();
        }
      }
      return result;
    } catch (Exception e) {
      log.error(
          "Controller execution failed for resource {}, metadata {}", resourceID, metadata, e);
      final String exception = e.getClass().getSimpleName();
      if (resourceClass.isPresent()) {
        String metricsPrefix = getMetricNamePrefix(resourceClass.get());
        getHistogram(metricsPrefix, name, execName, FAILURE).update(toSeconds(startTime));
        getCounter(metricsPrefix, name, execName, FAILURE, EXCEPTION, exception).inc();
        if (namespaceOptional.isPresent()) {
          getHistogram(metricsPrefix, namespaceOptional.get(), name, execName, FAILURE)
              .update(toSeconds(startTime));
          getCounter(
                  metricsPrefix,
                  namespaceOptional.get(),
                  name,
                  execName,
                  FAILURE,
                  EXCEPTION,
                  exception)
              .inc();
        }
      }
      throw e;
    }
  }

  @Override
  public void reconcileCustomResource(
      HasMetadata resource, RetryInfo retryInfo, Map<String, Object> metadata) {
    log.debug(
        "Reconcile custom resource {}, with retryInfo {} metadata {}",
        resource,
        retryInfo,
        metadata);
    String metricsPrefix = getMetricNamePrefix(resource.getClass());
    if (retryInfo != null) {
      final String namespace = resource.getMetadata().getNamespace();
      getCounter(metricsPrefix, RECONCILIATION, RETRIES).inc();
      getCounter(metricsPrefix, namespace, RECONCILIATION, RETRIES).inc();
    }
    getCounter(metricsPrefix, (String) metadata.get(CONTROLLER_NAME), RECONCILIATIONS_QUEUE_SIZE)
        .inc();
  }

  @Override
  public void failedReconciliation(
      HasMetadata resource, Exception exception, Map<String, Object> metadata) {
    log.error(
        "Failed reconciliation for resource {} with metadata {}", resource, exception, exception);
    String metricsPrefix = getMetricNamePrefix(resource.getClass());
    getCounter(metricsPrefix, RECONCILIATION, FAILED).inc();
    getCounter(metricsPrefix, resource.getMetadata().getNamespace(), RECONCILIATION, FAILED).inc();
  }

  @Override
  public void finishedReconciliation(HasMetadata resource, Map<String, Object> metadata) {
    log.debug("Finished reconciliation for resource {} with metadata {}", resource, metadata);
    String metricsPrefix = getMetricNamePrefix(resource.getClass());
    getCounter(metricsPrefix, RECONCILIATION, FINISHED).inc();
    getCounter(metricsPrefix, resource.getMetadata().getNamespace(), RECONCILIATION, FINISHED);
  }

  @Override
  public void cleanupDoneFor(ResourceID resourceID, Map<String, Object> metadata) {
    log.debug("Cleanup Done for resource {} with metadata {}", resourceID, metadata);
    String metricsPrefix = getMetricNamePrefix(resourceID.getClass());
    getCounter(metricsPrefix, RECONCILIATION, CLEANUP).inc();
    resourceID
        .getNamespace()
        .ifPresent(ns -> getCounter(metricsPrefix, ns, RECONCILIATION, CLEANUP).inc());
  }

  @Override
  public <T extends Map<?, ?>> T monitorSizeOf(T map, String name) {
    log.debug("Monitor size for {}", name);
    Gauge<Integer> gauge =
        new Gauge<>() {
          @Override
          public Integer getValue() {
            return map.size();
          }
        };
    gauges.put(MetricRegistry.name(name, SIZE), gauge);
    return map;
  }

  @Override
  public void reconciliationExecutionStarted(HasMetadata resource, Map<String, Object> metadata) {
    log.debug("Reconciliation execution started");
    String namespace = resource.getMetadata().getNamespace();
    String metricsPrefix = getMetricNamePrefix(resource.getClass());
    getCounter(metricsPrefix, (String) metadata.get(CONTROLLER_NAME), RECONCILIATIONS_EXECUTIONS)
        .inc();
    getCounter(
            metricsPrefix,
            namespace,
            (String) metadata.get(CONTROLLER_NAME),
            RECONCILIATIONS_EXECUTIONS)
        .inc();
  }

  @Override
  public void reconciliationExecutionFinished(HasMetadata resource, Map<String, Object> metadata) {
    log.debug("Reconciliation execution finished");
    String namespace = resource.getMetadata().getNamespace();
    String metricsPrefix = getMetricNamePrefix(resource.getClass());
    getCounter(metricsPrefix, (String) metadata.get(CONTROLLER_NAME), RECONCILIATIONS_EXECUTIONS)
        .dec();
    getCounter(
            metricsPrefix,
            namespace,
            (String) metadata.get(CONTROLLER_NAME),
            RECONCILIATIONS_EXECUTIONS)
        .dec();
    getCounter(metricsPrefix, (String) metadata.get(CONTROLLER_NAME), RECONCILIATIONS_QUEUE_SIZE)
        .dec();
  }

  private long toSeconds(long startTimeInMilliseconds) {
    return TimeUnit.MILLISECONDS.toSeconds(clock.getTimeMillis() - startTimeInMilliseconds);
  }

  private Optional<Class<? extends BaseResource<?, ?, ?, ?, ?>>> getResourceClass(
      Map<String, Object> metadata) {
    GroupVersionKind resourceGvk = (GroupVersionKind) metadata.get(Constants.RESOURCE_GVK_KEY);

    if (resourceGvk == null) {
      return Optional.empty();
    }

    Class<? extends BaseResource<?, ?, ?, ?, ?>> resourceClass;

    if (resourceGvk.getKind().equals(SparkApplication.class.getSimpleName())) {
      resourceClass = SparkApplication.class;
    } else if (resourceGvk.getKind().equals(SparkCluster.class.getSimpleName())) {
      resourceClass = SparkCluster.class;
    } else {
      return Optional.empty();
    }
    return Optional.of(resourceClass);
  }
}
