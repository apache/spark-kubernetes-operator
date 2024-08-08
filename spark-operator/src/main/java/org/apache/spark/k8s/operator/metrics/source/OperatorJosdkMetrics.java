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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
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
import org.apache.spark.metrics.source.Source;
import org.apache.spark.util.Clock;
import org.apache.spark.util.SystemClock;

@Slf4j
public class OperatorJosdkMetrics implements Source, Metrics {
  public static final String FINISHED = "finished";
  public static final String CLEANUP = "cleanup";
  public static final String FAILED = "failed";
  public static final String RETRIES = "retries";
  private final Map<String, Histogram> histograms = new ConcurrentHashMap<>();
  private final Map<String, Counter> counters = new ConcurrentHashMap<>();
  private final Map<String, Gauge<?>> gauges = new ConcurrentHashMap<>();
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
  private final MetricRegistry metricRegistry;

  public OperatorJosdkMetrics() {
    this.clock = new SystemClock();
    this.metricRegistry = new MetricRegistry();
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
          aClass -> getCounter(aClass, action.name().toLowerCase(), RESOURCE, EVENT).inc());
      if (resource.isPresent() && namespaceOptional.isPresent()) {
        getCounter(
                resource.get(),
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
        getHistogram(resourceClass.get(), name, execName, successType).update(toSeconds(startTime));
        getCounter(resourceClass.get(), name, execName, SUCCESS, successType).inc();
        if (namespaceOptional.isPresent()) {
          getHistogram(resourceClass.get(), namespaceOptional.get(), name, execName, successType)
              .update(toSeconds(startTime));
          getCounter(
                  resourceClass.get(),
                  namespaceOptional.get(),
                  name,
                  execName,
                  SUCCESS,
                  successType)
              .inc();
        }
      }
      return result;
    } catch (Exception e) {
      log.error(
          "Controller execution failed for resource {}, metadata {}", resourceID, metadata, e);
      final String exception = e.getClass().getSimpleName();
      if (resourceClass.isPresent()) {
        getHistogram(resourceClass.get(), name, execName, FAILURE).update(toSeconds(startTime));
        getCounter(resourceClass.get(), name, execName, FAILURE, EXCEPTION, exception).inc();
        if (namespaceOptional.isPresent()) {
          getHistogram(resourceClass.get(), namespaceOptional.get(), name, execName, FAILURE)
              .update(toSeconds(startTime));
          getCounter(
                  resourceClass.get(),
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
    if (retryInfo != null) {
      final String namespace = resource.getMetadata().getNamespace();
      getCounter(resource.getClass(), RECONCILIATION, RETRIES).inc();
      getCounter(resource.getClass(), namespace, RECONCILIATION, RETRIES).inc();
    }
    getCounter(
            resource.getClass(), (String) metadata.get(CONTROLLER_NAME), RECONCILIATIONS_QUEUE_SIZE)
        .inc();
  }

  @Override
  public void failedReconciliation(
      HasMetadata resource, Exception exception, Map<String, Object> metadata) {
    log.error(
        "Failed reconciliation for resource {} with metadata {}", resource, exception, exception);
    getCounter(resource.getClass(), RECONCILIATION, FAILED).inc();
    getCounter(resource.getClass(), resource.getMetadata().getNamespace(), RECONCILIATION, FAILED)
        .inc();
  }

  @Override
  public void finishedReconciliation(HasMetadata resource, Map<String, Object> metadata) {
    log.debug("Finished reconciliation for resource {} with metadata {}", resource, metadata);
    getCounter(resource.getClass(), RECONCILIATION, FINISHED).inc();
    getCounter(
        resource.getClass(), resource.getMetadata().getNamespace(), RECONCILIATION, FINISHED);
  }

  @Override
  public void cleanupDoneFor(ResourceID resourceID, Map<String, Object> metadata) {
    log.debug("Cleanup Done for resource {} with metadata {}", resourceID, metadata);
    getCounter(resourceID.getClass(), RECONCILIATION, CLEANUP).inc();
    resourceID
        .getNamespace()
        .ifPresent(ns -> getCounter(resourceID.getClass(), ns, RECONCILIATION, CLEANUP).inc());
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
    getCounter(
            resource.getClass(), (String) metadata.get(CONTROLLER_NAME), RECONCILIATIONS_EXECUTIONS)
        .inc();
    getCounter(
            resource.getClass(),
            namespace,
            (String) metadata.get(CONTROLLER_NAME),
            RECONCILIATIONS_EXECUTIONS)
        .inc();
  }

  @Override
  public void reconciliationExecutionFinished(HasMetadata resource, Map<String, Object> metadata) {
    log.debug("Reconciliation execution finished");
    String namespace = resource.getMetadata().getNamespace();
    getCounter(
            resource.getClass(), (String) metadata.get(CONTROLLER_NAME), RECONCILIATIONS_EXECUTIONS)
        .dec();
    getCounter(
            resource.getClass(),
            namespace,
            (String) metadata.get(CONTROLLER_NAME),
            RECONCILIATIONS_EXECUTIONS)
        .dec();
    getCounter(
            resource.getClass(), (String) metadata.get(CONTROLLER_NAME), RECONCILIATIONS_QUEUE_SIZE)
        .dec();
  }

  private long toSeconds(long startTimeInMilliseconds) {
    return TimeUnit.MILLISECONDS.toSeconds(clock.getTimeMillis() - startTimeInMilliseconds);
  }

  private Histogram getHistogram(Class<?> kclass, String... names) {
    String name = MetricRegistry.name(kclass.getSimpleName(), names).toLowerCase();
    Histogram histogram;
    if (histograms.containsKey(name)) {
      histogram = histograms.get(name);
    } else {
      histogram = metricRegistry.histogram(name);
      histograms.put(name, histogram);
    }
    return histogram;
  }

  private Counter getCounter(Class<?> klass, String... names) {
    String name = MetricRegistry.name(klass.getSimpleName(), names).toLowerCase();
    Counter counter;
    if (counters.containsKey(name)) {
      counter = counters.get(name);
    } else {
      counter = metricRegistry.counter(name);
      counters.put(name, counter);
    }
    return counter;
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
    } else {
      return Optional.empty();
    }
    return Optional.of(resourceClass);
  }
}
