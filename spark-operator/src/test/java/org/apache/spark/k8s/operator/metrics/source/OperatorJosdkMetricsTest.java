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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import com.codahale.metrics.Metric;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.javaoperatorsdk.operator.api.monitoring.Metrics;
import io.javaoperatorsdk.operator.api.reconciler.Constants;
import io.javaoperatorsdk.operator.processing.GroupVersionKind;
import io.javaoperatorsdk.operator.processing.event.Event;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceAction;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.SparkApplication;

class OperatorJosdkMetricsTest {
  private static final String DEFAULT_NAMESPACE = "default";
  private static final String TEST_RESOURCE_NAME = "test1";
  private static final ResourceID resourceId = new ResourceID("spark-pi", "testns");

  private static final Map<String, Object> metadata =
      Map.of(
          Constants.RESOURCE_GVK_KEY,
          GroupVersionKind.gvkFor(SparkApplication.class),
          Constants.CONTROLLER_NAME,
          "test-controller-name");
  private static final String controllerName = "test-controller";

  private OperatorJosdkMetrics operatorMetrics;

  @BeforeEach
  void setup() {
    operatorMetrics = new OperatorJosdkMetrics();
  }

  @Test
  void testTimeControllerExecution() throws Exception {
    TestingExecutionBase<SparkApplication> successExecution = new TestingExecutionBase<>();
    operatorMetrics.timeControllerExecution(successExecution);
    Map<String, Metric> metrics = operatorMetrics.metricRegistry().getMetrics();
    assertEquals(4, metrics.size());
    assertTrue(metrics.containsKey("sparkapplication.test-controller.reconcile.both"));
    assertTrue(metrics.containsKey("sparkapplication.testns.test-controller.reconcile.both"));
    assertTrue(metrics.containsKey("sparkapplication.test-controller.reconcile.success.both"));
    assertTrue(
        metrics.containsKey("sparkapplication.testns.test-controller.reconcile.success.both"));

    FooTestingExecutionBase<SparkApplication> failedExecution = new FooTestingExecutionBase<>();
    try {
      operatorMetrics.timeControllerExecution(failedExecution);
    } catch (Exception e) {
      assertEquals("Foo exception", e.getMessage());
      assertEquals(8, metrics.size());
      assertTrue(metrics.containsKey("sparkapplication.test-controller.reconcile.failure"));
      assertTrue(
          metrics.containsKey(
              "sparkapplication.test-controller.reconcile.failure.exception"
                  + ".nosuchfieldexception"));
      assertTrue(metrics.containsKey("sparkapplication.testns.test-controller.reconcile.failure"));
      assertTrue(
          metrics.containsKey(
              "sparkapplication.testns.test-controller.reconcile.failure."
                  + "exception.nosuchfieldexception"));
    }
  }

  @Test
  void testReconciliationFinished() {
    operatorMetrics.finishedReconciliation(buildNamespacedResource(), metadata);
    Map<String, Metric> metrics = operatorMetrics.metricRegistry().getMetrics();
    assertEquals(2, metrics.size());
    assertTrue(metrics.containsKey("configmap.default.reconciliation.finished"));
    assertTrue(metrics.containsKey("configmap.reconciliation.finished"));
  }

  @Test
  void testReconciliationExecutionStartedAndFinished() {
    operatorMetrics.reconciliationExecutionStarted(buildNamespacedResource(), metadata);
    Map<String, Metric> metrics = operatorMetrics.metricRegistry().getMetrics();
    assertEquals(2, metrics.size());
    assertTrue(metrics.containsKey("configmap.test-controller-name.reconciliations.executions"));
    assertTrue(
        metrics.containsKey("configmap.default.test-controller-name.reconciliations.executions"));
    operatorMetrics.reconciliationExecutionFinished(buildNamespacedResource(), metadata);
    assertEquals(3, metrics.size());
    assertTrue(metrics.containsKey("configmap.test-controller-name.reconciliations.queue.size"));
  }

  @Test
  void testReceivedEvent() {
    Event event = new ResourceEvent(ResourceAction.ADDED, resourceId, buildNamespacedResource());
    operatorMetrics.receivedEvent(event, metadata);
    Map<String, Metric> metrics = operatorMetrics.metricRegistry().getMetrics();
    assertEquals(2, metrics.size());
    assertTrue(metrics.containsKey("sparkapplication.added.resource.event"));
    assertTrue(metrics.containsKey("sparkapplication.testns.added.resource.event"));
  }

  private static final class TestingExecutionBase<T> implements Metrics.ControllerExecution<T> {
    @Override
    public String controllerName() {
      return controllerName;
    }

    @Override
    public String successTypeName(Object o) {
      return "both";
    }

    @Override
    public ResourceID resourceID() {
      return resourceId;
    }

    @Override
    public Map<String, Object> metadata() {
      return metadata;
    }

    @Override
    public String name() {
      return "reconcile";
    }

    @Override
    public T execute() throws Exception {
      Thread.sleep(1000);
      return null;
    }
  }

  private static final class FooTestingExecutionBase<T> implements Metrics.ControllerExecution<T> {
    @Override
    public String controllerName() {
      return controllerName;
    }

    @Override
    public String successTypeName(Object o) {
      return "resource";
    }

    @Override
    public ResourceID resourceID() {
      return resourceId;
    }

    @Override
    public Map<String, Object> metadata() {
      return metadata;
    }

    @Override
    public String name() {
      return "reconcile";
    }

    @Override
    public T execute() throws Exception {
      throw new NoSuchFieldException("Foo exception");
    }
  }

  private HasMetadata buildNamespacedResource() {
    ConfigMap cm = new ConfigMap();
    cm.setMetadata(
        new ObjectMetaBuilder()
            .withName(TEST_RESOURCE_NAME)
            .withNamespace(DEFAULT_NAMESPACE)
            .build());
    return cm;
  }
}
