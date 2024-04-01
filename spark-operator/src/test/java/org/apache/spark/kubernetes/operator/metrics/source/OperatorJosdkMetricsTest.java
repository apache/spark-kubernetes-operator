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

package org.apache.spark.kubernetes.operator.metrics.source;

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
import org.apache.spark.kubernetes.operator.SparkApplication;
import org.apache.spark.kubernetes.operator.reconciler.SparkApplicationReconciler;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

class OperatorJosdkMetricsTest {
    public static final String DEFAULT_NAMESPACE = "default";
    public static final String TEST_RESOURCE_NAME = "test1";
    private static final ResourceID resourceId = new ResourceID("spark-pi", "testns");

    private static final Map<String, Object> metadata =
            Map.of(Constants.RESOURCE_GVK_KEY, GroupVersionKind.gvkFor(SparkApplication.class),
                    Constants.CONTROLLER_NAME, "test-controller-name");
    private static final String controllerName = SparkApplicationReconciler.class.getSimpleName();

    private OperatorJosdkMetrics operatorMetrics;

    @BeforeEach
    public void setup() {
        operatorMetrics =
                new OperatorJosdkMetrics();
    }

    @Test
    void testTimeControllerExecution() throws Exception {
        var successExecution = new TestingExecutionBase<>();
        operatorMetrics.timeControllerExecution(successExecution);
        Map<String, Metric> metrics = operatorMetrics.metricRegistry().getMetrics();
        Assertions.assertEquals(4, metrics.size());
        Assertions.assertTrue(
                metrics.containsKey("sparkapplication.sparkapplicationreconciler.reconcile.both"));
        Assertions.assertTrue(metrics.containsKey(
                "sparkapplication.testns.sparkapplicationreconciler.reconcile.both"));
        Assertions.assertTrue(metrics.containsKey(
                "sparkapplication.sparkapplicationreconciler.reconcile.success.both"));
        Assertions.assertTrue(metrics.containsKey(
                "sparkapplication.testns.sparkapplicationreconciler.reconcile.success.both"));

        var failedExecution = new FooTestingExecutionBase<>();
        try {
            operatorMetrics.timeControllerExecution(failedExecution);
        } catch (Exception e) {
            Assertions.assertEquals(e.getMessage(), "Foo exception");
            Assertions.assertEquals(8, metrics.size());
            Assertions.assertTrue(metrics.containsKey(
                    "sparkapplication.sparkapplicationreconciler.reconcile.failure"));
            Assertions.assertTrue(metrics.containsKey(
                    "sparkapplication.sparkapplicationreconciler.reconcile.failure.exception" +
                            ".nosuchfieldexception"));
            Assertions.assertTrue(metrics.containsKey(
                    "sparkapplication.testns.sparkapplicationreconciler.reconcile.failure"));
            Assertions.assertTrue(metrics.containsKey(
                    "sparkapplication.testns.sparkapplicationreconciler.reconcile.failure." +
                            "exception.nosuchfieldexception"));
        }
    }

    @Test
    void testReconciliationFinished() {
        operatorMetrics.finishedReconciliation(buildNamespacedResource(), metadata);
        Map<String, Metric> metrics = operatorMetrics.metricRegistry().getMetrics();
        Assertions.assertEquals(2, metrics.size());
        Assertions.assertTrue(metrics.containsKey("configmap.default.reconciliation.finished"));
        Assertions.assertTrue(metrics.containsKey("configmap.reconciliation.finished"));
    }

    @Test
    void testReconciliationExecutionStartedAndFinished() {
        operatorMetrics.reconciliationExecutionStarted(buildNamespacedResource(), metadata);
        Map<String, Metric> metrics = operatorMetrics.metricRegistry().getMetrics();
        Assertions.assertEquals(2, metrics.size());
        Assertions.assertTrue(
                metrics.containsKey("configmap.test-controller-name.reconciliations.executions"));
        Assertions.assertTrue(metrics.containsKey(
                "configmap.default.test-controller-name.reconciliations.executions"));
        operatorMetrics.reconciliationExecutionFinished(buildNamespacedResource(), metadata);
        Assertions.assertEquals(3, metrics.size());
        Assertions.assertTrue(
                metrics.containsKey("configmap.test-controller-name.reconciliations.queue.size"));
    }

    @Test
    void testReceivedEvent() {
        Event event =
                new ResourceEvent(ResourceAction.ADDED, resourceId, buildNamespacedResource());
        operatorMetrics.receivedEvent(event, metadata);
        Map<String, Metric> metrics = operatorMetrics.metricRegistry().getMetrics();
        Assertions.assertEquals(2, metrics.size());
        Assertions.assertTrue(metrics.containsKey("sparkapplication.added.resource.event"));
        Assertions.assertTrue(metrics.containsKey("sparkapplication.testns.added.resource.event"));
    }

    private static class TestingExecutionBase<T> implements Metrics.ControllerExecution<T> {
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

    private static class FooTestingExecutionBase<T> implements Metrics.ControllerExecution<T> {
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
        var cm = new ConfigMap();
        cm.setMetadata(new ObjectMetaBuilder()
                .withName(TEST_RESOURCE_NAME)
                .withNamespace(DEFAULT_NAMESPACE)
                .build());
        return cm;
    }
}
