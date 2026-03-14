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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.status.ApplicationAttemptInfo;
import org.apache.spark.k8s.operator.status.ApplicationAttemptSummary;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.status.ClusterStatus;

class LoggingUtilsTest {

  private static final Logger LOG = LogManager.getLogger(LoggingUtilsTest.class);

  @AfterEach
  void cleanup() {
    MDC.clear();
  }

  @Test
  void testSparkApplicationLogContainsMDC() throws InterruptedException {
    TestListAppender appender = installAppender(1);
    try {
      SparkApplication app = createSparkApp("spark-pi", "default", 3);
      LoggingUtils.TrackedMDC trackedMDC = new LoggingUtils.TrackedMDC();
      trackedMDC.set(app);
      LOG.warn("Start app reconciliation.");
      trackedMDC.reset();

      assertTrue(
          appender.latch.await(5, TimeUnit.SECONDS), "Timed out waiting for log event");

      LogEvent event = appender.events.get(0);
      assertEquals(
          "spark-pi",
          event.getContextData().getValue(LoggingUtils.TrackedMDC.ResourceNameKey));
      assertEquals(
          "default",
          event.getContextData().getValue(LoggingUtils.TrackedMDC.ResourceNamespaceKey));
      assertEquals(
          "3", event.getContextData().getValue(LoggingUtils.TrackedMDC.AppAttemptIdKey));

      String formatted = appender.formattedMessages.get(0);
      assertTrue(formatted.contains("resource.name=spark-pi"), formatted);
      assertTrue(formatted.contains("resource.namespace=default"), formatted);
      assertTrue(formatted.contains("resource.app.attemptId=3"), formatted);
    } finally {
      removeAppender(appender);
    }
  }

  @Test
  void testSparkClusterLogContainsMDC() throws InterruptedException {
    TestListAppender appender = installAppender(1);
    try {
      SparkCluster cluster = createSparkCluster("my-cluster", "spark-ns");
      LoggingUtils.TrackedMDC trackedMDC = new LoggingUtils.TrackedMDC();
      trackedMDC.set(cluster);
      LOG.warn("Start cluster reconciliation.");
      trackedMDC.reset();

      assertTrue(
          appender.latch.await(5, TimeUnit.SECONDS), "Timed out waiting for log event");

      LogEvent event = appender.events.get(0);
      assertEquals(
          "my-cluster",
          event.getContextData().getValue(LoggingUtils.TrackedMDC.ResourceNameKey));
      assertEquals(
          "spark-ns",
          event.getContextData().getValue(LoggingUtils.TrackedMDC.ResourceNamespaceKey));
      assertEquals(
          "0", event.getContextData().getValue(LoggingUtils.TrackedMDC.AppAttemptIdKey));

      String formatted = appender.formattedMessages.get(0);
      assertTrue(formatted.contains("resource.name=my-cluster"), formatted);
      assertTrue(formatted.contains("resource.namespace=spark-ns"), formatted);
    } finally {
      removeAppender(appender);
    }
  }

  @Test
  void testResetClearsMDCFromLogOutput() throws InterruptedException {
    TestListAppender appender = installAppender(1);
    try {
      SparkApplication app = createSparkApp("spark-pi", "default", 3);
      LoggingUtils.TrackedMDC trackedMDC = new LoggingUtils.TrackedMDC();
      trackedMDC.set(app);
      trackedMDC.reset();
      LOG.warn("After reset.");

      assertTrue(
          appender.latch.await(5, TimeUnit.SECONDS), "Timed out waiting for log event");

      LogEvent event = appender.events.get(0);
      assertNull(
          event.getContextData().getValue(LoggingUtils.TrackedMDC.ResourceNameKey));
      assertNull(
          event.getContextData().getValue(LoggingUtils.TrackedMDC.ResourceNamespaceKey));
      assertNull(
          event.getContextData().getValue(LoggingUtils.TrackedMDC.AppAttemptIdKey));

      String formatted = appender.formattedMessages.get(0);
      assertTrue(
          formatted.contains("{}"), "Expected empty MDC in log output, got: " + formatted);
    } finally {
      removeAppender(appender);
    }
  }

  @Test
  void testSetWithNullDoesNotSetMDC() throws InterruptedException {
    TestListAppender appender = installAppender(1);
    try {
      LoggingUtils.TrackedMDC trackedMDC = new LoggingUtils.TrackedMDC();
      trackedMDC.set(null);
      LOG.warn("Null resource.");

      assertTrue(
          appender.latch.await(5, TimeUnit.SECONDS), "Timed out waiting for log event");

      LogEvent event = appender.events.get(0);
      assertNull(
          event.getContextData().getValue(LoggingUtils.TrackedMDC.ResourceNameKey));
      assertNull(
          event.getContextData().getValue(LoggingUtils.TrackedMDC.ResourceNamespaceKey));
      assertNull(
          event.getContextData().getValue(LoggingUtils.TrackedMDC.AppAttemptIdKey));
    } finally {
      removeAppender(appender);
    }
  }

  private static TestListAppender installAppender(int expectedEvents) {
    PatternLayout layout =
        PatternLayout.newBuilder().withPattern("%p %X %m%n").build();
    TestListAppender appender =
        new TestListAppender("TestListAppender", layout, expectedEvents);
    appender.start();
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    ctx.getConfiguration().getRootLogger().addAppender(appender, Level.ALL, null);
    ctx.updateLoggers();
    return appender;
  }

  private static void removeAppender(TestListAppender appender) {
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    ctx.getConfiguration().getRootLogger().removeAppender(appender.getName());
    ctx.updateLoggers();
    appender.stop();
  }

  private static SparkApplication createSparkApp(
      String name, String namespace, long attemptId) {
    SparkApplication app = new SparkApplication();
    ObjectMeta meta = new ObjectMeta();
    meta.setName(name);
    meta.setNamespace(namespace);
    app.setMetadata(meta);
    ApplicationAttemptInfo attemptInfo = new ApplicationAttemptInfo(attemptId, 0, 0, 0);
    ApplicationAttemptSummary summary = new ApplicationAttemptSummary(attemptInfo);
    ApplicationStatus status =
        new ApplicationStatus(
            new ApplicationState(),
            Map.of(0L, new ApplicationState()),
            null,
            summary);
    app.setStatus(status);
    return app;
  }

  private static SparkCluster createSparkCluster(String name, String namespace) {
    SparkCluster cluster = new SparkCluster();
    ObjectMeta meta = new ObjectMeta();
    meta.setName(name);
    meta.setNamespace(namespace);
    cluster.setMetadata(meta);
    cluster.setStatus(new ClusterStatus());
    return cluster;
  }

  static class TestListAppender extends AbstractAppender {
    final List<LogEvent> events = new CopyOnWriteArrayList<>();
    final List<String> formattedMessages = new CopyOnWriteArrayList<>();
    final CountDownLatch latch;

    TestListAppender(String name, PatternLayout layout, int expectedEvents) {
      super(name, null, layout, true, Property.EMPTY_ARRAY);
      this.latch = new CountDownLatch(expectedEvents);
    }

    @Override
    public void append(LogEvent event) {
      events.add(event.toImmutable());
      formattedMessages.add(
          new String(getLayout().toByteArray(event), StandardCharsets.UTF_8));
      latch.countDown();
    }
  }
}
