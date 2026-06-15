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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DynamicConfigMonitorTest {

  @TempDir Path tempDir;

  private DynamicConfigMonitor monitor;

  @AfterEach
  void tearDown() {
    if (monitor != null) {
      monitor.stop();
    }
    SparkOperatorConfManager.INSTANCE.refresh(Map.of());
  }

  @Test
  void initialLoadAppliesOverridesAndNotifiesNamespaceUpdater() throws IOException {
    String configKey = SparkOperatorConf.RECONCILER_INTERVAL_SECONDS.getKey();
    Path configFile = tempDir.resolve("spark-operator-dynamic.properties");
    Files.writeString(configFile, configKey + "=60\n");

    Set<String> watchedNamespaces = Set.of("ns-a", "ns-b");
    AtomicReference<Set<String>> updaterCalledWith = new AtomicReference<>();

    monitor =
        new DynamicConfigMonitor(
            configFile,
            Duration.ofMillis(50),
            () -> watchedNamespaces,
            ns -> updaterCalledWith.set(new HashSet<>(ns)));
    monitor.start();

    assertTrue(monitor.isRunning());
    assertEquals("60", SparkOperatorConfManager.INSTANCE.getValue(configKey));
    assertEquals(watchedNamespaces, updaterCalledWith.get());
  }

  @Test
  void detectsFileChangeOnNextPoll() throws IOException {
    String configKey = SparkOperatorConf.RECONCILER_INTERVAL_SECONDS.getKey();
    Path configFile = tempDir.resolve("spark-operator-dynamic.properties");
    Files.writeString(configFile, configKey + "=60\n");

    AtomicReference<Set<String>> updaterCalledWith = new AtomicReference<>();
    monitor =
        new DynamicConfigMonitor(
            configFile,
            Duration.ofMillis(50),
            Set::of,
            ns -> updaterCalledWith.set(new HashSet<>(ns)));
    monitor.start();
    assertEquals("60", SparkOperatorConfManager.INSTANCE.getValue(configKey));

    Files.writeString(configFile, configKey + "=120\n");

    await()
        .atMost(Duration.ofSeconds(5))
        .untilAsserted(
            () -> assertEquals("120", SparkOperatorConfManager.INSTANCE.getValue(configKey)));
  }

  @Test
  void handlesMissingFileGracefully() {
    Path configFile = tempDir.resolve("does-not-exist.properties");
    AtomicReference<Set<String>> updaterCalledWith = new AtomicReference<>();

    monitor =
        new DynamicConfigMonitor(
            configFile,
            Duration.ofMillis(50),
            Set::of,
            ns -> updaterCalledWith.set(new HashSet<>(ns)));
    monitor.start();

    assertTrue(monitor.isRunning());
    assertFalse(SparkOperatorConfManager.INSTANCE.getAll().containsKey("missing"));
  }

  @Test
  void noOpWhenFileUnchanged() throws IOException {
    String configKey = SparkOperatorConf.RECONCILER_INTERVAL_SECONDS.getKey();
    Path configFile = tempDir.resolve("spark-operator-dynamic.properties");
    Files.writeString(configFile, configKey + "=60\n");

    AtomicReference<Integer> updaterCallCount = new AtomicReference<>(0);
    monitor =
        new DynamicConfigMonitor(
            configFile,
            Duration.ofMillis(50),
            Set::of,
            ns -> updaterCallCount.updateAndGet(c -> c + 1));
    monitor.start();
    int countAfterStart = updaterCallCount.get();
    assertEquals(1, countAfterStart, "namespace updater invoked once on initial load");

    await().pollDelay(Duration.ofMillis(300)).until(() -> true);
    assertEquals(
        countAfterStart, updaterCallCount.get(), "namespace updater not re-invoked without change");
  }
}
