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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import lombok.extern.slf4j.Slf4j;

/**
 * Periodically reloads dynamic configuration overrides from a properties file on disk. The file is
 * expected to be populated by mounting a ConfigMap as a volume into the operator pod, so changes
 * applied to the ConfigMap propagate to disk without requiring a Kubernetes informer. When the
 * file contents change, {@link SparkOperatorConfManager} is refreshed and the caller-supplied
 * namespace updater is invoked.
 */
@Slf4j
public class DynamicConfigMonitor {

  private final Path configFile;
  private final Duration reloadInterval;
  private final Supplier<Set<String>> watchedNamespacesSupplier;
  private final Consumer<Set<String>> namespaceUpdater;
  private final ScheduledExecutorService scheduler;
  private final boolean ownsScheduler;

  private final AtomicReference<Map<String, String>> lastLoaded = new AtomicReference<>(Map.of());
  private final AtomicReference<ScheduledFuture<?>> scheduledTask = new AtomicReference<>();

  public DynamicConfigMonitor(
      Path configFile,
      Duration reloadInterval,
      Supplier<Set<String>> watchedNamespacesSupplier,
      Consumer<Set<String>> namespaceUpdater) {
    this(configFile, reloadInterval, watchedNamespacesSupplier, namespaceUpdater, null);
  }

  DynamicConfigMonitor(
      Path configFile,
      Duration reloadInterval,
      Supplier<Set<String>> watchedNamespacesSupplier,
      Consumer<Set<String>> namespaceUpdater,
      ScheduledExecutorService scheduler) {
    this.configFile = configFile;
    this.reloadInterval = reloadInterval;
    this.watchedNamespacesSupplier = watchedNamespacesSupplier;
    this.namespaceUpdater = namespaceUpdater;
    if (scheduler == null) {
      this.scheduler =
          Executors.newSingleThreadScheduledExecutor(
              r -> {
                Thread t = new Thread(r, "spark-operator-dynamic-config");
                t.setDaemon(true);
                return t;
              });
      this.ownsScheduler = true;
    } else {
      this.scheduler = scheduler;
      this.ownsScheduler = false;
    }
  }

  /**
   * Schedules periodic reloads at the configured interval. The first reload runs through the
   * scheduler with no initial delay (rather than synchronously here), so a failing initial load
   * never blocks operator startup.
   */
  public void start() {
    log.info(
        "Starting dynamic config monitor on {} with reload interval {}",
        configFile,
        reloadInterval);
    long millis = reloadInterval.toMillis();
    scheduledTask.set(
        scheduler.scheduleAtFixedRate(this::reloadSafely, 0, millis, TimeUnit.MILLISECONDS));
  }

  /** Stops the scheduler if it was created internally. */
  public void stop() {
    log.info("Stopping dynamic config monitor");
    if (ownsScheduler) {
      scheduler.shutdownNow();
    }
  }

  /**
   * Returns true once {@link #start()} has scheduled the periodic reload and the underlying
   * scheduler is still running. Because {@link #reloadSafely()} swallows reload failures, the
   * scheduled task only becomes done when it is cancelled (e.g. via {@link #stop()}).
   */
  public boolean isRunning() {
    ScheduledFuture<?> task = scheduledTask.get();
    return task != null && !task.isDone() && !scheduler.isShutdown();
  }

  private void reloadSafely() {
    try {
      reload();
    } catch (RuntimeException e) {
      log.error("Failed to reload dynamic config from {}", configFile, e);
    }
  }

  private void reload() {
    Map<String, String> current = readProperties();
    if (current.equals(lastLoaded.get())) {
      return;
    }
    log.info(
        "Detected dynamic config change in {}, applying {} overrides", configFile, current.size());
    SparkOperatorConfManager.INSTANCE.refresh(current);
    lastLoaded.set(current);
    namespaceUpdater.accept(watchedNamespacesSupplier.get());
  }

  private Map<String, String> readProperties() {
    if (!Files.isRegularFile(configFile)) {
      return Map.of();
    }
    Properties properties = new Properties();
    try (InputStream in = Files.newInputStream(configFile)) {
      properties.load(in);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read dynamic config file " + configFile, e);
    }
    Map<String, String> result = new HashMap<>(properties.size());
    properties.forEach((k, v) -> result.put(String.valueOf(k), String.valueOf(v)));
    return Map.copyOf(result);
  }
}
