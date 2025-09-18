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

package org.apache.spark.k8s.operator.metrics;

import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.metrics.source.OperatorJvmSource;
import org.apache.spark.metrics.sink.Sink;
import org.apache.spark.metrics.source.Source;

/** Manages the metrics system for the Spark Operator. */
@Slf4j
public class MetricsSystem {
  private final AtomicBoolean running = new AtomicBoolean(false);
  @Getter private final Set<Sink> sinks;
  @Getter private final Set<Source> sources;
  @Getter private final MetricRegistry registry;
  @Getter private final Properties properties;
  // PrometheusPullModelHandler is registered by default, metrics exposed via http port
  @Getter private final PrometheusPullModelHandler prometheusPullModelHandler;
  private final Map<String, SinkProperties> sinkPropertiesMap;

  /** Constructs a new MetricsSystem with default properties. */
  public MetricsSystem() {
    this(new Properties());
  }

  /**
   * Constructs a new MetricsSystem with the given properties.
   *
   * @param properties The properties to configure the metrics system.
   */
  public MetricsSystem(Properties properties) {
    this.sources = new HashSet<>();
    this.sinks = new HashSet<>();
    this.registry = new MetricRegistry();
    this.properties = properties;
    this.sinkPropertiesMap = MetricsSystemFactory.parseSinkProperties(this.properties);
    // Add default sinks
    this.prometheusPullModelHandler = new PrometheusPullModelHandler(new Properties(), registry);
    this.sinks.add(prometheusPullModelHandler);
  }

  /**
   * Starts the metrics system, registering default sources and configured sinks. Throws
   * IllegalStateException if the system is already running.
   */
  public void start() {
    if (running.get()) {
      throw new IllegalStateException(
          "Attempting to start a MetricsSystem that is already running");
    }
    running.set(true);
    registerDefaultSources();
    registerSinks();
    sinks.forEach(Sink::start);
  }

  /** Stops the metrics system, stopping all sinks and clearing the registry. */
  public void stop() {
    if (running.get()) {
      sinks.forEach(Sink::stop);
      registry.removeMatching(MetricFilter.ALL);
    } else {
      log.error("Stopping a MetricsSystem that is not running");
    }
    running.set(false);
  }

  /** Triggers all registered sinks to report their metrics. */
  public void report() {
    sinks.forEach(Sink::report);
  }

  /** Registers default metric sources, such as JVM metrics. */
  protected void registerDefaultSources() {
    registerSource(new OperatorJvmSource());
  }

  /**
   * Registers all configured metrics sinks based on the provided properties. Sinks are instantiated
   * via reflection.
   */
  protected void registerSinks() {
    log.info("sinkPropertiesMap: {}", sinkPropertiesMap);
    sinkPropertiesMap
        .values()
        .forEach(
            sinkProp -> {
              try {
                Class<Sink> sinkClass = (Class<Sink>) Class.forName(sinkProp.getClassName());
                Sink sinkInstance;
                sinkInstance =
                    sinkClass
                        .getConstructor(Properties.class, MetricRegistry.class)
                        .newInstance(sinkProp.getProperties(), registry);
                sinks.add(sinkInstance);
              } catch (InstantiationException
                  | IllegalAccessException
                  | IllegalArgumentException
                  | InvocationTargetException
                  | NoSuchMethodException
                  | SecurityException
                  | ClassNotFoundException e) {
                if (log.isErrorEnabled()) {
                  log.error(
                      "Fail to create metrics sink for sink name {}, sink properties {}",
                      sinkProp.getClassName(),
                      sinkProp.getProperties());
                }
                throw new IllegalStateException("Fail to create metrics sink", e);
              }
            });
  }

  /**
   * Registers a custom metric source with the metrics system.
   *
   * @param source The Source to register.
   */
  public void registerSource(Source source) {
    sources.add(source);
    try {
      String regName = MetricRegistry.name(source.sourceName());
      registry.register(regName, source.metricRegistry());
    } catch (IllegalArgumentException e) {
      log.error("Metrics already registered", e);
    }
  }

  @Data
  public static class SinkProperties {
    String className;
    Properties properties;

    /** Constructs a new, empty SinkProperties object. */
    public SinkProperties() {
      this.className = "";
      this.properties = new Properties();
    }
  }
}
