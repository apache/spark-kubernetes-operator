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

package org.apache.spark.kubernetes.operator.metrics;

import com.codahale.metrics.MetricFilter;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.kubernetes.operator.metrics.source.JVMSource;
import org.apache.spark.metrics.sink.Sink;
import org.apache.spark.metrics.source.Source;
import com.codahale.metrics.MetricRegistry;
import org.apache.spark.util.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class MetricsSystem {
    private AtomicBoolean running = new AtomicBoolean(false);
    @Getter
    private List<Sink> sinks;
    @Getter
    private List<Source> sources;
    @Getter
    private MetricRegistry registry;
    private Properties properties;
    private Map<String, SinkProps> sinkPropertiesMap;

    public MetricsSystem() {
        this.sinks = new ArrayList<>();
        this.sources = new ArrayList<>();
        this.registry = new MetricRegistry();
        this.properties = new Properties();
        this.sinkPropertiesMap = new HashMap<>();
    }

    public MetricsSystem(Properties properties) {
        this.sinks = new ArrayList<>();
        this.sources = new ArrayList<>();
        this.registry = new MetricRegistry();
        this.properties = properties;
        this.sinkPropertiesMap = MetricsSystemFactory.parseSinkProperties(this.properties);
    }

    public void start() {
        if (running.get()) {
            throw new IllegalStateException(
                    "Attempting to start a MetricsSystem that is already running");
        }
        running.set(true);
        registerSources();
        registerSinks();
        sinks.forEach(Sink::start);
    }

    public void stop() {
        if (running.get()) {
            sinks.forEach(Sink::stop);
            registry.removeMatching(MetricFilter.ALL);
        } else {
            log.error("Stopping a MetricsSystem that is not running");
        }
        running.set(false);
    }

    public void report() {
        sinks.forEach(Sink::report);
    }

    public void registerSinks() {
        log.info("sinkPropertiesMap: {}", sinkPropertiesMap);
        sinkPropertiesMap.values().forEach(sinkProp -> {
            Class<Sink> sink = Utils.classForName(sinkProp.getClassName(), true, false);
            Sink sinkInstance;
            try {
                sinkInstance = sink.getConstructor(Properties.class, MetricRegistry.class)
                        .newInstance(sinkProp.getProperties(), registry);
            } catch (Exception e) {
                if (log.isErrorEnabled()) {
                    log.error("Fail to create metrics sink for sink name {}, sink properties {}",
                            sinkProp.getClassName(), sinkProp.getProperties());
                }
                throw new RuntimeException("Fail to create metrics sink", e);
            }
            sinks.add(sinkInstance);
        });
    }

    private void registerSources() {
        // TO-DO parse the properties to config sources
        registerSource(new JVMSource());
    }

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
    public static class SinkProps {
        String className;
        Properties properties;

        public SinkProps() {
            this.className = "";
            this.properties = new Properties();
        }
    }
}
