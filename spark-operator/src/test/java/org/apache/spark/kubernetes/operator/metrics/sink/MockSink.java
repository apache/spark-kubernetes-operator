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

package org.apache.spark.kubernetes.operator.metrics.sink;

import org.apache.spark.metrics.sink.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;

@SuppressWarnings("PMD")
public class MockSink implements Sink {
    private static final Logger logger = LoggerFactory.getLogger(MockSink.class);
    private Properties properties;
    private MetricRegistry metricRegistry;
    public static final String DEFAULT_UNIT = "SECONDS";
    public static final int DEFAULT_PERIOD = 20;
    public static final String KEY_PERIOD = "period";
    public static final String KEY_UNIT = "unit";

    public int getPollPeriod() {
        return Integer.parseInt((String) properties.getOrDefault(KEY_PERIOD, DEFAULT_PERIOD));
    }

    public TimeUnit getTimeUnit() {
        return TimeUnit.valueOf((String) properties.getOrDefault(KEY_UNIT, DEFAULT_UNIT));
    }

    public MockSink(Properties properties, MetricRegistry metricRegistry) {
        logger.info("Current properties: {}", properties);
        this.properties = properties;
        this.metricRegistry = metricRegistry;
    }

    @Override
    public void start() {
        logger.info("Mock sink started");
    }

    @Override
    public void stop() {
        logger.info("Mock sink stopped");
    }

    @Override
    public void report() {
        logger.info("Mock sink reported");
    }
}
