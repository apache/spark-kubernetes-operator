/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.spark.kubernetes.operator.metrics.sink;

import com.codahale.metrics.MetricRegistry;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.metrics.sink.PrometheusServlet;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.spark.kubernetes.operator.utils.ProbeUtil.sendMessage;

@Slf4j
public class PrometheusPullModelSink extends PrometheusServlet implements HttpHandler {
    public PrometheusPullModelSink(Properties properties, MetricRegistry registry) {
        super(properties, registry);
    }

    @Override
    public void start() {
        log.info("PrometheusPullModelSink started");
    }

    @Override
    public void stop() {
        log.info("PrometheusPullModelSink stopped");
    }

    @Override
    public void report() {
        //no-op
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        // https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/metrics/sink/PrometheusServlet.scala#L50
        // Temporary solution since PrometheusServlet.getMetricsSnapshot method does not use
        // httpServletRequest at all
        HttpServletRequest httpServletRequest = null;
        String value = getMetricsSnapshot(httpServletRequest);
        // Prometheus will have invalid syntax exception while parsing value equal to "[]", e.g:
        // metrics_jvm_threadStates_deadlocks_Number{type="gauges"} []
        // metrics_jvm_threadStates_deadlocks_Value{type="gauges"} []
        String[] records = value.split("\n");
        List<String> filteredRecords = new ArrayList<>();
        for (String record : records) {
            String[] keyValuePair = record.split(" ");
            if ("[]".equals(keyValuePair[1])) {
                log.info("Bug identified strconv.ParseFloat: parsing []");
                continue;
            }
            filteredRecords.add(record);
        }
        sendMessage(exchange, 200, String.join("\n", filteredRecords));
    }
}
