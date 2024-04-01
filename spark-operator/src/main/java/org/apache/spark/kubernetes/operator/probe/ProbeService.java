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

package org.apache.spark.kubernetes.operator.probe;

import com.sun.net.httpserver.HttpServer;
import io.javaoperatorsdk.operator.Operator;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.kubernetes.operator.health.SentinelManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import static org.apache.spark.kubernetes.operator.config.SparkOperatorConf.OperatorProbePort;

@Slf4j
public class ProbeService {
    public static final String HEALTHZ = "/healthz";
    public static final String STARTUP = "/startup";
    HttpServer server;

    public ProbeService(List<Operator> operators, SentinelManager sentinelManager) {
        HealthProbe healthProbe = new HealthProbe(operators);
        healthProbe.registerSentinelResourceManager(sentinelManager);
        try {
            server = HttpServer.create(new InetSocketAddress(OperatorProbePort.getValue()), 0);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create Probe Service Server", e);
        }
        server.createContext(STARTUP, new ReadinessProbe(operators));
        server.createContext(HEALTHZ, healthProbe);
        server.setExecutor(null);
    }

    public void start() {
        log.info("Probe service started");
        server.start();
    }

    public void stop() {
        log.info("Probe service stopped");
        server.stop(0);
    }
}
