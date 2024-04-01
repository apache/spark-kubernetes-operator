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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.RuntimeInfo;
import io.javaoperatorsdk.operator.health.InformerHealthIndicator;
import io.javaoperatorsdk.operator.health.Status;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.kubernetes.operator.health.SentinelManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.spark.kubernetes.operator.utils.ProbeUtil.areOperatorsStarted;
import static org.apache.spark.kubernetes.operator.utils.ProbeUtil.sendMessage;

@Getter
@Slf4j
public class HealthProbe implements HttpHandler {
    private final List<Operator> operators;
    private final List<SentinelManager<?>> sentinelManagers = new ArrayList<>();

    public HealthProbe(List<Operator> operators) {
        this.operators = operators;
    }

    public void registerSentinelResourceManager(SentinelManager<?> sentinelManager) {
        sentinelManagers.add(sentinelManager);
    }

    public boolean isHealthy() {
        var operatorsAreReady = areOperatorsStarted(operators);
        if (operatorsAreReady.isEmpty() || !operatorsAreReady.get()) {
            return false;
        }

        var runtimeInfosAreHealthy = operators.stream().map(operator ->
                checkInformersHealth(operator.getRuntimeInfo())
        ).reduce((a, b) -> a && b);

        if (runtimeInfosAreHealthy.isEmpty() || !runtimeInfosAreHealthy.get()) {
            return false;
        }

        for (SentinelManager<?> sentinelManager : sentinelManagers) {
            if (!sentinelManager.allSentinelsAreHealthy()) {
                log.error("One sentinel manager {} reported an unhealthy condition.",
                        sentinelManager);
                return false;
            }
        }

        return true;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (isHealthy()) {
            sendMessage(exchange, 200, "healthy");
        } else {
            sendMessage(exchange, 500, "unhealthy");
        }
    }

    private boolean checkInformersHealth(RuntimeInfo operatorRuntimeInfo) {
        log.info("Checking informer health");
        List<Boolean> informersHealthList = new ArrayList<>();
        for (var controllerEntry :
                operatorRuntimeInfo.unhealthyInformerWrappingEventSourceHealthIndicator()
                        .entrySet()) {
            for (var eventSourceEntry : controllerEntry.getValue().entrySet()) {
                Map<String, InformerHealthIndicator> informers =
                        eventSourceEntry.getValue().informerHealthIndicators();
                for (var informerEntry : informers.entrySet()) {
                    if (informerEntry.getValue().getStatus() == Status.HEALTHY) {
                        informersHealthList.add(true);
                    } else {
                        if (log.isErrorEnabled()) {
                            log.error(
                                    "Controller: {}, Event Source: {}, Informer: {} is not in a " +
                                            "healthy state",
                                    controllerEntry.getKey(), eventSourceEntry.getKey(),
                                    informerEntry.getKey());
                        }
                        informersHealthList.add(false);
                    }
                }
            }
        }
        return informersHealthList.stream().reduce((a, b) -> a && b).orElse(true);
    }
}
