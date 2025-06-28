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

package org.apache.spark.k8s.operator.probe;

import static java.net.HttpURLConnection.*;
import static org.apache.spark.k8s.operator.utils.ProbeUtil.areOperatorsStarted;
import static org.apache.spark.k8s.operator.utils.ProbeUtil.sendMessage;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.javaoperatorsdk.operator.Operator;
import lombok.extern.slf4j.Slf4j;

/** Readiness probe for the operator. */
@Slf4j
public class ReadinessProbe implements HttpHandler {
  private final List<Operator> operators;

  public ReadinessProbe(List<Operator> operators) {
    this.operators = operators;
  }

  @Override
  public void handle(HttpExchange httpExchange) throws IOException {
    Optional<Boolean> operatorsAreReady = areOperatorsStarted(operators);
    if (operatorsAreReady.isEmpty() || !operatorsAreReady.get()) {
      sendMessage(httpExchange, HTTP_BAD_REQUEST, "spark operators are not ready yet");
    }

    if (!passRbacCheck()) {
      sendMessage(
          httpExchange, HTTP_FORBIDDEN, "required rbac test failed, operators are not ready");
    }

    sendMessage(httpExchange, HTTP_OK, "started");
  }

  public boolean passRbacCheck() {
    return true;
  }
}
