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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import com.sun.net.httpserver.HttpExchange;
import io.javaoperatorsdk.operator.Operator;
import io.javaoperatorsdk.operator.RuntimeInfo;
import lombok.extern.slf4j.Slf4j;

/** A utility class to provide common functionalities for probe services. */
@Slf4j
public final class ProbeUtil {

  private ProbeUtil() {}

  /**
   * Send an HTTP response message with the given response header HTTP status code and message.
   *
   * @param httpExchange The handler for this HTTP response.
   * @param code A response header HTTP status code defined in java.net.HttpURLConnection.HTTP_*
   * @param message A message to send as a body
   * @throws IOException Failed to send a response.
   */
  public static void sendMessage(HttpExchange httpExchange, int code, String message)
      throws IOException {
    try (OutputStream outputStream = httpExchange.getResponseBody()) {
      byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
      httpExchange.sendResponseHeaders(code, bytes.length);
      outputStream.write(bytes);
      outputStream.flush();
    }
  }

  public static Optional<Boolean> areOperatorsStarted(List<Operator> operators) {
    return operators.stream()
        .map(
            operator -> {
              RuntimeInfo runtimeInfo = operator.getRuntimeInfo();
              if (runtimeInfo != null) {
                if (!operator.getRuntimeInfo().isStarted()) {
                  log.error("Operator is not running");
                  return false;
                }
                return true;
              }
              return false;
            })
        .reduce((a, b) -> a && b);
  }
}
