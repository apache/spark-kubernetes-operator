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

import static java.net.HttpURLConnection.HTTP_BAD_METHOD;
import static org.apache.spark.k8s.operator.utils.ProbeUtil.sendMessage;

import java.io.IOException;
import java.util.Set;

import com.sun.net.httpserver.Filter;
import com.sun.net.httpserver.HttpExchange;
import lombok.extern.slf4j.Slf4j;

/**
 * A filter that only allows GET and HTTP requests and returns 405 Method Not Allowed for all
 * other HTTP methods.
 */
@Slf4j
public class HttpMethodFilter extends Filter {
  @Override
  public void doFilter(HttpExchange exchange, Chain chain) throws IOException {
    if (!Set.of("GET", "HEAD").contains(exchange.getRequestMethod().toUpperCase())) {
      sendMessage(exchange, HTTP_BAD_METHOD, "Method Not Allowed");
      exchange.close();
      return;
    }
    chain.doFilter(exchange);
  }

  @Override
  public String description() {
    return "Filter that only allows GET requests";
  }
}
