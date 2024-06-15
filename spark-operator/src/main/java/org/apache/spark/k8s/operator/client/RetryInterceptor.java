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

package org.apache.spark.k8s.operator.client;

import static io.fabric8.kubernetes.client.utils.Utils.closeQuietly;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.apache.commons.lang3.StringUtils;

/** Intercepts HTTP requests and add custom retry on 429 and 5xx to overcome server instability */
@Slf4j
@RequiredArgsConstructor
@Builder
public class RetryInterceptor implements Interceptor {
  private static final String RETRY_AFTER_HEADER_NAME = "Retry-After";
  public static final int HTTP_TOO_MANY_REQUESTS = 429;
  public static final int INTERNAL_SERVER_ERROR = 500;

  private final Long maxAttemptCount;
  private final Long maxRetryAfterInSecs;
  private final Long defaultRetryAfterInSecs;

  @Override
  public Response intercept(Chain chain) throws IOException {
    Request request = chain.request();
    Response response = chain.proceed(request);
    int tryCount = 0;
    while (!response.isSuccessful()
        && (response.code() == HTTP_TOO_MANY_REQUESTS || response.code() >= INTERNAL_SERVER_ERROR)
        && tryCount < maxAttemptCount) {
      // only retry on consecutive 429 and 5xx failure responses
      if (log.isWarnEnabled()) {
        log.warn(
            "Request is not successful. attempt={} response-code={} response-headers={}",
            tryCount,
            response.code(),
            response.headers());
      }
      Optional<Long> retryAfter = getRetryAfter(response);
      if (retryAfter.isPresent()) {
        try {
          TimeUnit.SECONDS.sleep(retryAfter.get());
        } catch (InterruptedException e) {
          if (log.isErrorEnabled()) {
            log.error("Aborting retry.", e);
          }
        }
      }
      tryCount++;

      ResponseBody responseBody = response.body();
      if (responseBody != null) {
        closeQuietly(responseBody);
      }
      // retry the request for 429 and 5xx
      response = chain.proceed(request);
    }
    return response;
  }

  private Optional<Long> getRetryAfter(Response response) {
    String retryAfter = response.header(RETRY_AFTER_HEADER_NAME);
    if (StringUtils.isNotEmpty(retryAfter)) {
      try {
        return Optional.of(Math.min(Long.parseLong(retryAfter), maxRetryAfterInSecs));
      } catch (NumberFormatException e) {
        if (log.isErrorEnabled()) {
          log.error(
              String.format(
                  "Error while parsing Retry-After header %s. Retrying with default %s",
                  retryAfter, defaultRetryAfterInSecs),
              e);
        }
        return Optional.of(defaultRetryAfterInSecs);
      }
    }
    return Optional.empty();
  }
}
