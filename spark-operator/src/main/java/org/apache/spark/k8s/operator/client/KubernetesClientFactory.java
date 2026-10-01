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

package org.apache.spark.k8s.operator.client;

import java.util.List;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.RequestConfig;
import io.fabric8.kubernetes.client.RequestConfigBuilder;
import io.fabric8.kubernetes.client.http.HttpClient;
import io.fabric8.kubernetes.client.http.Interceptor;
import io.fabric8.kubernetes.client.utils.HttpClientUtils;
import io.fabric8.kubernetes.client.vertx.VertxHttpClientBuilder;
import io.fabric8.kubernetes.client.vertx.VertxHttpClientFactory;

/** Factory for building Kubernetes clients with metrics configured. */
public final class KubernetesClientFactory {

  private KubernetesClientFactory() {}

  /**
   * Builds a KubernetesClient with the given interceptors.
   *
   * @param interceptors A list of interceptors to add to the client.
   * @return A new KubernetesClient instance.
   */
  public static KubernetesClient buildKubernetesClient(final List<Interceptor> interceptors) {
    return buildKubernetesClient(interceptors, null);
  }

  /**
   * Builds a KubernetesClient with the given interceptors and configuration.
   *
   * @param interceptors A list of interceptors to add to the client.
   * @param kubernetesClientConfig The Kubernetes client configuration.
   * @return A new KubernetesClient instance.
   */
  public static KubernetesClient buildKubernetesClient(
      final List<Interceptor> interceptors, final Config kubernetesClientConfig) {
    return new KubernetesClientBuilder()
        .withConfig(kubernetesClientConfig)
        .withHttpClientFactory(
            new VertxHttpClientFactory() {
              @Override
              public HttpClient.Builder newBuilder(Config config) {
                VertxHttpClientBuilder builder = super.newBuilder();
                HttpClientUtils.applyCommonConfiguration(config, builder, this);
                for (Interceptor interceptor : interceptors) {
                  builder.addOrReplaceInterceptor(interceptor.getClass().getName(), interceptor);
                }
                return builder;
              }
            })
        .build();
  }

  /**
   * Returns a client over the same connections, with the same interceptors and request timeout,
   * which does not retry a failed request. The given client retries a throttled request, a server
   * error and a connection failure with backoff, which holds the caller meanwhile. Since fabric8
   * calls {@link Interceptor#afterConnectionFailure} only while a request has a retry left, the
   * interceptors do not see a request of the returned client which gets no response, so the client
   * metrics do not count it as failed.
   *
   * @param client The client to derive from.
   * @return A client which does not retry a failed request.
   */
  public static KubernetesClient withoutRetries(final KubernetesClient client) {
    RequestConfig requestConfig =
        new RequestConfigBuilder(client.getConfiguration().getRequestConfig())
            .withRequestRetryBackoffLimit(0)
            .build();
    return client.newClient(requestConfig).adapt(KubernetesClient.class);
  }
}
