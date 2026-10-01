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

import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.apache.spark.k8s.operator.metrics.source.KubernetesMetricsInterceptor.HTTP_REQUEST_FAILED_GROUP;
import static org.apache.spark.k8s.operator.metrics.source.KubernetesMetricsInterceptor.HTTP_REQUEST_GROUP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.metrics.source.KubernetesMetricsInterceptor;

@EnableKubernetesMockClient
class KubernetesClientFactoryTest {

  private static final String CONFIG_MAP_PATH = "/api/v1/namespaces/ns-1/configmaps/cm-1";

  private KubernetesMockServer server;
  private KubernetesClient client;

  @Test
  void withoutRetriesSendsAFailedRequestOnceThroughTheSameInterceptors() {
    KubernetesMetricsInterceptor interceptor = new KubernetesMetricsInterceptor();
    try (KubernetesClient given =
        KubernetesClientFactory.buildKubernetesClient(
            List.of(interceptor), client.getConfiguration())) {
      // The given client retries a request which fails once, and the retry finds no ConfigMap.
      server.expect().get().withPath(CONFIG_MAP_PATH).andReturn(HTTP_UNAVAILABLE, null).once();
      assertThat(given.configMaps().inNamespace("ns-1").withName("cm-1").get()).isNull();
      assertThat(meterCount(interceptor, HTTP_REQUEST_GROUP)).isEqualTo(2L);

      server.expect().get().withPath(CONFIG_MAP_PATH).andReturn(HTTP_UNAVAILABLE, null).once();
      KubernetesClient withoutRetries = KubernetesClientFactory.withoutRetries(given);
      assertThatThrownBy(
              () -> withoutRetries.configMaps().inNamespace("ns-1").withName("cm-1").get())
          .isInstanceOfSatisfying(
              KubernetesClientException.class,
              e -> assertThat(e.getCode()).isEqualTo(HTTP_UNAVAILABLE));
      // The single request is still measured by the interceptor of the given client.
      assertThat(meterCount(interceptor, HTTP_REQUEST_GROUP)).isEqualTo(3L);

      // A connection failure is counted as a request but not as failed, since fabric8 reports it
      // to the interceptors only before a retry.
      long failed = meterCount(interceptor, HTTP_REQUEST_FAILED_GROUP);
      server.shutdown();
      assertThatThrownBy(
              () -> withoutRetries.configMaps().inNamespace("ns-1").withName("cm-1").get())
          .isInstanceOf(KubernetesClientException.class);
      assertThat(meterCount(interceptor, HTTP_REQUEST_GROUP)).isEqualTo(4L);
      assertThat(meterCount(interceptor, HTTP_REQUEST_FAILED_GROUP)).isEqualTo(failed);
    }
  }

  private static long meterCount(KubernetesMetricsInterceptor interceptor, String name) {
    return interceptor.metricRegistry().meter(name).getCount();
  }
}
