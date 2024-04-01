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

package org.apache.spark.kubernetes.operator.client;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.okhttp.OkHttpClientFactory;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import org.apache.spark.kubernetes.operator.config.SparkOperatorConf;
import org.apache.spark.kubernetes.operator.metrics.MetricsSystem;
import org.apache.spark.kubernetes.operator.metrics.source.KubernetesMetricsInterceptor;

import java.util.ArrayList;
import java.util.List;

/**
 * Build Kubernetes Client with metrics configured
 */
public class KubernetesClientFactory {
    private static final KubernetesMetricsInterceptor kubernetesMetricsInterceptor =
            new KubernetesMetricsInterceptor();

    public static KubernetesClient buildKubernetesClient(MetricsSystem metricsSystem) {
        return buildKubernetesClient(metricsSystem, null);
    }

    public static KubernetesClient buildKubernetesClient(MetricsSystem metricsSystem,
                                                         Config kubernetesClientConfig) {
        List<Interceptor> clientInterceptors = new ArrayList<>();
        clientInterceptors.add(new RetryInterceptor());

        if (SparkOperatorConf.KubernetesClientMetricsEnabled.getValue()) {
            clientInterceptors.add(kubernetesMetricsInterceptor);
            // Avoid duplicate register metrics exception
            if (!metricsSystem.getSources().contains(kubernetesMetricsInterceptor)) {
                metricsSystem.registerSource(kubernetesMetricsInterceptor);
            }
        }

        return new KubernetesClientBuilder()
                .withConfig(kubernetesClientConfig)
                .withHttpClientFactory(
                        new OkHttpClientFactory() {
                            @Override
                            protected void additionalConfig(OkHttpClient.Builder builder) {
                                for (Interceptor interceptor : clientInterceptors) {
                                    builder.addInterceptor(interceptor);
                                }
                            }
                        }
                )
                .build();
    }
}
