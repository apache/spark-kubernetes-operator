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

package org.apache.spark.k8s.operator.spec;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeer;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Network policy for the Spark workers.
 *
 * @since 0.1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class WorkerNetworkPolicySpec {
  /**
   * Port of a metrics endpoint (e.g. a JMX-to-Prometheus exporter agent) running in the worker
   * container, outside Spark's own web UI. The caller is responsible for making the container
   * actually listen on this port. Takes effect only together with {@link #metricsIngress}.
   */
  protected Integer metricsPort;

  /**
   * Sources allowed to scrape {@link #metricsPort} on the worker pods, in addition to the sources
   * the generated worker NetworkPolicy admits by default. When this list is empty, or when {@link
   * #metricsPort} is unset, no metrics ingress rule is generated and worker ingress stays as
   * restrictive as it is without these fields.
   */
  protected List<NetworkPolicyPeer> metricsIngress;
}
