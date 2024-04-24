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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ApplicationTimeoutConfig {
  /**
   * Operator may proactively terminate apps that cannot reach running healthy state to avoid
   * resource deadlock when batch scheduler is not integrated in the cluster.
   */

  /* Time to wait for driver reaches 'Running' state after requested driver */
  @Builder.Default protected Long driverStartTimeoutMillis = 300 * 1000L;

  /* Time to wait for driver becomes 'Ready' */
  @Builder.Default protected Long sparkSessionStartTimeoutMillis = 300 * 1000L;
  /* Time to wait for minimal desired number of executor pods become 'Ready' */
  @Builder.Default protected Long executorStartTimeoutMillis = 300 * 1000L;
  /* Time to wait for force delete resources at the end of attempt */
  @Builder.Default protected Long forceTerminationGracePeriodMillis = 300 * 1000L;
  /*
   * Backoff time before operator requeues the reconcile request when it cannot delete app
   * resource when cleaning up
   */
  @Builder.Default protected Long terminationRequeuePeriodMillis = 2 * 1000L;
}
