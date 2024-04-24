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
public class ApplicationTolerations {
  @Builder.Default protected RestartConfig restartConfig = new RestartConfig();

  @Builder.Default
  protected ApplicationTimeoutConfig applicationTimeoutConfig = new ApplicationTimeoutConfig();

  /** Determine the toleration behavior for executor / worker instances. */
  @Builder.Default protected InstanceConfig instanceConfig = new InstanceConfig();

  /**
   * Configure operator to delete / retain resources for an app after it terminates Please be
   * advised that: - This controls the resources created directly by Operator (spark-submit),
   * including driver and its configmap. - Resources created by driver are still controlled by
   * driver (via SparkConf). Tuning 'spark .kubernetes.driver.service.deleteOnTermination' and
   * `spark.kubernetes.executor .deleteOnTermination`in application for resource retaining policy is
   * still necessary. Setting this to "RetainOnFailure" / "NeverDelete" can be helpful in dev phase,
   * to debug Spark pod behavior. These shall not be enabled (or enabled with caution) for prod use
   * cases: this could cause resource quota usage increase unexpectedly Caution: in order to avoid
   * resource conflicts among multiple attempts, this should be set to 'AlwaysDelete' unless restart
   * policy is set to 'Never'
   */
  @Builder.Default
  protected ResourceRetentionPolicy resourceRetentionPolicy = ResourceRetentionPolicy.AlwaysDelete;
}
