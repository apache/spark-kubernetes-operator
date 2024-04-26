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

/**
 * Config tolerations of executor instances for the application. Used then the target cluster is
 * lack of batch / gang scheduling This is different from SparkConf: spark.executor.instances
 *
 * <p>For example, with below spec: spec: applicationTolerations: instanceConfig: minExecutors: 3
 * initExecutors: 5 maxExecutors: 10 sparkConf: spark.executor.instances: "10"
 *
 * <p>Spark would try to bring up 10 executors as defined in SparkConf. In addition, from SparkApp
 * perspective, + If Spark app acquires less than 5 executors in given tine window
 * (.spec.applicationTolerations.applicationTimeoutConfig.executorStartTimeoutMillis) after
 * submitted, it would be shut down proactively in order to avoid resource deadlock. + Spark app
 * would be marked as 'RUNNING_WITH_PARTIAL_CAPACITY' if it loses executors after successfully start
 * up. + Spark app would be marked as 'RunningHealthy' if it has at least min executors after
 * successfully start up.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class InstanceConfig {
  @Builder.Default protected long initExecutors = 0L;
  @Builder.Default protected long minExecutors = 0L;
  @Builder.Default protected long maxExecutors = 0L;
}
