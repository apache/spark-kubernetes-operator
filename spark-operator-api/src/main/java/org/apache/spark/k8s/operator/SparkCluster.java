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

package org.apache.spark.k8s.operator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

import org.apache.spark.k8s.operator.spec.ClusterSpec;
import org.apache.spark.k8s.operator.status.ClusterAttemptSummary;
import org.apache.spark.k8s.operator.status.ClusterState;
import org.apache.spark.k8s.operator.status.ClusterStateSummary;
import org.apache.spark.k8s.operator.status.ClusterStatus;

/** SparkCluster is the Custom Resource Definition (CRD) for a Spark cluster. */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize()
@Group(Constants.API_GROUP)
@Version(Constants.API_VERSION)
@ShortNames({"sparkcluster"})
@JsonIgnoreProperties(ignoreUnknown = true)
public class SparkCluster
    extends BaseResource<
        ClusterStateSummary, ClusterAttemptSummary, ClusterState, ClusterSpec, ClusterStatus> {
  /**
   * Initializes and returns a new ClusterStatus object.
   *
   * @return A new ClusterStatus instance.
   */
  @Override
  public ClusterStatus initStatus() {
    return new ClusterStatus();
  }

  /**
   * Initializes and returns a new ClusterSpec object.
   *
   * @return A new ClusterSpec instance.
   */
  @Override
  public ClusterSpec initSpec() {
    return new ClusterSpec();
  }
}
