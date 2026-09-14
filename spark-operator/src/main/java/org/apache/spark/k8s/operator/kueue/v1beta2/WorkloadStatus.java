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

package org.apache.spark.k8s.operator.kueue.v1beta2;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.fabric8.kubernetes.api.model.Condition;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * WorkloadStatus represents the current status of a Kueue Workload.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkloadStatus {
  @Builder.Default
  private List<Condition> conditions = new ArrayList<>();
  private Admission admission;

  /**
   * Checks if the workload has been admitted by Kueue.
   *
   * @return true if an "Admitted" condition exists with status "True", false otherwise.
   */
  public boolean isAdmitted() {
    return conditions != null
        && conditions.stream()
            .anyMatch(
                c ->
                    "Admitted".equalsIgnoreCase(c.getType())
                        && "True".equalsIgnoreCase(c.getStatus()));
  }

  /**
   * Checks if the workload has finished.
   *
   * @return true if a "Finished" condition exists with status "True", false otherwise.
   */
  public boolean isFinished() {
    return conditions != null
        && conditions.stream()
            .anyMatch(
                c ->
                    "Finished".equalsIgnoreCase(c.getType())
                        && "True".equalsIgnoreCase(c.getStatus()));
  }
}
