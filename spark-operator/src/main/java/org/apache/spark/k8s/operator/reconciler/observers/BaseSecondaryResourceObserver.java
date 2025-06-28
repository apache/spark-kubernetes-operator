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

package org.apache.spark.k8s.operator.reconciler.observers;

import java.util.Optional;

import io.fabric8.kubernetes.api.model.HasMetadata;

import org.apache.spark.k8s.operator.spec.BaseSpec;
import org.apache.spark.k8s.operator.status.BaseAttemptSummary;
import org.apache.spark.k8s.operator.status.BaseState;
import org.apache.spark.k8s.operator.status.BaseStatus;

/**
 * Observe given secondary resource, return state to be updated if applicable. These observers only
 * observe secondary resource status and update the status of owner, SparkApplication, if needed.
 *
 * @param <S> The type of the state summary.
 * @param <AS> The type of the attempt summary.
 * @param <STATE> The type of the state.
 * @param <SPEC> The type of the spec.
 * @param <STATUS> The type of the status.
 * @param <SR> The type of the secondary resource.
 */
public abstract class BaseSecondaryResourceObserver<
    S,
    AS extends BaseAttemptSummary,
    STATE extends BaseState<S>,
    SPEC extends BaseSpec,
    STATUS extends BaseStatus<S, STATE, AS>,
    SR extends HasMetadata> {
  public abstract Optional<STATE> observe(SR secondaryResource, SPEC spec, STATUS currentStatus);
}
