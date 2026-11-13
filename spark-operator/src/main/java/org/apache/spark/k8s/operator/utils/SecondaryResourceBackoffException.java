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

package org.apache.spark.k8s.operator.utils;

import java.time.Duration;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.KubernetesClientException;
import lombok.Getter;

/**
 * Signals that {@link ReconcilerUtils#getOrCreateSecondaryResource} needs to wait longer than the
 * reconciler should block a thread for before retrying resource creation. Callers must catch this
 * ahead of any broader {@code catch (Exception e)} and requeue the reconcile after the delay
 * carried by this exception instead of treating it as a failure.
 */
@Getter
public final class SecondaryResourceBackoffException extends RuntimeException {

  private final HasMetadata resource;
  private final Duration requeueAfter;
  private final long attemptCount;
  private final long maxAttempts;

  SecondaryResourceBackoffException(
      HasMetadata resource,
      Duration requeueAfter,
      long attemptCount,
      long maxAttempts,
      KubernetesClientException cause) {
    super(
        "Backing off "
            + requeueAfter
            + " before retrying creation of resource with kind: "
            + resource.getKind()
            + ", name: "
            + resource.getMetadata().getName()
            + " (attempt "
            + attemptCount
            + "/"
            + maxAttempts
            + ")",
        cause);
    this.resource = resource;
    this.requeueAfter = requeueAfter;
    this.attemptCount = attemptCount;
    this.maxAttempts = maxAttempts;
  }
}
