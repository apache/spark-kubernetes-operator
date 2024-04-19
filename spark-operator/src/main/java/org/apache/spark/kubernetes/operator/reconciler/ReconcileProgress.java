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

package org.apache.spark.kubernetes.operator.reconciler;

import java.time.Duration;

import lombok.Data;

import static org.apache.spark.kubernetes.operator.config.SparkOperatorConf.SparkAppReconcileIntervalSeconds;

/**
 * Represents the progress of a reconcile request
 * - completed : is set to true if there's no more actions expected in the same reconciliation
 * - requeue : describes whether the mentioned resource need to be reconciled again - and if so,
 * the frequency
 */
@Data
public class ReconcileProgress {
  private boolean completed;
  boolean requeue;
  private Duration requeueAfterDuration;

  private ReconcileProgress(boolean completed, boolean requeue, Duration requeueAfterDuration) {
    this.completed = completed;
    this.requeue = requeue;
    this.requeueAfterDuration = requeueAfterDuration;
  }

  public static ReconcileProgress proceed() {
    return new ReconcileProgress(false, true,
        Duration.ofSeconds(SparkAppReconcileIntervalSeconds.getValue()));
  }

  public static ReconcileProgress completeAndDefaultRequeue() {
    return new ReconcileProgress(true, true,
        Duration.ofSeconds(SparkAppReconcileIntervalSeconds.getValue()));
  }

  public static ReconcileProgress completeAndRequeueAfter(Duration requeueAfterDuration) {
    return new ReconcileProgress(true, true, requeueAfterDuration);
  }

  public static ReconcileProgress completeAndImmediateRequeue() {
    return new ReconcileProgress(true, true, Duration.ZERO);
  }

  public static ReconcileProgress completeAndNoRequeue() {
    return new ReconcileProgress(true, false, Duration.ZERO);
  }
}
