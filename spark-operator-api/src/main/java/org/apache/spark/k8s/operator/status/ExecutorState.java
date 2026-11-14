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

package org.apache.spark.k8s.operator.status;

/**
 * The state of an executor of a Spark application, observed from its pod and declared by the
 * operator in {@code status.executorState}, so that a client such as a batch scheduler can follow
 * the executors which Spark Dynamic Allocation adds and removes.
 *
 * @since 1.1.0
 */
public enum ExecutorState {
  /** The executor pod is pending: not yet scheduled, held by a scheduling gate, or starting. */
  PENDING,

  /** The executor pod is running, including while it is being deleted. */
  RUNNING,

  /** The executor pod succeeded, or disappeared after the application succeeded. */
  COMPLETED,

  /** The executor pod failed, or disappeared while the driver was not running. */
  FAILED,

  /**
   * The phase of the executor pod is unknown, or the pod disappeared while the driver was running.
   */
  UNKNOWN;

  /**
   * Checks whether the executor has terminated, i.e. it completed or failed.
   *
   * @return True if the executor completed or failed.
   */
  public boolean isTerminated() {
    return this == COMPLETED || this == FAILED;
  }
}
