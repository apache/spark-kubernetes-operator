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

import io.fabric8.kubernetes.api.model.Pod;

import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStatus;

/**
 * Observes whether driver pod reaches running state (in other words, whether its at least
 * scheduled). Note that this means `DriverStarted` and we need to wait `DriverReady` as a next
 * step.
 */
public class AppDriverRunningObserver extends BaseAppDriverObserver {
  /**
   * Observes the driver pod to determine if it is running.
   *
   * @param driver The driver Pod object.
   * @param spec The ApplicationSpec of the Spark application.
   * @param currentStatus The current ApplicationStatus of the Spark application.
   * @return An Optional containing the new ApplicationState if the driver is terminated, otherwise
   *     empty.
   */
  @Override
  public Optional<ApplicationState> observe(
      Pod driver, ApplicationSpec spec, ApplicationStatus currentStatus) {
    return observeDriverTermination(driver, true, spec);
  }
}
