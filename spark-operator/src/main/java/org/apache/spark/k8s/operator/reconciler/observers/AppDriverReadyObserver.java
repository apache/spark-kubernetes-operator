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

import static org.apache.spark.k8s.operator.Constants.DRIVER_READY_MESSAGE;
import static org.apache.spark.k8s.operator.status.ApplicationStateSummary.DriverReady;

import java.util.Optional;

import io.fabric8.kubernetes.api.model.Pod;

import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.utils.PodUtils;

/** Observes whether driver is ready */
public class AppDriverReadyObserver extends BaseAppDriverObserver {
  @Override
  public Optional<ApplicationState> observe(
      Pod driver, ApplicationSpec spec, ApplicationStatus status) {
    if (DriverReady.ordinal() <= status.getCurrentState().getCurrentStateSummary().ordinal()) {
      return Optional.empty();
    }
    if (PodUtils.isPodReady(driver)) {
      return Optional.of(new ApplicationState(DriverReady, DRIVER_READY_MESSAGE));
    }
    return observeDriverTermination(driver, true, spec);
  }
}
