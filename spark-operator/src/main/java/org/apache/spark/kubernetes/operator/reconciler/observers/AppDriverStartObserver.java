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

package org.apache.spark.kubernetes.operator.reconciler.observers;

import io.fabric8.kubernetes.api.model.Pod;
import org.apache.spark.kubernetes.operator.Constants;
import org.apache.spark.kubernetes.operator.spec.ApplicationSpec;
import org.apache.spark.kubernetes.operator.status.ApplicationState;
import org.apache.spark.kubernetes.operator.status.ApplicationStateSummary;
import org.apache.spark.kubernetes.operator.status.ApplicationStatus;
import org.apache.spark.kubernetes.operator.utils.PodUtils;

import java.util.Optional;

public class AppDriverStartObserver extends BaseAppDriverObserver {
    @Override
    public Optional<ApplicationState> observe(Pod driver,
                                              ApplicationSpec spec,
                                              ApplicationStatus currentStatus) {
        if (ApplicationStateSummary.DRIVER_STARTED.ordinal()
                <= currentStatus.getCurrentState().getCurrentStateSummary().ordinal()) {
            return Optional.empty();
        }
        if (PodUtils.isPodStarted(driver, spec)) {
            return Optional.of(new ApplicationState(ApplicationStateSummary.DRIVER_STARTED,
                    Constants.DriverRunning));
        }
        return observeDriverTermination(driver, false, spec);
    }
}
