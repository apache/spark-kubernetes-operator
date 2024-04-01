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

package org.apache.spark.kubernetes.operator;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;
import org.apache.spark.kubernetes.operator.spec.ApplicationSpec;
import org.apache.spark.kubernetes.operator.status.ApplicationAttemptSummary;
import org.apache.spark.kubernetes.operator.status.ApplicationState;
import org.apache.spark.kubernetes.operator.status.ApplicationStateSummary;
import org.apache.spark.kubernetes.operator.status.ApplicationStatus;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize()
@Group(Constants.API_GROUP)
@Version(Constants.API_VERSION)
@ShortNames({"sparkapp"})
@JsonIgnoreProperties(ignoreUnknown = true)
public class SparkApplication extends
        BaseResource<ApplicationStateSummary, ApplicationAttemptSummary, ApplicationState,
                ApplicationSpec, ApplicationStatus> {
    @Override
    public ApplicationStatus initStatus() {
        return new ApplicationStatus();
    }

    @Override
    public ApplicationSpec initSpec() {
        return new ApplicationSpec();
    }
}
