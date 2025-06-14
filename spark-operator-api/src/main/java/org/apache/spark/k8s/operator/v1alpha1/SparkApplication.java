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

package org.apache.spark.k8s.operator.v1alpha1;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.ShortNames;
import io.fabric8.kubernetes.model.annotation.Version;

import org.apache.spark.k8s.operator.BaseResource;
import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.status.ApplicationAttemptSummary;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize()
@Group(Constants.API_GROUP)
@Version(value = "v1alpha1", storage = false)
@ShortNames({"sparkapp"})
@JsonIgnoreProperties(ignoreUnknown = true)
public class SparkApplication
    extends BaseResource<
        ApplicationStateSummary,
        ApplicationAttemptSummary,
        ApplicationState,
        ApplicationSpec,
        ApplicationStatus> {
  @Override
  public ApplicationStatus initStatus() {
    return new ApplicationStatus();
  }

  @Override
  public ApplicationSpec initSpec() {
    return new ApplicationSpec();
  }

  public org.apache.spark.k8s.operator.SparkApplication asLatestSparkApplication() {
    org.apache.spark.k8s.operator.SparkApplication app =
        new org.apache.spark.k8s.operator.SparkApplication();
    app.setMetadata(getMetadata());
    app.setSpec(spec);
    app.setStatus(status);
    return app;
  }
}
