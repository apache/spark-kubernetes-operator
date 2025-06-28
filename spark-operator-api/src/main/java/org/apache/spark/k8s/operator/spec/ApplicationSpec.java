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

package org.apache.spark.k8s.operator.spec;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import io.fabric8.generator.annotation.Required;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/** Spec for a Spark application. */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@EqualsAndHashCode(callSuper = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ApplicationSpec extends BaseSpec {
  protected String mainClass;
  @Required protected RuntimeVersions runtimeVersions;
  protected String jars;
  protected String pyFiles;
  protected String sparkRFiles;
  protected String files;
  @Builder.Default protected DeploymentMode deploymentMode = DeploymentMode.ClusterMode;
  protected String proxyUser;
  @Builder.Default protected List<String> driverArgs = new ArrayList<>();

  @Builder.Default
  protected ApplicationTolerations applicationTolerations = new ApplicationTolerations();

  protected BaseApplicationTemplateSpec driverSpec;
  protected BaseApplicationTemplateSpec executorSpec;
  protected List<DriverServiceIngressSpec> driverServiceIngressList;
  protected List<ConfigMapSpec> configMapSpecs;
}
