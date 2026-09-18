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

package org.apache.spark.k8s.operator.kueue.v1beta2;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import org.apache.spark.k8s.operator.Constants;

/**
 * Fabric8 CustomResource representation of a cluster-scoped Kueue ResourceFlavor
 * (kueue.x-k8s.io/v1beta2).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize
@Group(Constants.KUEUE_API_GROUP)
@Version(Constants.KUEUE_API_VERSION)
@JsonIgnoreProperties(ignoreUnknown = true)
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class ResourceFlavor extends CustomResource<ResourceFlavorSpec, Void> {

  @Override
  protected ResourceFlavorSpec initSpec() {
    return new ResourceFlavorSpec();
  }
}
