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

import static org.apache.spark.k8s.operator.spec.DeploymentMode.ClusterMode;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ApplicationSpecTest {
  @Test
  void testBuilder() {
    ApplicationSpec spec1 = new ApplicationSpec();
    ApplicationSpec spec2 = new ApplicationSpec.ApplicationSpecBuilder().build();
    assertEquals(spec1, spec2);
  }

  @Test
  void testInitSpecWithDefaults() {
    ApplicationSpec spec = new ApplicationSpec();
    assertEquals(ClusterMode, spec.getDeploymentMode());
    assertNull(spec.getDriverSpec());
    assertNull(spec.getExecutorSpec());
    ApplicationTolerations tolerations = spec.getApplicationTolerations();
    assertNotNull(tolerations);
    assertEquals(RestartPolicy.Never, tolerations.getRestartConfig().getRestartPolicy());
    assertEquals(ResourceRetainPolicy.Never, tolerations.getResourceRetainPolicy());
    assertEquals(0, tolerations.instanceConfig.initExecutors);
    assertEquals(0, tolerations.instanceConfig.minExecutors);
    assertEquals(0, tolerations.instanceConfig.maxExecutors);
  }
}
