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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ApplicationSpecTest {
  @Test
  void testInitSpecWithDefaults() {
    ApplicationSpec applicationSpec1 = new ApplicationSpec();
    ApplicationSpec applicationSpec2 = new ApplicationSpec.ApplicationSpecBuilder().build();
    Assertions.assertEquals(applicationSpec1, applicationSpec2);
    Assertions.assertEquals(DeploymentMode.ClusterMode, applicationSpec1.getDeploymentMode());
    Assertions.assertNull(applicationSpec1.getDriverSpec());
    Assertions.assertNull(applicationSpec1.getExecutorSpec());
    Assertions.assertNotNull(applicationSpec1.getApplicationTolerations());
    Assertions.assertEquals(
        RestartPolicy.Never,
        applicationSpec1.getApplicationTolerations().getRestartConfig().getRestartPolicy());
    Assertions.assertEquals(
        ResourceRetainPolicy.Never,
        applicationSpec1.getApplicationTolerations().getResourceRetainPolicy());
  }
}
