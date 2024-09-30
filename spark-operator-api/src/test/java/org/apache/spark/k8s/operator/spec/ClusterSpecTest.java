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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ClusterSpecTest {
  @Test
  void testBuilder() {
    ClusterSpec spec1 = new ClusterSpec();
    ClusterSpec spec2 = new ClusterSpec.ClusterSpecBuilder().build();
    assertEquals(spec1, spec2);
  }

  @Test
  void testInitSpecWithDefaults() {
    ClusterSpec spec1 = new ClusterSpec();
    assertEquals(null, spec1.runtimeVersions.jdkVersion);
    assertEquals(null, spec1.runtimeVersions.scalaVersion);
    assertEquals(null, spec1.runtimeVersions.sparkVersion);
    assertEquals(0, spec1.clusterTolerations.instanceConfig.initWorkers);
    assertEquals(0, spec1.clusterTolerations.instanceConfig.minWorkers);
    assertEquals(0, spec1.clusterTolerations.instanceConfig.maxWorkers);
  }
}
