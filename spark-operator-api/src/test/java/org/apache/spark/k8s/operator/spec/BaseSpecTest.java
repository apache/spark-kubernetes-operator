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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class BaseSpecTest {
  @Test
  void testDefaults() {
    BaseSpec spec = new BaseSpec();
    assertFalse(spec.isSuspend());
    assertNotNull(spec.getSparkConf());
    assertTrue(spec.getSparkConf().isEmpty());
  }

  @Test
  void testSettersAndGetters() {
    BaseSpec spec = new BaseSpec();
    spec.setSuspend(true);
    assertTrue(spec.isSuspend());
    spec.setSuspend(false);
    assertFalse(spec.isSuspend());
  }

  @Test
  void testSerialization() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    BaseSpec spec = new BaseSpec();
    spec.setSuspend(true);
    String json = mapper.writeValueAsString(spec);
    BaseSpec deserialized = mapper.readValue(json, BaseSpec.class);
    assertEquals(spec, deserialized);
    assertTrue(deserialized.isSuspend());

    String emptyJson = "{}";
    BaseSpec fromEmpty = mapper.readValue(emptyJson, BaseSpec.class);
    assertFalse(fromEmpty.isSuspend());
  }
}
