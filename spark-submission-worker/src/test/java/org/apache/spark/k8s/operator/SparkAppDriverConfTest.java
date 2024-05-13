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

package org.apache.spark.k8s.operator;

import static org.mockito.Mockito.mock;

import java.util.UUID;

import scala.Option;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.SparkConf;
import org.apache.spark.deploy.k8s.submit.JavaMainAppResource;

class SparkAppDriverConfTest {
  @Test
  void testResourceNamePrefix() {
    // Resource prefix shall be deterministic per SparkApp per attempt
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("foo", "bar");
    sparkConf.set("spark.executor.instances", "1");
    String appId = UUID.randomUUID().toString();
    SparkAppDriverConf sparkAppDriverConf =
        SparkAppDriverConf.create(
            sparkConf, appId, mock(JavaMainAppResource.class), "foo", null, Option.empty());
    String resourcePrefix = sparkAppDriverConf.resourceNamePrefix();
    Assertions.assertEquals(resourcePrefix, appId);
    Assertions.assertTrue(sparkAppDriverConf.configMapNameDriver().contains(resourcePrefix));
    Assertions.assertTrue(sparkAppDriverConf.driverServiceName().contains(resourcePrefix));
  }
}
