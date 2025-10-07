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

package org.apache.spark.k8s.operator.config;

import java.io.IOException;
import java.util.Collections;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.utils.StringUtils;

class SparkOperatorConfManagerTest {
  @Test
  void testLoadPropertiesFromInitFile() throws IOException {
    String propBackUp = System.getProperty("spark.kubernetes.operator.basePropertyFileName");
    try {
      String propsFilePath =
          SparkOperatorConfManagerTest.class
              .getClassLoader()
              .getResource("spark-operator.properties")
              .getPath();
      System.setProperty("spark.kubernetes.operator.basePropertyFileName", propsFilePath);
      SparkOperatorConfManager confManager = new SparkOperatorConfManager();
      Assertions.assertEquals("bar", confManager.getValue("spark.kubernetes.operator.foo"));
    } finally {
      if (StringUtils.isNotEmpty(propBackUp)) {
        System.setProperty("spark.kubernetes.operator.basePropertyFileName", propBackUp);
      } else {
        System.clearProperty("spark.kubernetes.operator.basePropertyFileName");
      }
    }
  }

  @Test
  void testOverrideProperties() {
    String propBackUp = System.getProperty("spark.kubernetes.operator.foo");
    System.setProperty("spark.kubernetes.operator.foo", "bar");
    try {
      SparkOperatorConfManager confManager = new SparkOperatorConfManager();
      Assertions.assertEquals("bar", confManager.getInitialValue("spark.kubernetes.operator.foo"));
      Assertions.assertEquals("bar", confManager.getValue("spark.kubernetes.operator.foo"));

      confManager.refresh(Collections.singletonMap("spark.kubernetes.operator.foo", "barbar"));
      Assertions.assertEquals("bar", confManager.getInitialValue("spark.kubernetes.operator.foo"));
      Assertions.assertEquals("barbar", confManager.getValue("spark.kubernetes.operator.foo"));

      confManager.refresh(Collections.singletonMap("spark.kubernetes.operator.foo", "barbarbar"));
      Assertions.assertEquals("bar", confManager.getInitialValue("spark.kubernetes.operator.foo"));
      Assertions.assertEquals("barbarbar", confManager.getValue("spark.kubernetes.operator.foo"));

    } finally {
      if (StringUtils.isNotEmpty(propBackUp)) {
        System.setProperty("spark.kubernetes.operator.foo", propBackUp);
      } else {
        System.clearProperty("spark.kubernetes.operator.foo");
      }
    }
  }
}
