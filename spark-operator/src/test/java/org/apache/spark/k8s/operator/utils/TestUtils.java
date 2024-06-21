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

package org.apache.spark.k8s.operator.utils;

import static org.apache.spark.k8s.operator.Constants.API_GROUP;
import static org.apache.spark.k8s.operator.Constants.API_VERSION;

import java.io.File;
import java.util.Map;

import io.fabric8.kubernetes.api.model.ObjectMeta;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;

public class TestUtils {
  public static SparkApplication createMockApp(String namespace) {
    SparkApplication cr = new SparkApplication();
    cr.setKind("SparkApplication");
    cr.setApiVersion(String.join("/", API_GROUP, API_VERSION));
    cr.setSpec(cr.initSpec());
    ObjectMeta meta = new ObjectMeta();
    meta.setGeneration(0L);
    meta.setLabels(Map.of(Constants.LABEL_SENTINEL_RESOURCE, "true"));
    meta.setName("sentinel");
    meta.setNamespace(namespace);
    cr.setMetadata(meta);
    return cr;
  }

  public static void cleanPropertiesFile(String filePath) {
    File myObj = new File(filePath);
    if (!myObj.delete()) {
      throw new RuntimeException("Failed to clean properties file: " + filePath);
    }
  }

  public static boolean notTimedOut(long startTime, long maxWaitTimeInMills) {
    long elapsedTimeInMills = calculateElapsedTimeInMills(startTime);
    return elapsedTimeInMills < maxWaitTimeInMills;
  }

  public static long calculateElapsedTimeInMills(long startTime) {
    return System.currentTimeMillis() - startTime;
  }
}
