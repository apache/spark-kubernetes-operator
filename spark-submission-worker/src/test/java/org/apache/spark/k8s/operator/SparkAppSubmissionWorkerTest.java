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

import static org.apache.spark.k8s.operator.SparkAppSubmissionWorker.DEFAULT_ID_LENGTH_LIMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import org.apache.spark.SparkConf;
import org.apache.spark.deploy.k8s.submit.JavaMainAppResource;
import org.apache.spark.deploy.k8s.submit.PythonMainAppResource;
import org.apache.spark.deploy.k8s.submit.RMainAppResource;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.spec.RuntimeVersions;
import org.apache.spark.k8s.operator.status.ApplicationAttemptSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;
import org.apache.spark.k8s.operator.status.AttemptInfo;

@SuppressWarnings("PMD.UnusedLocalVariable")
class SparkAppSubmissionWorkerTest {
  @Test
  void buildDriverConfShouldApplySpecAndPropertiesOverride() {
    Map<SparkAppDriverConf, List<Object>> constructorArgs = new HashMap<>();
    try (MockedConstruction<SparkAppDriverConf> mocked =
        mockConstruction(
            SparkAppDriverConf.class,
            (mock, context) -> constructorArgs.put(mock, new ArrayList<>(context.arguments())))) {
      SparkApplication mockApp = mock(SparkApplication.class);
      ApplicationSpec mockSpec = mock(ApplicationSpec.class);
      ObjectMeta appMeta = new ObjectMetaBuilder().withName("app1").withNamespace("ns1").build();
      Map<String, String> appProps = new HashMap<>();
      appProps.put("foo", "bar");
      appProps.put("spark.executor.instances", "1");
      appProps.put("spark.kubernetes.namespace", "ns2");
      Map<String, String> overrides = new HashMap<>();
      overrides.put("spark.executor.instances", "5");
      overrides.put("spark.kubernetes.namespace", "ns3");
      when(mockSpec.getSparkConf()).thenReturn(appProps);
      when(mockApp.getSpec()).thenReturn(mockSpec);
      when(mockApp.getMetadata()).thenReturn(appMeta);
      when(mockSpec.getProxyUser()).thenReturn("foo-user");
      when(mockSpec.getMainClass()).thenReturn("foo-class");
      when(mockSpec.getDriverArgs()).thenReturn(List.of("a", "b"));

      SparkAppSubmissionWorker submissionWorker = new SparkAppSubmissionWorker();
      SparkAppDriverConf conf = submissionWorker.buildDriverConf(mockApp, overrides);
      assertEquals(7, constructorArgs.get(conf).size());

      // validate SparkConf with override
      assertInstanceOf(SparkConf.class, constructorArgs.get(conf).get(0));
      SparkConf createdConf = (SparkConf) constructorArgs.get(conf).get(0);
      assertEquals("bar", createdConf.get("foo"));
      assertEquals("5", createdConf.get("spark.executor.instances"));

      assertEquals(
          "ns1",
          createdConf.get("spark.kubernetes.namespace"),
          "namespace from CR takes highest precedence");

      // validate main resources
      assertInstanceOf(JavaMainAppResource.class, constructorArgs.get(conf).get(3));
      JavaMainAppResource mainResource = (JavaMainAppResource) constructorArgs.get(conf).get(3);
      assertTrue(mainResource.primaryResource().isEmpty());

      assertEquals("foo-class", constructorArgs.get(conf).get(4));

      assertInstanceOf(String[].class, constructorArgs.get(conf).get(5));
      String[] capturedArgs = (String[]) constructorArgs.get(conf).get(5);
      assertEquals(2, capturedArgs.length);
      assertEquals("a", capturedArgs[0]);
      assertEquals("b", capturedArgs[1]);
    }
  }

  @Test
  void buildDriverConfForPythonApp() {
    Map<SparkAppDriverConf, List<Object>> constructorArgs = new HashMap<>();
    try (MockedConstruction<SparkAppDriverConf> mocked =
        mockConstruction(
            SparkAppDriverConf.class,
            (mock, context) -> constructorArgs.put(mock, new ArrayList<>(context.arguments())))) {
      SparkApplication mockApp = mock(SparkApplication.class);
      ApplicationSpec mockSpec = mock(ApplicationSpec.class);
      ObjectMeta appMeta = new ObjectMetaBuilder().withName("app1").withNamespace("ns1").build();
      when(mockApp.getSpec()).thenReturn(mockSpec);
      when(mockApp.getMetadata()).thenReturn(appMeta);
      when(mockSpec.getPyFiles()).thenReturn("foo");

      SparkAppSubmissionWorker submissionWorker = new SparkAppSubmissionWorker();
      SparkAppDriverConf conf = submissionWorker.buildDriverConf(mockApp, Collections.emptyMap());
      assertEquals(7, constructorArgs.get(conf).size());

      // validate main resources
      assertInstanceOf(PythonMainAppResource.class, constructorArgs.get(conf).get(3));
      PythonMainAppResource mainResource = (PythonMainAppResource) constructorArgs.get(conf).get(3);
      assertEquals("foo", mainResource.primaryResource());
    }
  }

  @Test
  void handlePyFiles() {
    Map<SparkAppDriverConf, List<Object>> constructorArgs = new HashMap<>();
    try (MockedConstruction<SparkAppDriverConf> mocked =
        mockConstruction(
            SparkAppDriverConf.class,
            (mock, context) -> constructorArgs.put(mock, new ArrayList<>(context.arguments())))) {
      SparkApplication mockApp = mock(SparkApplication.class);
      ApplicationSpec mockSpec = mock(ApplicationSpec.class);
      ObjectMeta appMeta = new ObjectMetaBuilder().withName("app1").withNamespace("ns1").build();
      when(mockApp.getSpec()).thenReturn(mockSpec);
      when(mockApp.getMetadata()).thenReturn(appMeta);
      when(mockSpec.getMainClass()).thenReturn("org.apache.spark.deploy.PythonRunner");
      when(mockSpec.getPyFiles()).thenReturn("main.py,lib.py");

      SparkAppSubmissionWorker submissionWorker = new SparkAppSubmissionWorker();
      SparkAppDriverConf conf = submissionWorker.buildDriverConf(mockApp, Collections.emptyMap());
      assertEquals(7, constructorArgs.get(conf).size());
      assertEquals(
          "lib.py", ((SparkConf) constructorArgs.get(conf).get(0)).get("spark.submit.pyFiles"));

      // validate main resources
      assertInstanceOf(PythonMainAppResource.class, constructorArgs.get(conf).get(3));
      PythonMainAppResource mainResource = (PythonMainAppResource) constructorArgs.get(conf).get(3);
      assertEquals("main.py", mainResource.primaryResource());
    }
  }

  @Test
  void buildDriverConfForRApp() {
    Map<SparkAppDriverConf, List<Object>> constructorArgs = new HashMap<>();
    try (MockedConstruction<SparkAppDriverConf> mocked =
        mockConstruction(
            SparkAppDriverConf.class,
            (mock, context) -> constructorArgs.put(mock, new ArrayList<>(context.arguments())))) {
      SparkApplication mockApp = mock(SparkApplication.class);
      ApplicationSpec mockSpec = mock(ApplicationSpec.class);
      ObjectMeta appMeta = new ObjectMetaBuilder().withName("app1").withNamespace("ns1").build();
      when(mockApp.getSpec()).thenReturn(mockSpec);
      when(mockApp.getMetadata()).thenReturn(appMeta);
      when(mockSpec.getSparkRFiles()).thenReturn("foo");

      SparkAppSubmissionWorker submissionWorker = new SparkAppSubmissionWorker();
      SparkAppDriverConf conf = submissionWorker.buildDriverConf(mockApp, Collections.emptyMap());
      assertEquals(7, constructorArgs.get(conf).size());

      // validate main resources
      assertInstanceOf(RMainAppResource.class, constructorArgs.get(conf).get(3));
      RMainAppResource mainResource = (RMainAppResource) constructorArgs.get(conf).get(3);
      assertEquals("foo", mainResource.primaryResource());
    }
  }

  @Test
  void sparkAppIdShouldBeDeterministicPerAppPerAttempt() {
    SparkApplication mockApp1 = mock(SparkApplication.class);
    SparkApplication mockApp2 = mock(SparkApplication.class);
    ApplicationStatus mockStatus1 = mock(ApplicationStatus.class);
    ApplicationStatus mockStatus2 = mock(ApplicationStatus.class);
    String appName1 = "app1";
    String appName2 = "app2";
    ObjectMeta appMeta1 = new ObjectMetaBuilder().withName(appName1).withNamespace("ns").build();
    ObjectMeta appMeta2 = new ObjectMetaBuilder().withName(appName2).withNamespace("ns").build();
    when(mockApp1.getMetadata()).thenReturn(appMeta1);
    when(mockApp2.getMetadata()).thenReturn(appMeta2);
    when(mockApp1.getStatus()).thenReturn(mockStatus1);
    when(mockApp2.getStatus()).thenReturn(mockStatus2);

    String appId1 = SparkAppSubmissionWorker.generateSparkAppId(mockApp1);
    String appId2 = SparkAppSubmissionWorker.generateSparkAppId(mockApp2);

    assertNotEquals(appId1, appId2);
    assertTrue(appId1.contains(appName1));
    assertTrue(appId1.length() <= DEFAULT_ID_LENGTH_LIMIT);
    assertTrue(appId2.length() <= DEFAULT_ID_LENGTH_LIMIT);
    // multiple invoke shall give same result
    assertEquals(
        appId1,
        SparkAppSubmissionWorker.generateSparkAppId(mockApp1),
        "Multiple invoke of generateSparkAppId shall give same result.");
    assertEquals(
        appId2,
        SparkAppSubmissionWorker.generateSparkAppId(mockApp2),
        "Multiple invoke of generateSparkAppId shall give same result.");

    ApplicationAttemptSummary mockAttempt = mock(ApplicationAttemptSummary.class);
    AttemptInfo mockAttemptInfo = mock(AttemptInfo.class);
    when(mockAttempt.getAttemptInfo()).thenReturn(mockAttemptInfo);
    when(mockAttemptInfo.getId()).thenReturn(2L);
    when(mockStatus1.getCurrentAttemptSummary()).thenReturn(mockAttempt);
    when(mockStatus2.getCurrentAttemptSummary()).thenReturn(mockAttempt);

    String appId1Attempt2 = SparkAppSubmissionWorker.generateSparkAppId(mockApp1);
    assertTrue(appId1Attempt2.contains(appName1));
    assertNotEquals(appId1, appId1Attempt2);
    assertTrue(appId1Attempt2.length() <= DEFAULT_ID_LENGTH_LIMIT);

    String appId2Attempt2 = SparkAppSubmissionWorker.generateSparkAppId(mockApp2);
    assertNotEquals(appId2, appId2Attempt2);
    assertEquals(appId2Attempt2, SparkAppSubmissionWorker.generateSparkAppId(mockApp2));
    assertTrue(appId2Attempt2.length() <= DEFAULT_ID_LENGTH_LIMIT);

    assertEquals(appId1Attempt2, SparkAppSubmissionWorker.generateSparkAppId(mockApp1));
  }

  @Test
  void generatedSparkAppIdShouldComplyLengthLimit() {
    String namespaceName = "n".repeat(253);
    String appName = "a".repeat(253);

    SparkApplication mockApp = mock(SparkApplication.class);
    ObjectMeta appMeta =
        new ObjectMetaBuilder().withName(appName).withNamespace(namespaceName).build();
    when(mockApp.getMetadata()).thenReturn(appMeta);
    String appId = SparkAppSubmissionWorker.generateSparkAppId(mockApp);
    assertTrue(appId.length() <= DEFAULT_ID_LENGTH_LIMIT);
  }

  @Test
  void checkAppIdWhenUserSpecifiedInSparkConf() {
    SparkApplication mockApp = mock(SparkApplication.class);
    ApplicationSpec mockSpec = mock(ApplicationSpec.class);
    Map<String, String> appProps = new HashMap<>();
    appProps.put("spark.app.id", "foo");
    ObjectMeta appMeta = new ObjectMetaBuilder().withName("app1").withNamespace("ns1").build();
    when(mockSpec.getSparkConf()).thenReturn(appProps);
    when(mockApp.getSpec()).thenReturn(mockSpec);
    when(mockApp.getMetadata()).thenReturn(appMeta);

    SparkAppSubmissionWorker submissionWorker = new SparkAppSubmissionWorker();
    SparkAppDriverConf conf = submissionWorker.buildDriverConf(mockApp, Collections.emptyMap());
    assertEquals("foo", conf.appId());
  }

  @Test
  void supportSparkVersionPlaceHolder() {
    SparkApplication mockApp = mock(SparkApplication.class);
    ApplicationSpec mockSpec = mock(ApplicationSpec.class);
    RuntimeVersions mockRuntimeVersions = mock(RuntimeVersions.class);
    Map<String, String> appProps = new HashMap<>();
    appProps.put("spark.kubernetes.container.image", "apache/spark:{{SPARK_VERSION}}");
    appProps.put("spark.kubernetes.driver.container.image", "apache/spark:{{SPARK_VERSION}}");
    appProps.put("spark.kubernetes.executor.container.image", "apache/spark:{{SPARK_VERSION}}");
    appProps.put("spark.kubernetes.key", "apache/spark:{{SPARK_VERSION}}");
    ObjectMeta appMeta = new ObjectMetaBuilder().withName("app1").withNamespace("ns1").build();
    when(mockSpec.getSparkConf()).thenReturn(appProps);
    when(mockApp.getSpec()).thenReturn(mockSpec);
    when(mockApp.getMetadata()).thenReturn(appMeta);
    when(mockSpec.getRuntimeVersions()).thenReturn(mockRuntimeVersions);
    when(mockRuntimeVersions.getSparkVersion()).thenReturn("dev");

    SparkAppSubmissionWorker submissionWorker = new SparkAppSubmissionWorker();
    SparkAppDriverConf conf = submissionWorker.buildDriverConf(mockApp, Collections.emptyMap());
    assertEquals("apache/spark:dev", conf.get("spark.kubernetes.container.image"));
    assertEquals("apache/spark:dev", conf.get("spark.kubernetes.driver.container.image"));
    assertEquals("apache/spark:dev", conf.get("spark.kubernetes.executor.container.image"));
    assertEquals("apache/spark:{{SPARK_VERSION}}", conf.get("spark.kubernetes.key"));
  }
}
