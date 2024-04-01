/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.kubernetes.operator;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.k8s.submit.JavaMainAppResource;
import org.apache.spark.deploy.k8s.submit.PythonMainAppResource;
import org.apache.spark.deploy.k8s.submit.RMainAppResource;
import org.apache.spark.kubernetes.operator.spec.ApplicationSpec;
import org.apache.spark.kubernetes.operator.status.ApplicationAttemptSummary;
import org.apache.spark.kubernetes.operator.status.ApplicationStatus;
import org.apache.spark.kubernetes.operator.status.AttemptInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

class ApplicationClientWorkerTest {
    @Test
    void buildDriverConfShouldApplySpecAndPropertiesOverride() {
        Map<ApplicationDriverConf, List<Object>> constructorArgs = new HashMap<>();
        try (MockedConstruction<ApplicationDriverConf> mocked = mockConstruction(
                ApplicationDriverConf.class,
                (mock, context) -> constructorArgs.put(mock,
                        new ArrayList<>(context.arguments())))) {
            SparkApplication mockApp = mock(SparkApplication.class);
            ApplicationSpec mockSpec = mock(ApplicationSpec.class);
            ObjectMeta appMeta = new ObjectMetaBuilder()
                    .withName("app1")
                    .withNamespace("ns1")
                    .build();
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

            ApplicationDriverConf conf =
                    ApplicationClientWorker.buildDriverConf(mockApp, overrides);
            Assertions.assertEquals(6, constructorArgs.get(conf).size());

            // validate SparkConf with override
            Assertions.assertTrue(constructorArgs.get(conf).get(0) instanceof SparkConf);
            SparkConf createdConf = (SparkConf) constructorArgs.get(conf).get(0);
            Assertions.assertEquals("bar", createdConf.get("foo"));
            Assertions.assertEquals("5", createdConf.get("spark.executor.instances"));

            // namespace from CR takes highest precedence
            Assertions.assertEquals("ns1", createdConf.get("spark.kubernetes.namespace"));

            // validate main resources
            Assertions.assertTrue(constructorArgs.get(conf).get(2) instanceof JavaMainAppResource);
            JavaMainAppResource mainResource =
                    (JavaMainAppResource) constructorArgs.get(conf).get(2);
            Assertions.assertTrue(mainResource.primaryResource().isEmpty());

            Assertions.assertEquals("foo-class", constructorArgs.get(conf).get(3));

            Assertions.assertTrue(constructorArgs.get(conf).get(4) instanceof String[]);
            String[] capturedArgs = (String[]) constructorArgs.get(conf).get(4);
            Assertions.assertEquals(2, capturedArgs.length);
            Assertions.assertEquals("a", capturedArgs[0]);
            Assertions.assertEquals("b", capturedArgs[1]);
        }
    }

    @Test
    void buildDriverConfForPythonApp() {
        Map<ApplicationDriverConf, List<Object>> constructorArgs = new HashMap<>();
        try (MockedConstruction<ApplicationDriverConf> mocked = mockConstruction(
                ApplicationDriverConf.class,
                (mock, context) -> constructorArgs.put(mock,
                        new ArrayList<>(context.arguments())))) {
            SparkApplication mockApp = mock(SparkApplication.class);
            ApplicationSpec mockSpec = mock(ApplicationSpec.class);
            ObjectMeta appMeta = new ObjectMetaBuilder()
                    .withName("app1")
                    .withNamespace("ns1")
                    .build();
            when(mockApp.getSpec()).thenReturn(mockSpec);
            when(mockApp.getMetadata()).thenReturn(appMeta);
            when(mockSpec.getPyFiles()).thenReturn("foo");

            ApplicationDriverConf conf =
                    ApplicationClientWorker.buildDriverConf(mockApp, Collections.emptyMap());
            Assertions.assertEquals(6, constructorArgs.get(conf).size());

            // validate main resources
            Assertions.assertTrue(
                    constructorArgs.get(conf).get(2) instanceof PythonMainAppResource);
            PythonMainAppResource mainResource =
                    (PythonMainAppResource) constructorArgs.get(conf).get(2);
            Assertions.assertEquals("foo", mainResource.primaryResource());
        }
    }

    @Test
    void buildDriverConfForRApp() {
        Map<ApplicationDriverConf, List<Object>> constructorArgs = new HashMap<>();
        try (MockedConstruction<ApplicationDriverConf> mocked = mockConstruction(
                ApplicationDriverConf.class,
                (mock, context) -> constructorArgs.put(mock,
                        new ArrayList<>(context.arguments())))) {
            SparkApplication mockApp = mock(SparkApplication.class);
            ApplicationSpec mockSpec = mock(ApplicationSpec.class);
            ObjectMeta appMeta = new ObjectMetaBuilder()
                    .withName("app1")
                    .withNamespace("ns1")
                    .build();
            when(mockApp.getSpec()).thenReturn(mockSpec);
            when(mockApp.getMetadata()).thenReturn(appMeta);
            when(mockSpec.getSparkRFiles()).thenReturn("foo");

            ApplicationDriverConf conf =
                    ApplicationClientWorker.buildDriverConf(mockApp, Collections.emptyMap());
            Assertions.assertEquals(6, constructorArgs.get(conf).size());

            // validate main resources
            Assertions.assertTrue(constructorArgs.get(conf).get(2) instanceof RMainAppResource);
            RMainAppResource mainResource = (RMainAppResource) constructorArgs.get(conf).get(2);
            Assertions.assertEquals("foo", mainResource.primaryResource());
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
        ObjectMeta appMeta1 = new ObjectMetaBuilder()
                .withName(appName1)
                .withNamespace("ns")
                .build();
        ObjectMeta appMeta2 = new ObjectMetaBuilder()
                .withName(appName2)
                .withNamespace("ns")
                .build();
        when(mockApp1.getMetadata()).thenReturn(appMeta1);
        when(mockApp2.getMetadata()).thenReturn(appMeta2);
        when(mockApp1.getStatus()).thenReturn(mockStatus1);
        when(mockApp2.getStatus()).thenReturn(mockStatus2);

        String appId1 = ApplicationClientWorker.createSparkAppId(mockApp1);
        String appId2 = ApplicationClientWorker.createSparkAppId(mockApp2);

        Assertions.assertNotEquals(appId1, appId2);
        Assertions.assertTrue(appId1.contains(appName1));
        // multiple invoke shall give same result
        Assertions.assertEquals(appId1, ApplicationClientWorker.createSparkAppId(mockApp1));

        ApplicationAttemptSummary mockAttempt = mock(ApplicationAttemptSummary.class);
        AttemptInfo mockAttemptInfo = mock(AttemptInfo.class);
        when(mockAttempt.getAttemptInfo()).thenReturn(mockAttemptInfo);
        when(mockAttemptInfo.getId()).thenReturn(2L);
        when(mockStatus1.getCurrentAttemptSummary()).thenReturn(mockAttempt);

        String appId1Attempt2 = ApplicationClientWorker.createSparkAppId(mockApp1);
        Assertions.assertTrue(appId1Attempt2.contains(appName1));
        Assertions.assertNotEquals(appId1, appId1Attempt2);

        Assertions.assertEquals(appId1Attempt2, ApplicationClientWorker.createSparkAppId(mockApp1));
    }
}
