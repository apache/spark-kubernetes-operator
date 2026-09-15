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

package org.apache.spark.k8s.operator.context;

import static org.apache.spark.k8s.operator.utils.Utils.driverLabels;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.SparkAppSubmissionWorker;
import org.apache.spark.k8s.operator.SparkApplication;

class SparkAppContextTest {
  private final SparkApplication application = buildApplication();
  private final Pod driverPodSpec = driverPod("sparkapp1-1-driver");

  @Test
  void currentAttemptDriverPodIsFoundBehindPreviousAttemptPod() {
    Pod previousAttemptDriver = driverPod("sparkapp1-0-driver");
    SparkAppContext context = buildContext(List.of(previousAttemptDriver, driverPodSpec));

    Optional<Pod> driverPod = context.getCurrentAttemptDriverPod();

    Assertions.assertTrue(driverPod.isPresent());
    Assertions.assertEquals("sparkapp1-1-driver", driverPod.get().getMetadata().getName());
  }

  @Test
  void terminatingPreviousAttemptPodWithSameNameIsNotCurrentAttemptDriver() {
    // The driver pod name is reused across attempts, e.g. with a user-specified spark.app.id. The
    // previous attempt's pod has been deleted by the clean-up step and is still terminating.
    Pod terminatingPreviousAttemptDriver =
        new PodBuilder(driverPodSpec)
            .editOrNewMetadata()
            .withDeletionTimestamp(Instant.now().toString())
            .endMetadata()
            .build();
    SparkAppContext context = buildContext(List.of(terminatingPreviousAttemptDriver));

    Assertions.assertTrue(context.getDriverPod().isPresent());
    Assertions.assertTrue(context.getCurrentAttemptDriverPod().isEmpty());
  }

  @Test
  void podWithDifferentNameIsNotCurrentAttemptDriver() {
    SparkAppContext context = buildContext(List.of(driverPod("sparkapp1-0-driver")));

    Assertions.assertTrue(context.getDriverPod().isPresent());
    Assertions.assertTrue(context.getCurrentAttemptDriverPod().isEmpty());
  }

  @Test
  void noDriverPodDoesNotBuildDriverSpec() {
    SparkAppContext context = buildContext(List.of());

    Assertions.assertTrue(context.getCurrentAttemptDriverPod().isEmpty());
    org.mockito.Mockito.verify(context, org.mockito.Mockito.never()).getDriverPodSpec();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private SparkAppContext buildContext(List<Pod> cachedPods) {
    Context josdkContext = mock(Context.class);
    when(josdkContext.getSecondaryResourcesAsStream(Pod.class))
        .thenAnswer(invocation -> cachedPods.stream());
    SparkAppContext context =
        spy(new SparkAppContext(application, josdkContext, mock(SparkAppSubmissionWorker.class)));
    doReturn(driverPodSpec).when(context).getDriverPodSpec();
    return context;
  }

  private SparkApplication buildApplication() {
    SparkApplication app = new SparkApplication();
    app.setMetadata(new ObjectMetaBuilder().withName("sparkapp1").withNamespace("default").build());
    return app;
  }

  private Pod driverPod(String name) {
    return new PodBuilder()
        .withNewMetadata()
        .withName(name)
        .withNamespace("default")
        .withLabels(driverLabels(application))
        .endMetadata()
        .build();
  }
}
