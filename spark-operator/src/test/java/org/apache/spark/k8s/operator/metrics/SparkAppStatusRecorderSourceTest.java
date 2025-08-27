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

package org.apache.spark.k8s.operator.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import com.codahale.metrics.Timer;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStateSummary;
import org.apache.spark.k8s.operator.status.ApplicationStatus;

class SparkAppStatusRecorderSourceTest {

  @Test
  void recordStatusUpdateLatency() {
    SparkAppStatusRecorderSource source = new SparkAppStatusRecorderSource();
    SparkApplication app1 = prepareApplication("foo-1", "bar-1");
    SparkApplication app2 = prepareApplication("foo-2", "bar-2");

    ApplicationState stateUpdate11 =
        new ApplicationState(ApplicationStateSummary.DriverRequested, "foo");
    ApplicationState stateUpdate12 =
        new ApplicationState(ApplicationStateSummary.DriverRequested, "bar");
    // record short latency
    source.recordStatusUpdateLatency(app1.getStatus(), stateUpdate11);
    source.recordStatusUpdateLatency(app2.getStatus(), stateUpdate12);
    app1.setStatus(app1.getStatus().appendNewState(stateUpdate11));

    ApplicationState stateUpdate2 =
        new ApplicationState(ApplicationStateSummary.DriverStarted, "foo");
    source.recordStatusUpdateLatency(app1.getStatus(), stateUpdate2);

    Map<String, Timer> timers = source.metricRegistry().getTimers();
    assertEquals(2, timers.size());
    assertTrue(timers.containsKey("sparkapp.latency.from.Submitted.to.DriverRequested"));
    assertTrue(
        timers.get("sparkapp.latency.from.Submitted.to.DriverRequested").getSnapshot().getMin()
            > 0);
    assertEquals(2, timers.get("sparkapp.latency.from.Submitted.to.DriverRequested").getCount());
    assertTrue(timers.containsKey("sparkapp.latency.from.DriverRequested.to.DriverStarted"));
    assertEquals(
        1, timers.get("sparkapp.latency.from.DriverRequested.to.DriverStarted").getCount());
    assertTrue(
        timers.get("sparkapp.latency.from.DriverRequested.to.DriverStarted").getSnapshot().getMin()
            > 0);
  }

  protected SparkApplication prepareApplication(String name, String namespace) {
    SparkApplication app = new SparkApplication();
    app.setMetadata(new ObjectMetaBuilder().withName(name).withNamespace(namespace).build());
    app.setStatus(new ApplicationStatus());
    return app;
  }
}
