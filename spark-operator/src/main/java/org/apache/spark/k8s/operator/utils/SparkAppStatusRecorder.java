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

import java.util.List;

import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.listeners.SparkAppStatusListener;
import org.apache.spark.k8s.operator.metrics.SparkAppStatusRecorderSource;
import org.apache.spark.k8s.operator.status.ApplicationState;
import org.apache.spark.k8s.operator.status.ApplicationStatus;

/** Records the status of a Spark application. */
public class SparkAppStatusRecorder
    extends StatusRecorder<ApplicationStatus, SparkApplication, SparkAppStatusListener> {
  protected final SparkAppStatusRecorderSource recorderSource;

  /**
   * Constructs a new SparkAppStatusRecorder.
   *
   * @param statusListeners A list of SparkAppStatusListener instances.
   * @param recorderSource The SparkAppStatusRecorderSource for metrics.
   */
  public SparkAppStatusRecorder(
      List<SparkAppStatusListener> statusListeners, SparkAppStatusRecorderSource recorderSource) {
    super(statusListeners, ApplicationStatus.class, SparkApplication.class);
    this.recorderSource = recorderSource;
  }

  /**
   * Appends a new state to the application status, records latency, and persists the updated
   * status.
   *
   * @param context The SparkAppContext for the application.
   * @param newState The new ApplicationState to append.
   */
  public void appendNewStateAndPersist(SparkAppContext context, ApplicationState newState) {
    ApplicationStatus appStatus = context.getResource().getStatus();
    recorderSource.recordStatusUpdateLatency(appStatus, newState);
    ApplicationStatus updatedStatus = appStatus.appendNewState(newState);
    persistStatus(context, updatedStatus);
  }
}
