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

package org.apache.spark.k8s.operator.reconciler.reconcilesteps;

import java.util.List;

import lombok.RequiredArgsConstructor;

import org.apache.spark.k8s.operator.context.SparkAppContext;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.reconciler.observers.BaseAppDriverObserver;
import org.apache.spark.k8s.operator.utils.SparkAppStatusRecorder;

/** Observes secondary resource and update app status if needed */
@RequiredArgsConstructor
public class AppResourceObserveStep extends AppReconcileStep {

  private final List<BaseAppDriverObserver> observers;

  @Override
  public ReconcileProgress reconcile(
      final SparkAppContext context, final SparkAppStatusRecorder statusRecorder) {
    return observeDriver(context, statusRecorder, observers);
  }
}
