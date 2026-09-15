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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.context.SparkClusterContext;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.status.ClusterState;
import org.apache.spark.k8s.operator.status.ClusterStateSummary;
import org.apache.spark.k8s.operator.utils.SparkClusterStatusRecorder;

class ClusterInitStepTest {
  @Test
  void suspendedClusterDoesNotRequestResources() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = new SparkCluster();
    cluster.setMetadata(
        new ObjectMetaBuilder().withName("cluster1").withNamespace("default").build());
    cluster.getSpec().setSuspend(true);
    when(mockContext.getResource()).thenReturn(cluster);

    ReconcileProgress progress = clusterInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.completeAndDefaultRequeue(), progress);
    verify(mockContext, never()).getMasterServiceSpec();
    verify(mockContext, never()).getMasterStatefulSetSpec();
    verify(mockContext, never()).getWorkerStatefulSetSpec();
    verify(mockContext, never()).getClient();
    verifyNoInteractions(recorder);
    Assertions.assertEquals(
        ClusterStateSummary.Submitted,
        cluster.getStatus().getCurrentState().getCurrentStateSummary());
  }

  @Test
  void nonInitializingClusterProceeds() {
    ClusterInitStep clusterInitStep = new ClusterInitStep();
    SparkClusterContext mockContext = mock(SparkClusterContext.class);
    SparkClusterStatusRecorder recorder = mock(SparkClusterStatusRecorder.class);
    SparkCluster cluster = new SparkCluster();
    cluster.setMetadata(
        new ObjectMetaBuilder().withName("cluster1").withNamespace("default").build());
    cluster.getSpec().setSuspend(true);
    cluster.setStatus(
        cluster
            .getStatus()
            .appendNewState(new ClusterState(ClusterStateSummary.RunningHealthy, "running")));
    when(mockContext.getResource()).thenReturn(cluster);

    ReconcileProgress progress = clusterInitStep.reconcile(mockContext, recorder);

    Assertions.assertEquals(ReconcileProgress.proceed(), progress);
    verifyNoInteractions(recorder);
  }
}
