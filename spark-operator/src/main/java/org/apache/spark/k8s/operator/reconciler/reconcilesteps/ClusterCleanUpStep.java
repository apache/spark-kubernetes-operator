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

import static org.apache.spark.k8s.operator.Constants.CLUSTER_RELEASED_AFTER_SCHEDULING_FAILURE_MESSAGE;
import static org.apache.spark.k8s.operator.SparkClusterResourceSpec.getMasterServiceName;
import static org.apache.spark.k8s.operator.SparkClusterResourceSpec.getWorkerNetworkPolicyName;
import static org.apache.spark.k8s.operator.SparkClusterResourceSpec.getWorkerServiceName;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.completeAndDefaultRequeue;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.proceed;

import java.util.List;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.context.SparkClusterContext;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadUtils;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.status.ClusterState;
import org.apache.spark.k8s.operator.status.ClusterStateSummary;
import org.apache.spark.k8s.operator.utils.EventUtils;
import org.apache.spark.k8s.operator.utils.ReconcilerUtils;
import org.apache.spark.k8s.operator.utils.SparkClusterStatusRecorder;

/**
 * Releases the resources of a cluster which failed with SchedulingFailure, and then terminates it
 * with ResourceReleased. Nothing requests them again, so the master and workers which were
 * requested before the failure would otherwise keep running, and its Kueue Workload would keep its
 * quota, until the cluster is deleted.
 */
@Slf4j
public final class ClusterCleanUpStep extends ClusterReconcileStep {
  /**
   * Releases the resources of a cluster in SchedulingFailure, which {@link ClusterInitStep} records
   * with the cause before anything is released, so that the release, which lasts until the pods
   * are gone, has a state to continue in. Like {@link ClusterSuspendStep}, everything is deleted by
   * name, since the spec may not build, which may be the failure itself, and the resources may be
   * left from before the cluster was suspended. Unlike a suspended cluster, the Services and the
   * NetworkPolicy are deleted as well, since nothing applies them again.
   *
   * <p>The Workload is released only after the master and worker pods are gone, or are still
   * terminating past their timeout, see {@link ClusterSuspendStep#waitForPods}. A pod stuck in
   * terminating does not hold the cluster beyond that, since no pod of its name is created again.
   * The Services and the NetworkPolicy hold no quota, so they are deleted only after the Workload
   * is released, like the resources of an application in {@link AppCleanUpStep}. Otherwise a
   * failure to delete them, e.g. by an operator which may not access them and so failed to apply
   * them, would hold the quota forever. Everything here is idempotent, so a release that failed is
   * simply retried. A failure which is not expected to clear on its own is reported, since
   * SchedulingFailure reports the failure to request the resources, not the one to release them.
   *
   * @param context The SparkClusterContext for the cluster.
   * @param statusRecorder The SparkClusterStatusRecorder for recording status updates.
   * @return The ReconcileProgress indicating the next step.
   */
  @Override
  public ReconcileProgress reconcile(
      SparkClusterContext context, SparkClusterStatusRecorder statusRecorder) {
    SparkCluster cluster = context.getResource();
    if (cluster.getStatus().getCurrentState().getCurrentStateSummary()
        != ClusterStateSummary.SchedulingFailure) {
      return proceed();
    }
    String namespace = cluster.getMetadata().getNamespace();
    String name = cluster.getMetadata().getName();
    KubernetesClient client = context.getClient();
    try {
      List<Pod> pods = ClusterSuspendStep.deleteMasterAndWorkers(client, cluster);
      Optional<ReconcileProgress> waiting = ClusterSuspendStep.waitForPods(pods);
      if (waiting.isPresent()) {
        log.debug("Waiting for the pods of the failed cluster to be deleted.");
        return waiting.get();
      }
      // A Workload admitted before the queue label was removed is released as well.
      KueueWorkloadUtils.deleteWorkloadOf(client, cluster);
      client.services().inNamespace(namespace).withName(getMasterServiceName(name)).delete();
      client.services().inNamespace(namespace).withName(getWorkerServiceName(name)).delete();
      client
          .network()
          .networkPolicies()
          .inNamespace(namespace)
          .withName(getWorkerNetworkPolicyName(name))
          .delete();
    } catch (KubernetesClientException e) {
      log.warn("Failed to release the resources of the failed cluster, will retry.", e);
      if (!ReconcilerUtils.isTransientError(e)) {
        EventUtils.warn(
            context.getEventRecorder(),
            EventUtils.REASON_CLUSTER_RELEASE_FAILED,
            "Failed to release the resources or the Kueue Workload of the cluster which failed "
                + "to request them, will retry. "
                + EventUtils.describe(e));
      }
      return completeAndDefaultRequeue();
    }
    return appendStateAndImmediateRequeue(
        context,
        statusRecorder,
        new ClusterState(
            ClusterStateSummary.ResourceReleased,
            CLUSTER_RELEASED_AFTER_SCHEDULING_FAILURE_MESSAGE));
  }
}
