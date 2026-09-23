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

import static org.apache.spark.k8s.operator.Constants.CLUSTER_RESUMED_MESSAGE;
import static org.apache.spark.k8s.operator.Constants.CLUSTER_SUSPENDED_MESSAGE;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_CLUSTER_NAME;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_ROLE_MASTER_VALUE;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_ROLE_NAME;
import static org.apache.spark.k8s.operator.Constants.LABEL_SPARK_ROLE_WORKER_VALUE;
import static org.apache.spark.k8s.operator.SparkClusterResourceSpec.getWorkerHorizontalPodAutoscalerName;
import static org.apache.spark.k8s.operator.SparkClusterResourceSpec.getWorkerPodDisruptionBudgetName;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.KUEUE_WORKLOAD_INFORMER_ENABLED;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.SUSPEND_HOLD_REQUEUE_INTERVAL_SECONDS;
import static org.apache.spark.k8s.operator.config.SparkOperatorConf.TRIM_ATTEMPT_STATE_TRANSITION_HISTORY;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.completeAndDefaultRequeue;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.completeAndRequeueAfter;
import static org.apache.spark.k8s.operator.reconciler.ReconcileProgress.proceed;

import java.time.Duration;

import io.fabric8.kubernetes.api.model.ListOptionsBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.context.SparkClusterContext;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadFactory;
import org.apache.spark.k8s.operator.kueue.KueueWorkloadUtils;
import org.apache.spark.k8s.operator.reconciler.ReconcileProgress;
import org.apache.spark.k8s.operator.status.ClusterState;
import org.apache.spark.k8s.operator.status.ClusterStateSummary;
import org.apache.spark.k8s.operator.utils.EventUtils;
import org.apache.spark.k8s.operator.utils.ReconcilerUtils;
import org.apache.spark.k8s.operator.utils.SparkClusterStatusRecorder;
import org.apache.spark.k8s.operator.utils.StringUtils;

/**
 * Suspends a running cluster by spec.suspend, and resumes it. A cluster runs until it is deleted,
 * so suspending it is the only way to give its quota back without deleting it.
 *
 * <p>A running cluster enters Suspended before anything is released, and the master and workers
 * are released only in Suspended. Releasing them first would leave a cluster without them in
 * RunningHealthy when spec.suspend is cleared in the meantime, and nothing recreates them there.
 * A resumed cluster leaves Suspended only after everything is released, and then starts over from
 * Submitted, where {@link ClusterInitStep} applies all of its resources again and requests a new
 * Kueue admission. Resuming earlier would let it keep the StatefulSets or the admitted Workload of
 * the previous spec, so that a spec changed in the meantime would run on the old admission.
 */
@Slf4j
public final class ClusterSuspendStep extends ClusterReconcileStep {
  /**
   * Reconciles a running or suspended cluster against spec.suspend.
   *
   * @param context The SparkClusterContext for the cluster.
   * @param statusRecorder The SparkClusterStatusRecorder for recording status updates.
   * @return The ReconcileProgress indicating the next step.
   */
  @Override
  public ReconcileProgress reconcile(
      SparkClusterContext context, SparkClusterStatusRecorder statusRecorder) {
    SparkCluster cluster = context.getResource();
    boolean suspend = cluster.getSpec().isSuspend();
    ClusterStateSummary summary = cluster.getStatus().getCurrentState().getCurrentStateSummary();
    if (summary == ClusterStateSummary.RunningHealthy && suspend) {
      return appendStateAndImmediateRequeue(
          context,
          statusRecorder,
          new ClusterState(ClusterStateSummary.Suspended, CLUSTER_SUSPENDED_MESSAGE));
    }
    if (summary == ClusterStateSummary.Suspended) {
      if (!releaseResources(context)) {
        return completeAndDefaultRequeue();
      }
      if (!suspend) {
        // Like a restarted application attempt, the resumed cluster drops the history of the run
        // which is over, so that suspending and resuming it again and again keeps it bounded.
        return updateStatusAndRequeueAfter(
            context,
            statusRecorder,
            cluster
                .getStatus()
                .appendNewState(
                    new ClusterState(ClusterStateSummary.Submitted, CLUSTER_RESUMED_MESSAGE),
                    TRIM_ATTEMPT_STATE_TRANSITION_HISTORY.getValue()),
            Duration.ZERO);
      }
      return completeAndRequeueAfter(
          Duration.ofSeconds(SUSPEND_HOLD_REQUEUE_INTERVAL_SECONDS.getValue()));
    }
    return proceed();
  }

  /**
   * Releases the master and workers of a suspended cluster, and then its Kueue Workload. The
   * Services and the NetworkPolicy are kept, since they hold no quota and are applied again on
   * resume. The HorizontalPodAutoscaler and the PodDisruptionBudget of the workers are deleted by
   * name rather than from the current spec, which may no longer ask for them, so that the resumed
   * cluster gets only those which its spec asks for.
   *
   * <p>The Workload is released only after the master and worker pods are gone, since Kueue would
   * admit another workload into the quota which the terminating pods still occupy. Other pods which
   * carry the cluster label, e.g. to reach the workers through their NetworkPolicy, do not count.
   * Everything here is idempotent, so pods that are not gone yet, or a release that failed, are
   * simply looked at again. A failure that sending the request again would not fix is reported.
   *
   * @param context The SparkClusterContext for the cluster.
   * @return True once everything is released, false while pods remain or a release failed.
   */
  private boolean releaseResources(SparkClusterContext context) {
    SparkCluster cluster = context.getResource();
    String namespace = cluster.getMetadata().getNamespace();
    String name = cluster.getMetadata().getName();
    KubernetesClient client = context.getClient();
    try {
      client.resource(context.getWorkerStatefulSetSpec()).delete();
      client.resource(context.getMasterStatefulSetSpec()).delete();
      client
          .autoscaling()
          .v2()
          .horizontalPodAutoscalers()
          .inNamespace(namespace)
          .withName(getWorkerHorizontalPodAutoscalerName(name))
          .delete();
      client
          .policy()
          .v1()
          .podDisruptionBudget()
          .inNamespace(namespace)
          .withName(getWorkerPodDisruptionBudgetName(name))
          .delete();
      // Only whether any pod remains matters, so one is enough. A page may come back empty with a
      // continue token, which still means that more pods remain.
      PodList pods =
          client
              .pods()
              .inNamespace(namespace)
              .withLabel(LABEL_SPARK_CLUSTER_NAME, name)
              .withLabelIn(
                  LABEL_SPARK_ROLE_NAME,
                  LABEL_SPARK_ROLE_MASTER_VALUE,
                  LABEL_SPARK_ROLE_WORKER_VALUE)
              .list(new ListOptionsBuilder().withLimit(1L).build());
      boolean morePods = StringUtils.isNotEmpty(pods.getMetadata().getContinue());
      if (!pods.getItems().isEmpty() || morePods) {
        // The deletion of each pod is observed by the pod informer, which reconciles again.
        log.debug("Waiting for the pods of the suspended cluster to be deleted.");
        return false;
      }
      // A Workload admitted before its queue label was removed is released as well, whenever the
      // operator may access Workloads, which the Workload informer requires too.
      if (KueueWorkloadFactory.hasQueueName(cluster)
          || KUEUE_WORKLOAD_INFORMER_ENABLED.getValue()) {
        KueueWorkloadUtils.deleteWorkloadOf(client, cluster);
      }
    } catch (KubernetesClientException e) {
      log.warn("Failed to release the resources of the suspended cluster, will retry.", e);
      if (!ReconcilerUtils.isRetryableError(e)) {
        EventUtils.warn(
            context.getEventRecorder(),
            EventUtils.REASON_SUSPEND_RELEASE_FAILED,
            "Failed to release the master and workers or the Kueue Workload of the suspended "
                + "cluster, will retry. "
                + EventUtils.describe(e));
      }
      return false;
    }
    return true;
  }
}
