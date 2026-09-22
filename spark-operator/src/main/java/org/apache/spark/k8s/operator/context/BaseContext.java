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

import java.util.Map;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.event.ResourceEventRecorder;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import org.apache.spark.k8s.operator.BaseResource;
import org.apache.spark.k8s.operator.kueue.KueuePodSetFlavor;

/**
 * Base class for context objects.
 *
 * @param <CR> The type of the custom resource.
 */
public abstract class BaseContext<CR extends BaseResource<?, ?, ?, ?, ?>> {

  /** The JOSDK context of the reconciliation this context belongs to. */
  protected final Context<?> josdkContext;

  /** The flavors which Kueue assigned to the pod sets, applied to the secondary resources. */
  protected Map<String, KueuePodSetFlavor> kueuePodSetFlavors = Map.of();

  /**
   * Constructs a context over the given JOSDK reconciliation context.
   *
   * @param josdkContext The JOSDK context of the current reconciliation.
   */
  protected BaseContext(Context<?> josdkContext) {
    this.josdkContext = josdkContext;
  }

  /**
   * Returns the custom resource associated with this context.
   *
   * @return The custom resource.
   */
  public abstract CR getResource();

  /**
   * Sets the flavors which Kueue assigned to the pod sets of the resource, so that its secondary
   * resources carry their node selectors and tolerations.
   *
   * @param kueuePodSetFlavors The KueuePodSetFlavor by the pod set name.
   */
  public void setKueuePodSetFlavors(Map<String, KueuePodSetFlavor> kueuePodSetFlavors) {
    synchronized (this) {
      this.kueuePodSetFlavors = kueuePodSetFlavors;
      applyKueuePodSetFlavors();
    }
  }

  /**
   * Applies the flavors to the secondary resource spec which was built before them, e.g. to find
   * the driver pod or to check whether the master exists. Called while holding the lock of this
   * context, so that a reader sees either spec with its flavors.
   */
  protected abstract void applyKueuePodSetFlavors();

  /**
   * Returns the Kubernetes client associated with this context.
   *
   * @return The Kubernetes client.
   */
  public KubernetesClient getClient() {
    return josdkContext.getClient();
  }

  /**
   * Returns the event recorder bound to the resource associated with this context. Recording an
   * event is best effort: the recorder drops the event when publishing is disabled, and logs and
   * swallows write failures.
   *
   * @return The event recorder.
   */
  public ResourceEventRecorder getEventRecorder() {
    return josdkContext.eventRecorder();
  }
}
