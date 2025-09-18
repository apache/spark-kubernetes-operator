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

package org.apache.spark.k8s.operator.listeners;

import org.apache.spark.k8s.operator.BaseResource;
import org.apache.spark.k8s.operator.status.BaseStatus;

/**
 * Custom listeners, if added, would be listening to resource status change.
 *
 * @param <STATUS> The type of the status.
 * @param <CR> The type of the custom resource.
 */
public abstract class BaseStatusListener<
    STATUS extends BaseStatus<?, ?, ?>, CR extends BaseResource<?, ?, ?, ?, STATUS>> {
  /**
   * Called when the status of a resource changes.
   *
   * @param resource The custom resource whose status has changed.
   * @param prevStatus The previous status of the resource.
   * @param updatedStatus The updated status of the resource.
   */
  public abstract void listenStatus(CR resource, STATUS prevStatus, STATUS updatedStatus);
}
