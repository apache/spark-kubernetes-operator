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

package org.apache.spark.k8s.operator.kueue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.Toleration;

/**
 * The node selector and tolerations of the ResourceFlavors which Kueue assigned to a pod set.
 * Like Kueue built-in integrations, they are applied to the pods of the pod set.
 *
 * @param nodeSelector The merged `nodeLabels` of the ResourceFlavors.
 * @param tolerations The merged `tolerations` of the ResourceFlavors.
 */
public record KueuePodSetFlavor(Map<String, String> nodeSelector, List<Toleration> tolerations) {

  private static final String OPERATOR_EQUAL = "Equal";

  /**
   * Adds the node selector and tolerations to the given pod spec. A node selector conflict must be
   * checked beforehand, see {@link KueueWorkloadUtils#resolvePodSetFlavors}.
   *
   * @param podSpec The pod spec to be modified in place.
   */
  public void applyTo(final PodSpec podSpec) {
    Map<String, String> mergedNodeSelector = new HashMap<>();
    if (podSpec.getNodeSelector() != null) {
      mergedNodeSelector.putAll(podSpec.getNodeSelector());
    }
    mergedNodeSelector.putAll(nodeSelector);
    podSpec.setNodeSelector(mergedNodeSelector);
    List<Toleration> mergedTolerations = new ArrayList<>();
    if (podSpec.getTolerations() != null) {
      mergedTolerations.addAll(podSpec.getTolerations());
    }
    addTolerations(mergedTolerations, tolerations);
    podSpec.setTolerations(mergedTolerations);
  }

  /**
   * Like Kueue's tolerations.Merge, appends the tolerations which are not present yet, so that the
   * existing one wins.
   */
  static void addTolerations(final List<Toleration> target, final List<Toleration> extras) {
    for (Toleration toleration : extras) {
      if (target.stream().noneMatch(t -> isSameToleration(t, toleration))) {
        target.add(toleration);
      }
    }
  }

  /**
   * Like Kueue's tolerations.Equal, compares the key, operator, value and effect. Like Go, a
   * missing field is the same as an empty one. An empty operator is the same as `Equal`, and
   * `tolerationSeconds` is ignored.
   */
  static boolean isSameToleration(final Toleration a, final Toleration b) {
    return orEmpty(a.getKey()).equals(orEmpty(b.getKey()))
        && operator(a).equals(operator(b))
        && orEmpty(a.getValue()).equals(orEmpty(b.getValue()))
        && orEmpty(a.getEffect()).equals(orEmpty(b.getEffect()));
  }

  private static String operator(final Toleration toleration) {
    String operator = orEmpty(toleration.getOperator());
    return operator.isEmpty() ? OPERATOR_EQUAL : operator;
  }

  private static String orEmpty(final String value) {
    return Objects.toString(value, "");
  }
}
