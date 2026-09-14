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

package org.apache.spark.k8s.operator.kueue.v1beta1;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Condition;
import io.fabric8.kubernetes.api.model.ConditionBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.utils.ModelUtils;

class WorkloadTest {

  private static final ObjectMapper OBJECT_MAPPER = ModelUtils.objectMapper;

  @Test
  void testWorkloadSerializationDeserialization() throws Exception {
    PodSet driverPodSet =
        PodSet.builder()
            .name("driver")
            .count(1)
            .template(new PodTemplateSpecBuilder().build())
            .build();
    PodSet executorPodSet =
        PodSet.builder()
            .name("executor")
            .count(5)
            .minCount(2)
            .template(new PodTemplateSpecBuilder().build())
            .build();

    Workload workload = new Workload();
    workload.setMetadata(
        new ObjectMetaBuilder()
            .withName("test-workload")
            .withNamespace("default")
            .withLabels(Map.of(Constants.LABEL_QUEUE_NAME, "test-queue"))
            .build());

    workload.setSpec(
        WorkloadSpec.builder()
            .queueName("test-queue")
            .active(true)
            .podSets(List.of(driverPodSet, executorPodSet))
            .priorityClassName("high-priority")
            .priority(100)
            .build());

    Condition admittedCondition =
        new ConditionBuilder()
            .withType("Admitted")
            .withStatus("True")
            .withReason("AdmittedByKueue")
            .build();

    PodSetFlavors driverFlavors =
        PodSetFlavors.builder()
            .name("driver")
            .flavors(Map.of("cpu", "default-flavor"))
            .nodeSelector(Map.of("instance-type", "m5.large"))
            .tolerations(
                List.of(
                    new TolerationBuilder()
                        .withKey("spot")
                        .withOperator("Equal")
                        .withValue("true")
                        .build()))
            .build();

    Admission admission =
        Admission.builder()
            .clusterQueue("cluster-queue")
            .podSetFlavors(List.of(driverFlavors))
            .build();

    workload.setStatus(
        WorkloadStatus.builder()
            .conditions(List.of(admittedCondition))
            .admission(admission)
            .build());

    String json = OBJECT_MAPPER.writeValueAsString(workload);
    Workload deserialized = OBJECT_MAPPER.readValue(json, Workload.class);

    assertNotNull(deserialized);
    assertEquals("test-workload", deserialized.getMetadata().getName());
    assertEquals("test-queue", deserialized.getSpec().getQueueName());
    assertTrue(deserialized.getSpec().getActive());
    assertEquals(2, deserialized.getSpec().getPodSets().size());
    assertEquals("driver", deserialized.getSpec().getPodSets().get(0).getName());
    assertEquals(1, deserialized.getSpec().getPodSets().get(0).getCount());
    assertEquals("executor", deserialized.getSpec().getPodSets().get(1).getName());
    assertEquals(5, deserialized.getSpec().getPodSets().get(1).getCount());
    assertEquals(2, deserialized.getSpec().getPodSets().get(1).getMinCount());
    assertTrue(deserialized.getStatus().isAdmitted());
    assertFalse(deserialized.getStatus().isFinished());
    assertNotNull(deserialized.getStatus().getAdmission());
    assertEquals("cluster-queue", deserialized.getStatus().getAdmission().getClusterQueue());
    assertEquals(1, deserialized.getStatus().getAdmission().getPodSetFlavors().size());
    assertEquals(
        "m5.large",
        deserialized
            .getStatus()
            .getAdmission()
            .getPodSetFlavors()
            .get(0)
            .getNodeSelector()
            .get("instance-type"));
  }

  @Test
  void testWorkloadStatusConditions() {
    WorkloadStatus status = new WorkloadStatus();
    assertFalse(status.isAdmitted());
    assertFalse(status.isFinished());

    Condition nonAdmitted =
        new ConditionBuilder().withType("Admitted").withStatus("False").build();
    status.getConditions().add(nonAdmitted);
    assertFalse(status.isAdmitted());

    Condition admitted =
        new ConditionBuilder().withType("Admitted").withStatus("True").build();
    status.getConditions().add(admitted);
    assertTrue(status.isAdmitted());
    assertFalse(status.isFinished());

    Condition finished =
        new ConditionBuilder().withType("Finished").withStatus("True").build();
    status.getConditions().add(finished);
    assertTrue(status.isFinished());
  }

  @Test
  void testInitSpecAndStatus() {
    Workload workload = new Workload();
    assertNotNull(workload.initSpec());
    assertNotNull(workload.initStatus());
    assertNotNull(new WorkloadList());
  }
}
