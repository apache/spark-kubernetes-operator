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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetSpecBuilder;
import io.fabric8.kubernetes.api.model.autoscaling.v2.HorizontalPodAutoscalerSpecBuilder;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.kueue.v1beta1.PodSet;
import org.apache.spark.k8s.operator.kueue.v1beta1.Workload;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.spec.BaseApplicationTemplateSpec;
import org.apache.spark.k8s.operator.spec.ClusterSpec;
import org.apache.spark.k8s.operator.spec.MasterSpec;
import org.apache.spark.k8s.operator.spec.WorkerSpec;

class KueueWorkloadFactoryTest {

  @Test
  void testParseMemoryToMiB() {
    assertEquals(1024L, KueueWorkloadFactory.parseMemoryToMiB("1g"));
    assertEquals(1024L, KueueWorkloadFactory.parseMemoryToMiB("1G"));
    assertEquals(1024L, KueueWorkloadFactory.parseMemoryToMiB("1gi"));
    assertEquals(1024L, KueueWorkloadFactory.parseMemoryToMiB("1GiB"));
    assertEquals(512L, KueueWorkloadFactory.parseMemoryToMiB("512m"));
    assertEquals(512L, KueueWorkloadFactory.parseMemoryToMiB("512M"));
    assertEquals(512L, KueueWorkloadFactory.parseMemoryToMiB("512Mi"));
    assertEquals(2048L, KueueWorkloadFactory.parseMemoryToMiB("2048"));
    assertEquals(2048L, KueueWorkloadFactory.parseMemoryToMiB("2048m"));
    assertEquals(2L, KueueWorkloadFactory.parseMemoryToMiB("2048k"));
    assertEquals(1L, KueueWorkloadFactory.parseMemoryToMiB("1048576b"));
    assertEquals(1024L * 1024L, KueueWorkloadFactory.parseMemoryToMiB("1t"));
    assertEquals(1024L * 1024L * 1024L, KueueWorkloadFactory.parseMemoryToMiB("1p"));
    // Default fallback on empty/invalid
    assertEquals(1024L, KueueWorkloadFactory.parseMemoryToMiB(null));
    assertEquals(1024L, KueueWorkloadFactory.parseMemoryToMiB("invalid"));
  }

  @Test
  void testCalculateDriverMemoryMiB() {
    // 1g memory -> 1024MiB. Overhead factor 0.10 -> 102.4MiB -> min 384MiB.
    // Total = 1024 + 384 = 1408
    Map<String, String> conf = new HashMap<>();
    conf.put("spark.driver.memory", "1g");
    assertEquals(1408L, KueueWorkloadFactory.calculateDriverMemoryMiB(conf));

    // Explicit overhead 512m -> 1024 + 512 = 1536
    conf.put("spark.driver.memoryOverhead", "512m");
    assertEquals(1536L, KueueWorkloadFactory.calculateDriverMemoryMiB(conf));

    // Custom overhead factor 0.50 -> 1024 * 0.50 = 512 -> 1024 + 512 = 1536
    conf.remove("spark.driver.memoryOverhead");
    conf.put("spark.driver.memoryOverheadFactor", "0.50");
    assertEquals(1536L, KueueWorkloadFactory.calculateDriverMemoryMiB(conf));
  }

  @Test
  void testCalculateExecutorMemoryMiB() {
    Map<String, String> conf = new HashMap<>();
    // 2048MiB. Overhead: max(204.8, 384) = 384. Total = 2432
    conf.put("spark.executor.memory", "2g");
    assertEquals(2432L, KueueWorkloadFactory.calculateExecutorMemoryMiB(conf));

    // With PySpark memory
    conf.put("spark.executor.pyspark.memory", "1g");
    assertEquals(2432L + 1024L, KueueWorkloadFactory.calculateExecutorMemoryMiB(conf));
  }

  @Test
  void testHasAndGetQueueName() {
    SparkApplication app = new SparkApplication();
    assertFalse(KueueWorkloadFactory.hasQueueName(app));
    assertNull(KueueWorkloadFactory.getQueueName(app));

    // From label
    app.setMetadata(
        new ObjectMetaBuilder()
            .withLabels(Map.of(Constants.LABEL_QUEUE_NAME, "test-queue"))
            .build());
    assertTrue(KueueWorkloadFactory.hasQueueName(app));
    assertEquals("test-queue", KueueWorkloadFactory.getQueueName(app));

    // Empty label
    SparkApplication app2 = new SparkApplication();
    app2.setMetadata(
        new ObjectMetaBuilder()
            .withLabels(Map.of(Constants.LABEL_QUEUE_NAME, ""))
            .build());
    assertFalse(KueueWorkloadFactory.hasQueueName(app2));
    assertNull(KueueWorkloadFactory.getQueueName(app2));
  }

  @Test
  void testBuildWorkloadStaticAllocation() {
    SparkApplication app = new SparkApplication();
    app.setMetadata(
        new ObjectMetaBuilder()
            .withName("spark-pi")
            .withNamespace("spark-jobs")
            .withUid("app-uid-123")
            .withLabels(Map.of(Constants.LABEL_QUEUE_NAME, "team-a-queue"))
            .build());

    ApplicationSpec spec = new ApplicationSpec();
    spec.setSparkConf(
        Map.of(
            "spark.driver.cores", "2",
            "spark.driver.memory", "2g",
            "spark.executor.cores", "4",
            "spark.executor.memory", "4g",
            "spark.executor.instances", "3"));
    app.setSpec(spec);

    Workload workload = KueueWorkloadFactory.buildWorkload(app);
    assertNotNull(workload);
    assertEquals("spark-pi", workload.getMetadata().getName());
    assertEquals("spark-jobs", workload.getMetadata().getNamespace());
    assertEquals("team-a-queue", workload.getSpec().getQueueName());
    assertTrue(workload.getSpec().getActive());
    assertEquals(1, workload.getMetadata().getOwnerReferences().size());
    assertEquals("spark-pi", workload.getMetadata().getOwnerReferences().get(0).getName());
    assertTrue(workload.getMetadata().getOwnerReferences().get(0).getController());

    assertEquals(2, workload.getSpec().getPodSets().size());

    PodSet driverPodSet = workload.getSpec().getPodSets().get(0);
    assertEquals("driver", driverPodSet.getName());
    assertEquals(1, driverPodSet.getCount());
    assertNull(driverPodSet.getMinCount());
    Container driverContainer =
        driverPodSet.getTemplate().getSpec().getContainers().get(0);
    assertEquals(new Quantity("2"), driverContainer.getResources().getRequests().get("cpu"));
    assertEquals(
        new Quantity("2432Mi"),
        driverContainer.getResources().getRequests().get("memory")); // 2048 + 384 = 2432

    PodSet executorPodSet = workload.getSpec().getPodSets().get(1);
    assertEquals("executor", executorPodSet.getName());
    assertEquals(3, executorPodSet.getCount());
    assertNull(executorPodSet.getMinCount());
    Container executorContainer =
        executorPodSet.getTemplate().getSpec().getContainers().get(0);
    assertEquals(new Quantity("4"), executorContainer.getResources().getRequests().get("cpu"));
    assertEquals(
        new Quantity("4506Mi"),
        executorContainer.getResources().getRequests().get("memory")); // 4096 + 410 = 4506
  }

  @Test
  void testBuildWorkloadDynamicAllocation() {
    SparkApplication app = new SparkApplication();
    app.setMetadata(
        new ObjectMetaBuilder()
            .withName("spark-elastic")
            .withNamespace("default")
            .withLabels(Map.of(Constants.LABEL_QUEUE_NAME, "elastic-queue"))
            .build());

    ApplicationSpec spec = new ApplicationSpec();
    spec.setSparkConf(
        Map.of(
            "spark.dynamicAllocation.enabled", "true",
            "spark.dynamicAllocation.minExecutors", "2",
            "spark.dynamicAllocation.maxExecutors", "10",
            "spark.dynamicAllocation.initialExecutors", "4"));
    app.setSpec(spec);

    Workload workload = KueueWorkloadFactory.buildWorkload(app);
    PodSet executorPodSet = workload.getSpec().getPodSets().get(1);
    assertEquals(10, executorPodSet.getCount());
    assertEquals(2, executorPodSet.getMinCount());
  }

  @Test
  void testBuildWorkloadWithGpuAndTemplateSpec() {
    SparkApplication app = new SparkApplication();
    app.setMetadata(
        new ObjectMetaBuilder()
            .withName("spark-gpu")
            .withNamespace("default")
            .withLabels(Map.of(Constants.LABEL_QUEUE_NAME, "gpu-queue"))
            .build());

    PodTemplateSpec driverTemplate =
        new PodTemplateSpecBuilder()
            .withNewSpec()
            .withContainers(
                new ContainerBuilder()
                    .withName("custom-driver")
                    .withResources(
                        new ResourceRequirementsBuilder()
                            .withRequests(Map.of("cpu", new Quantity("3")))
                            .build())
                    .build())
            .endSpec()
            .build();

    ApplicationSpec spec = new ApplicationSpec();
    spec.setDriverSpec(new BaseApplicationTemplateSpec(driverTemplate));
    spec.setSparkConf(
        Map.of(
            "spark.driver.resource.gpu.amount", "1",
            "spark.executor.resource.gpu.amount", "2",
            "spark.executor.resource.gpu.vendor", "nvidia.com/gpu"));
    app.setSpec(spec);

    Workload workload = KueueWorkloadFactory.buildWorkload(app);
    PodSet driverPodSet = workload.getSpec().getPodSets().get(0);
    Container driverContainer =
        driverPodSet.getTemplate().getSpec().getContainers().get(0);
    assertEquals(new Quantity("3"), driverContainer.getResources().getRequests().get("cpu"));
    assertEquals(
        new Quantity("1"),
        driverContainer.getResources().getRequests().get("nvidia.com/gpu"));
    assertEquals(
        new Quantity("1"),
        driverContainer.getResources().getLimits().get("nvidia.com/gpu"));

    PodSet executorPodSet = workload.getSpec().getPodSets().get(1);
    Container executorContainer =
        executorPodSet.getTemplate().getSpec().getContainers().get(0);
    assertEquals(
        new Quantity("2"),
        executorContainer.getResources().getRequests().get("nvidia.com/gpu"));
  }

  @Test
  void testBuildWorkloadSuspended() {
    SparkApplication app = new SparkApplication();
    app.setMetadata(
        new ObjectMetaBuilder()
            .withName("spark-suspended")
            .withNamespace("default")
            .withLabels(Map.of(Constants.LABEL_QUEUE_NAME, "test-queue"))
            .build());
    ApplicationSpec spec = new ApplicationSpec();
    spec.setSuspend(true);
    app.setSpec(spec);

    Workload workload = KueueWorkloadFactory.buildWorkload(app);
    assertFalse(workload.getSpec().getActive());
  }

  @Test
  void testBuildWorkloadForSparkCluster() {
    SparkCluster cluster = new SparkCluster();
    cluster.setMetadata(
        new ObjectMetaBuilder()
            .withName("test-cluster")
            .withNamespace("spark-ns")
            .withLabels(Map.of(Constants.LABEL_QUEUE_NAME, "cluster-queue"))
            .build());

    PodTemplateSpec masterTemplate =
        new PodTemplateSpecBuilder()
            .withNewSpec()
            .addNewContainer()
            .withName("spark-master")
            .endContainer()
            .endSpec()
            .build();
    MasterSpec masterSpec =
        MasterSpec.builder()
            .statefulSetSpec(
                new StatefulSetSpecBuilder()
                    .withReplicas(1)
                    .withTemplate(masterTemplate)
                    .build())
            .build();

    PodTemplateSpec workerTemplate =
        new PodTemplateSpecBuilder()
            .withNewSpec()
            .addNewContainer()
            .withName("spark-worker")
            .endContainer()
            .endSpec()
            .build();
    WorkerSpec workerSpec =
        WorkerSpec.builder()
            .statefulSetSpec(
                new StatefulSetSpecBuilder()
                    .withReplicas(3)
                    .withTemplate(workerTemplate)
                    .build())
            .build();

    ClusterSpec clusterSpec =
        ClusterSpec.builder()
            .masterSpec(masterSpec)
            .workerSpec(workerSpec)
            .build();
    cluster.setSpec(clusterSpec);

    assertTrue(KueueWorkloadFactory.hasQueueName(cluster));
    assertEquals("cluster-queue", KueueWorkloadFactory.getQueueName(cluster));

    Workload workload = KueueWorkloadFactory.buildWorkload(cluster);
    assertNotNull(workload);
    assertEquals("test-cluster", workload.getMetadata().getName());
    assertEquals("spark-ns", workload.getMetadata().getNamespace());
    assertEquals("cluster-queue", workload.getSpec().getQueueName());
    assertTrue(workload.getSpec().getActive());
    assertEquals(
        "test-cluster",
        workload.getMetadata().getLabels().get(Constants.LABEL_SPARK_CLUSTER_NAME));
    assertEquals(2, workload.getSpec().getPodSets().size());

    PodSet masterPodSet = workload.getSpec().getPodSets().get(0);
    assertEquals("master", masterPodSet.getName());
    assertEquals(1, masterPodSet.getCount());
    assertNotNull(masterPodSet.getTemplate());

    PodSet workerPodSet = workload.getSpec().getPodSets().get(1);
    assertEquals("worker", workerPodSet.getName());
    assertEquals(3, workerPodSet.getCount());
    assertNull(workerPodSet.getMinCount());
  }

  @Test
  void testBuildWorkloadForSparkClusterWithHPA() {
    SparkCluster cluster = new SparkCluster();
    cluster.setMetadata(
        new ObjectMetaBuilder()
            .withName("test-cluster-hpa")
            .withNamespace("default")
            .withLabels(Map.of(Constants.LABEL_QUEUE_NAME, "hpa-queue"))
            .build());

    WorkerSpec workerSpec =
        WorkerSpec.builder()
            .statefulSetSpec(new StatefulSetSpecBuilder().withReplicas(2).build())
            .horizontalPodAutoscalerSpec(
                new HorizontalPodAutoscalerSpecBuilder()
                    .withMinReplicas(2)
                    .withMaxReplicas(10)
                    .build())
            .build();

    ClusterSpec clusterSpec =
        ClusterSpec.builder()
            .masterSpec(MasterSpec.builder().build())
            .workerSpec(workerSpec)
            .build();
    cluster.setSpec(clusterSpec);

    Workload workload = KueueWorkloadFactory.buildWorkload(cluster);
    PodSet workerPodSet = workload.getSpec().getPodSets().get(1);
    assertEquals("worker", workerPodSet.getName());
    assertEquals(10, workerPodSet.getCount());
    assertEquals(2, workerPodSet.getMinCount());
  }

  @Test
  void testBuildWorkloadForSuspendedSparkCluster() {
    SparkCluster cluster = new SparkCluster();
    cluster.setMetadata(
        new ObjectMetaBuilder()
            .withName("test-cluster-suspended")
            .withNamespace("default")
            .withLabels(Map.of(Constants.LABEL_QUEUE_NAME, "suspended-queue"))
            .build());

    ClusterSpec clusterSpec =
        ClusterSpec.builder()
            .masterSpec(MasterSpec.builder().build())
            .workerSpec(WorkerSpec.builder().build())
            .build();
    clusterSpec.setSuspend(true);
    cluster.setSpec(clusterSpec);

    Workload workload = KueueWorkloadFactory.buildWorkload(cluster);
    assertFalse(workload.getSpec().getActive());
  }
}
