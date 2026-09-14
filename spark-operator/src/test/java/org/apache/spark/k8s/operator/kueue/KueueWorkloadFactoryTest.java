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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSetSpecBuilder;
import org.junit.jupiter.api.Test;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.kueue.v1beta2.PodSet;
import org.apache.spark.k8s.operator.kueue.v1beta2.Workload;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.spec.BaseApplicationTemplateSpec;
import org.apache.spark.k8s.operator.spec.ClusterSpec;
import org.apache.spark.k8s.operator.spec.ClusterTolerations;
import org.apache.spark.k8s.operator.spec.MasterSpec;
import org.apache.spark.k8s.operator.spec.RuntimeVersions;
import org.apache.spark.k8s.operator.spec.WorkerInstanceConfig;
import org.apache.spark.k8s.operator.spec.WorkerSpec;

class KueueWorkloadFactoryTest {

  @Test
  void testCalculateDriverMemoryMiB() {
    // 1g memory -> 1024MiB. Overhead factor 0.10 -> 102MiB -> min 384MiB.
    // Total = 1024 + 384 = 1408
    Map<String, String> conf = new HashMap<>();
    conf.put("spark.driver.memory", "1g");
    assertEquals(1408L, KueueWorkloadFactory.calculateDriverMemoryMiB(conf, false));

    // Explicit overhead 512m -> 1024 + 512 = 1536
    conf.put("spark.driver.memoryOverhead", "512m");
    assertEquals(1536L, KueueWorkloadFactory.calculateDriverMemoryMiB(conf, false));

    // Custom overhead factor 0.50 -> 1024 * 0.50 = 512 -> 1024 + 512 = 1536
    conf.remove("spark.driver.memoryOverhead");
    conf.put("spark.driver.memoryOverheadFactor", "0.50");
    assertEquals(1536L, KueueWorkloadFactory.calculateDriverMemoryMiB(conf, false));

    // Custom minimum overhead 1g -> 1024 + max(512, 1024) = 2048
    conf.put("spark.driver.minMemoryOverhead", "1g");
    assertEquals(2048L, KueueWorkloadFactory.calculateDriverMemoryMiB(conf, false));
  }

  @Test
  void testCalculateDriverMemoryMiBForNonJvmApp() {
    Map<String, String> conf = new HashMap<>();
    conf.put("spark.driver.memory", "8g");
    // Non-JVM: 8192 + 0.40 * 8192 = 8192 + 3276 = 11468
    assertEquals(11468L, KueueWorkloadFactory.calculateDriverMemoryMiB(conf, true));
    // JVM: 8192 + 0.10 * 8192 = 8192 + 819 = 9011
    assertEquals(9011L, KueueWorkloadFactory.calculateDriverMemoryMiB(conf, false));

    // The deprecated `spark.kubernetes.memoryOverheadFactor` 0.20 -> 8192 + 1638 = 9830
    conf.put("spark.kubernetes.memoryOverheadFactor", "0.20");
    assertEquals(9830L, KueueWorkloadFactory.calculateDriverMemoryMiB(conf, true));
    assertEquals(9830L, KueueWorkloadFactory.calculateDriverMemoryMiB(conf, false));
  }

  @Test
  void testCalculateExecutorMemoryMiB() {
    Map<String, String> conf = new HashMap<>();
    // 2048MiB. Overhead: max(204, 384) = 384. Total = 2432
    conf.put("spark.executor.memory", "2g");
    assertEquals(2432L, KueueWorkloadFactory.calculateExecutorMemoryMiB(conf, false, false));

    // PySpark memory is added only for Python applications
    conf.put("spark.executor.pyspark.memory", "1g");
    assertEquals(2432L, KueueWorkloadFactory.calculateExecutorMemoryMiB(conf, false, false));
    // Non-JVM overhead: max(819, 384) = 819. Total = 2048 + 819 + 1024 = 3891
    assertEquals(3891L, KueueWorkloadFactory.calculateExecutorMemoryMiB(conf, true, true));
  }

  @Test
  void testCalculateExecutorMemoryMiBWithOffHeap() {
    Map<String, String> conf = new HashMap<>();
    conf.put("spark.executor.memory", "4g");
    conf.put("spark.memory.offHeap.size", "4g");
    // Off-heap memory is ignored if disabled. 4096 + 409 = 4505
    assertEquals(4505L, KueueWorkloadFactory.calculateExecutorMemoryMiB(conf, false, false));

    // 4096 + 409 + 4096 = 8601
    conf.put("spark.memory.offHeap.enabled", "true");
    assertEquals(8601L, KueueWorkloadFactory.calculateExecutorMemoryMiB(conf, false, false));

    // `spark.memory.offHeap.size` is in bytes unless otherwise specified
    conf.put("spark.memory.offHeap.size", "4294967296");
    assertEquals(8601L, KueueWorkloadFactory.calculateExecutorMemoryMiB(conf, false, false));

    // Like Spark, off-heap memory must be at least 1MiB when enabled
    conf.remove("spark.memory.offHeap.size");
    assertThrows(
        IllegalArgumentException.class,
        () -> KueueWorkloadFactory.calculateExecutorMemoryMiB(conf, false, false));
  }

  @Test
  void testCalculateMemoryWithUnits() {
    // 2048 + 384 = 2432
    assertEquals(
        2432L,
        KueueWorkloadFactory.calculateExecutorMemoryMiB(
            Map.of("spark.executor.memory", "2Gi"), false, false));
    assertThrows(
        NumberFormatException.class,
        () ->
            KueueWorkloadFactory.calculateDriverMemoryMiB(
                Map.of("spark.driver.memory", "10zz"), false));
    assertThrows(
        NumberFormatException.class,
        () ->
            KueueWorkloadFactory.calculateExecutorMemoryMiB(
                Map.of("spark.executor.memory", "1.5g"), false, false));
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
    assertEquals("sparkapplication-spark-pi", workload.getMetadata().getName());
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
    assertEquals("spark-kubernetes-driver", driverContainer.getName());
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
    assertEquals("spark-kubernetes-executor", executorContainer.getName());
    assertEquals(new Quantity("4"), executorContainer.getResources().getRequests().get("cpu"));
    assertEquals(
        new Quantity("4505Mi"),
        executorContainer.getResources().getRequests().get("memory")); // 4096 + 409 = 4505
  }

  @Test
  void testBuildWorkloadDefaultExecutorInstances() {
    SparkApplication app = new SparkApplication();
    app.setMetadata(
        new ObjectMetaBuilder().withName("spark-default").withNamespace("default").build());

    Workload workload = KueueWorkloadFactory.buildWorkload(app);
    PodSet executorPodSet = workload.getSpec().getPodSets().get(1);
    assertEquals(2, executorPodSet.getCount());
    assertNull(executorPodSet.getMinCount());
  }

  @Test
  void testBuildWorkloadDriverOnly() {
    SparkApplication app = new SparkApplication();
    app.setMetadata(
        new ObjectMetaBuilder().withName("pi-with-one-pod").withNamespace("default").build());

    ApplicationSpec spec = new ApplicationSpec();
    spec.setSparkConf(
        Map.of(
            "spark.kubernetes.driver.master", "local[10]",
            "spark.kubernetes.driver.request.cores", "5",
            "spark.kubernetes.driver.limit.cores", "5"));
    app.setSpec(spec);

    Workload workload = KueueWorkloadFactory.buildWorkload(app);
    assertEquals(1, workload.getSpec().getPodSets().size());
    PodSet driverPodSet = workload.getSpec().getPodSets().get(0);
    assertEquals("driver", driverPodSet.getName());
    assertEquals(1, driverPodSet.getCount());
    Container driverContainer =
        driverPodSet.getTemplate().getSpec().getContainers().get(0);
    assertEquals(new Quantity("5"), driverContainer.getResources().getRequests().get("cpu"));
  }

  @Test
  void testBuildWorkloadWithPodTemplateFile() {
    SparkApplication app = new SparkApplication();
    app.setMetadata(
        new ObjectMetaBuilder().withName("spark-template").withNamespace("default").build());
    ApplicationSpec spec = new ApplicationSpec();
    app.setSpec(spec);

    // A pod template file is not supported without a pod template spec
    spec.setSparkConf(
        Map.of(Constants.EXECUTOR_SPARK_TEMPLATE_FILE_PROP_KEY, "s3a://bucket/executor.yaml"));
    assertThrows(
        UnsupportedOperationException.class, () -> KueueWorkloadFactory.buildWorkload(app));

    // The executor template file is ignored in driver-only mode
    spec.setSparkConf(
        Map.of(
            Constants.EXECUTOR_SPARK_TEMPLATE_FILE_PROP_KEY, "s3a://bucket/executor.yaml",
            "spark.kubernetes.driver.master", "local[2]"));
    assertEquals(1, KueueWorkloadFactory.buildWorkload(app).getSpec().getPodSets().size());

    // The pod template spec takes precedence over the pod template file
    spec.setSparkConf(
        Map.of(Constants.DRIVER_SPARK_TEMPLATE_FILE_PROP_KEY, "s3a://bucket/driver.yaml"));
    assertThrows(
        UnsupportedOperationException.class, () -> KueueWorkloadFactory.buildWorkload(app));
    spec.setDriverSpec(new BaseApplicationTemplateSpec(new PodTemplateSpecBuilder().build()));
    assertEquals(2, KueueWorkloadFactory.buildWorkload(app).getSpec().getPodSets().size());
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
            "spark.dynamicAllocation.maxExecutors", "10"));
    app.setSpec(spec);

    assertThrows(
        UnsupportedOperationException.class, () -> KueueWorkloadFactory.buildWorkload(app));
  }

  @Test
  void testBuildWorkloadForPythonApp() {
    SparkApplication app = new SparkApplication();
    app.setMetadata(
        new ObjectMetaBuilder().withName("pi-python").withNamespace("default").build());

    ApplicationSpec spec = new ApplicationSpec();
    spec.setPyFiles("local:///opt/spark/examples/src/main/python/pi.py");
    spec.setSparkConf(
        Map.of(
            "spark.driver.memory", "8g",
            "spark.executor.memory", "8g",
            "spark.executor.pyspark.memory", "1g"));
    app.setSpec(spec);

    Workload workload = KueueWorkloadFactory.buildWorkload(app);
    Container driverContainer =
        workload.getSpec().getPodSets().get(0).getTemplate().getSpec().getContainers().get(0);
    assertEquals(
        new Quantity("11468Mi"),
        driverContainer.getResources().getRequests().get("memory")); // 8192 + 3276 = 11468
    Container executorContainer =
        workload.getSpec().getPodSets().get(1).getTemplate().getSpec().getContainers().get(0);
    assertEquals(
        new Quantity("12492Mi"),
        executorContainer.getResources().getRequests().get("memory")); // 8192 + 3276 + 1024
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
                    .withName("sidecar")
                    .withResources(
                        new ResourceRequirementsBuilder()
                            .withRequests(Map.of("cpu", new Quantity("100m")))
                            .build())
                    .build(),
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
            Constants.DRIVER_SPARK_CONTAINER_PROP_KEY, "custom-driver",
            "spark.driver.resource.gpu.amount", "1",
            "spark.driver.resource.gpu.vendor", "nvidia.com",
            "spark.executor.resource.gpu.amount", "2",
            "spark.executor.resource.gpu.vendor", "nvidia.com",
            "spark.executor.resource.fpga.amount", "3",
            "spark.executor.resource.fpga.vendor", "xilinx.com"));
    app.setSpec(spec);

    Workload workload = KueueWorkloadFactory.buildWorkload(app);
    PodSet driverPodSet = workload.getSpec().getPodSets().get(0);
    Container sidecarContainer =
        driverPodSet.getTemplate().getSpec().getContainers().get(0);
    assertEquals("sidecar", sidecarContainer.getName());
    assertEquals(
        Map.of("cpu", new Quantity("100m")), sidecarContainer.getResources().getRequests());

    Container driverContainer =
        driverPodSet.getTemplate().getSpec().getContainers().get(1);
    assertEquals("custom-driver", driverContainer.getName());
    // Spark overwrites the cpu request of the pod template with `spark.driver.cores`
    assertEquals(new Quantity("1"), driverContainer.getResources().getRequests().get("cpu"));
    assertEquals(
        new Quantity("1"),
        driverContainer.getResources().getRequests().get("nvidia.com/gpu"));
    assertEquals(
        new Quantity("1"),
        driverContainer.getResources().getLimits().get("nvidia.com/gpu"));
    assertFalse(driverContainer.getResources().getRequests().containsKey("nvidia.com"));

    PodSet executorPodSet = workload.getSpec().getPodSets().get(1);
    Container executorContainer =
        executorPodSet.getTemplate().getSpec().getContainers().get(0);
    assertEquals(
        new Quantity("2"),
        executorContainer.getResources().getRequests().get("nvidia.com/gpu"));
    assertEquals(
        new Quantity("2"),
        executorContainer.getResources().getLimits().get("nvidia.com/gpu"));
    assertEquals(
        new Quantity("3"),
        executorContainer.getResources().getRequests().get("xilinx.com/fpga"));
    assertEquals(
        new Quantity("3"),
        executorContainer.getResources().getLimits().get("xilinx.com/fpga"));
  }

  @Test
  void testBuildWorkloadWithExecutorTemplateSpecAndRequestCores() {
    SparkApplication app = new SparkApplication();
    app.setMetadata(
        new ObjectMetaBuilder().withName("spark-sidecar").withNamespace("default").build());

    PodTemplateSpec executorTemplate =
        new PodTemplateSpecBuilder()
            .withNewSpec()
            .withContainers(
                new ContainerBuilder()
                    .withName("sidecar")
                    .withResources(
                        new ResourceRequirementsBuilder()
                            .withRequests(Map.of("cpu", new Quantity("100m")))
                            .build())
                    .build(),
                new ContainerBuilder().withName("custom-executor").build())
            .endSpec()
            .build();

    ApplicationSpec spec = new ApplicationSpec();
    spec.setExecutorSpec(new BaseApplicationTemplateSpec(executorTemplate));
    spec.setSparkConf(
        Map.of(
            Constants.EXECUTOR_SPARK_CONTAINER_PROP_KEY, "custom-executor",
            "spark.kubernetes.driver.request.cores", "500m",
            "spark.driver.cores", "2",
            "spark.kubernetes.executor.request.cores", "1500m",
            "spark.executor.cores", "4"));
    app.setSpec(spec);

    Workload workload = KueueWorkloadFactory.buildWorkload(app);
    // `spark.kubernetes.{driver,executor}.request.cores` takes precedence over cores
    Container driverContainer =
        workload.getSpec().getPodSets().get(0).getTemplate().getSpec().getContainers().get(0);
    assertEquals(new Quantity("500m"), driverContainer.getResources().getRequests().get("cpu"));

    PodSet executorPodSet = workload.getSpec().getPodSets().get(1);
    Container sidecarContainer =
        executorPodSet.getTemplate().getSpec().getContainers().get(0);
    assertEquals("sidecar", sidecarContainer.getName());
    assertEquals(
        Map.of("cpu", new Quantity("100m")), sidecarContainer.getResources().getRequests());

    Container executorContainer =
        executorPodSet.getTemplate().getSpec().getContainers().get(1);
    assertEquals("custom-executor", executorContainer.getName());
    assertEquals(
        new Quantity("1500m"), executorContainer.getResources().getRequests().get("cpu"));
    assertEquals(
        new Quantity("1408Mi"), executorContainer.getResources().getRequests().get("memory"));
  }

  @Test
  void testBuildWorkloadWithNodeSelector() {
    SparkApplication app = new SparkApplication();
    app.setMetadata(
        new ObjectMetaBuilder().withName("spark-gpu-node").withNamespace("default").build());

    PodTemplateSpec driverTemplate =
        new PodTemplateSpecBuilder()
            .withNewSpec()
            .withNodeSelector(Map.of("topology.kubernetes.io/zone", "us-west-2a"))
            .endSpec()
            .build();

    ApplicationSpec spec = new ApplicationSpec();
    spec.setDriverSpec(new BaseApplicationTemplateSpec(driverTemplate));
    spec.setSparkConf(
        Map.of(
            "spark.kubernetes.node.selector.karpenter.sh/nodepool", "gpu",
            "spark.kubernetes.node.selector.node.kubernetes.io/instance-type", "g5.xlarge",
            "spark.kubernetes.driver.node.selector.node.kubernetes.io/instance-type",
                "m5.xlarge",
            "spark.kubernetes.executor.node.selector.node.kubernetes.io/instance-type",
                "p4d.24xlarge"));
    app.setSpec(spec);

    Workload workload = KueueWorkloadFactory.buildWorkload(app);
    // The role-specific selector wins and the pod template selector is kept
    assertEquals(
        Map.of(
            "topology.kubernetes.io/zone", "us-west-2a",
            "karpenter.sh/nodepool", "gpu",
            "node.kubernetes.io/instance-type", "m5.xlarge"),
        workload.getSpec().getPodSets().get(0).getTemplate().getSpec().getNodeSelector());
    assertEquals(
        Map.of(
            "karpenter.sh/nodepool", "gpu",
            "node.kubernetes.io/instance-type", "p4d.24xlarge"),
        workload.getSpec().getPodSets().get(1).getTemplate().getSpec().getNodeSelector());
  }

  @Test
  void testBuildWorkloadWithMalformedNumbers() {
    SparkApplication app = new SparkApplication();
    app.setMetadata(
        new ObjectMetaBuilder().withName("spark-invalid").withNamespace("default").build());
    ApplicationSpec spec = new ApplicationSpec();
    app.setSpec(spec);

    spec.setSparkConf(Map.of("spark.executor.instances", "not-a-number"));
    assertThrows(NumberFormatException.class, () -> KueueWorkloadFactory.buildWorkload(app));

    spec.setSparkConf(Map.of("spark.driver.memoryOverheadFactor", "not-a-number"));
    assertThrows(NumberFormatException.class, () -> KueueWorkloadFactory.buildWorkload(app));
  }

  @Test
  void testBuildWorkloadWithGpuWithoutVendor() {
    SparkApplication app = new SparkApplication();
    app.setMetadata(
        new ObjectMetaBuilder().withName("spark-gpu").withNamespace("default").build());
    ApplicationSpec spec = new ApplicationSpec();
    spec.setSparkConf(Map.of("spark.executor.resource.gpu.amount", "1"));
    app.setSpec(spec);

    assertThrows(
        IllegalArgumentException.class, () -> KueueWorkloadFactory.buildWorkload(app));
  }

  @Test
  void testBuildWorkloadWithZeroResourceAmount() {
    SparkApplication app = new SparkApplication();
    app.setMetadata(
        new ObjectMetaBuilder().withName("spark-no-gpu").withNamespace("default").build());
    ApplicationSpec spec = new ApplicationSpec();
    // Like Spark, a zero amount is ignored before the vendor check
    spec.setSparkConf(
        Map.of(
            "spark.driver.resource.gpu.amount", "0",
            "spark.executor.resource.gpu.amount", "0",
            "spark.executor.resource.gpu.vendor", "nvidia.com"));
    app.setSpec(spec);

    Workload workload = KueueWorkloadFactory.buildWorkload(app);
    for (PodSet podSet : workload.getSpec().getPodSets()) {
      Container container = podSet.getTemplate().getSpec().getContainers().get(0);
      assertEquals(Set.of("cpu", "memory"), container.getResources().getRequests().keySet());
      assertTrue(container.getResources().getLimits().isEmpty());
    }
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

  private static SparkCluster buildSparkCluster(
      final String name, final int initWorkers, final int minWorkers, final int maxWorkers) {
    SparkCluster cluster = new SparkCluster();
    cluster.setMetadata(
        new ObjectMetaBuilder()
            .withName(name)
            .withNamespace("spark-ns")
            .withLabels(Map.of(Constants.LABEL_QUEUE_NAME, "cluster-queue"))
            .build());
    cluster.setSpec(
        ClusterSpec.builder()
            .runtimeVersions(RuntimeVersions.builder().sparkVersion("4.2.0").build())
            .clusterTolerations(
                ClusterTolerations.builder()
                    .instanceConfig(
                        WorkerInstanceConfig.builder()
                            .initWorkers(initWorkers)
                            .minWorkers(minWorkers)
                            .maxWorkers(maxWorkers)
                            .build())
                    .build())
            .build());
    return cluster;
  }

  @Test
  void testBuildWorkloadForSparkCluster() {
    SparkCluster cluster = buildSparkCluster("test-cluster", 3, 3, 3);

    PodTemplateSpec masterTemplate =
        new PodTemplateSpecBuilder()
            .withNewSpec()
            .addNewContainer()
            .withName("sidecar")
            .endContainer()
            .addNewContainer()
            .withName("master")
            .withResources(
                new ResourceRequirementsBuilder()
                    .withRequests(Map.of("cpu", new Quantity("2")))
                    .build())
            .endContainer()
            .endSpec()
            .build();
    cluster
        .getSpec()
        .setMasterSpec(
            MasterSpec.builder()
                .statefulSetSpec(
                    new StatefulSetSpecBuilder()
                        .withReplicas(3)
                        .withTemplate(masterTemplate)
                        .build())
                .build());
    cluster.getSpec().setWorkerSpec(WorkerSpec.builder().build());

    assertTrue(KueueWorkloadFactory.hasQueueName(cluster));
    assertEquals("cluster-queue", KueueWorkloadFactory.getQueueName(cluster));

    Workload workload = KueueWorkloadFactory.buildWorkload(cluster);
    assertNotNull(workload);
    assertEquals("sparkcluster-test-cluster", workload.getMetadata().getName());
    assertEquals("spark-ns", workload.getMetadata().getNamespace());
    assertEquals("cluster-queue", workload.getSpec().getQueueName());
    assertTrue(workload.getSpec().getActive());
    assertEquals(
        "test-cluster",
        workload.getMetadata().getLabels().get(Constants.LABEL_SPARK_CLUSTER_NAME));
    assertEquals(2, workload.getSpec().getPodSets().size());

    // The operator always creates a single master regardless of the StatefulSet replicas
    PodSet masterPodSet = workload.getSpec().getPodSets().get(0);
    assertEquals("master", masterPodSet.getName());
    assertEquals(1, masterPodSet.getCount());
    Container masterContainer =
        masterPodSet.getTemplate().getSpec().getContainers().stream()
            .filter(c -> "master".equals(c.getName()))
            .findFirst()
            .orElseThrow();
    assertEquals(new Quantity("2"), masterContainer.getResources().getRequests().get("cpu"));

    // The number of workers comes from `initWorkers`
    PodSet workerPodSet = workload.getSpec().getPodSets().get(1);
    assertEquals("worker", workerPodSet.getName());
    assertEquals(3, workerPodSet.getCount());
    assertNull(workerPodSet.getMinCount());
    assertEquals(
        "worker", workerPodSet.getTemplate().getSpec().getContainers().get(0).getName());
  }

  @Test
  void testBuildWorkloadForSparkClusterWithHPA() {
    SparkCluster cluster = buildSparkCluster("test-cluster-hpa", 3, 1, 3);

    assertThrows(
        UnsupportedOperationException.class, () -> KueueWorkloadFactory.buildWorkload(cluster));
  }

  @Test
  void testBuildWorkloadForSuspendedSparkCluster() {
    SparkCluster cluster = buildSparkCluster("test-cluster-suspended", 1, 1, 1);
    cluster.getSpec().setSuspend(true);

    Workload workload = KueueWorkloadFactory.buildWorkload(cluster);
    assertFalse(workload.getSpec().getActive());
  }
}
