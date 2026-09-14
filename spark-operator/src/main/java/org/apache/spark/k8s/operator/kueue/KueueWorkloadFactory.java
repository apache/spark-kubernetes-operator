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
import java.util.Locale;
import java.util.Map;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.SparkClusterResourceSpec;
import org.apache.spark.k8s.operator.SparkClusterSubmissionWorker;
import org.apache.spark.k8s.operator.kueue.v1beta1.PodSet;
import org.apache.spark.k8s.operator.kueue.v1beta1.Workload;
import org.apache.spark.k8s.operator.kueue.v1beta1.WorkloadSpec;
import org.apache.spark.k8s.operator.reconciler.SparkClusterResourceSpecFactory;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.spec.ClusterSpec;
import org.apache.spark.k8s.operator.utils.ModelUtils;
import org.apache.spark.k8s.operator.utils.ReconcilerUtils;
import org.apache.spark.k8s.operator.utils.StringUtils;
import org.apache.spark.network.util.JavaUtils;

/**
 * Factory for creating Kueue Workload resources from Spark custom resources.
 * This factory supports both {@link SparkApplication} (driver and executor pod sets)
 * and {@link SparkCluster} (master and worker pod sets).
 */
@SuppressWarnings("PMD.GodClass")
public final class KueueWorkloadFactory {

  public static final String PODSET_DRIVER = "driver";
  public static final String PODSET_EXECUTOR = "executor";
  public static final String PODSET_MASTER = "master";
  public static final String PODSET_WORKER = "worker";

  public static final String DEFAULT_CORES = "1";
  public static final String DEFAULT_MEMORY = "1g";
  public static final String DEFAULT_MIN_MEMORY_OVERHEAD = "384m";
  public static final double DEFAULT_MEMORY_OVERHEAD_FACTOR = 0.10;
  public static final double NON_JVM_MEMORY_OVERHEAD_FACTOR = 0.40;
  public static final int DEFAULT_EXECUTOR_INSTANCES = 2;

  private KueueWorkloadFactory() {}

  /**
   * Builds a Kueue Workload from a SparkApplication resource.
   *
   * @param app The SparkApplication.
   * @return The constructed Kueue Workload.
   */
  public static Workload buildWorkload(final SparkApplication app) {
    String queueName = getQueueName(app);
    ApplicationSpec appSpec = app.getSpec();
    Map<String, String> sparkConf =
        appSpec != null && appSpec.getSparkConf() != null
            ? appSpec.getSparkConf()
            : Map.of();

    PodSet driverPodSet = buildDriverPodSet(app, sparkConf);
    PodSet executorPodSet = buildExecutorPodSet(app, sparkConf);

    List<PodSet> podSets = new ArrayList<>();
    podSets.add(driverPodSet);
    podSets.add(executorPodSet);

    boolean active = appSpec == null || !appSpec.isSuspend();

    Map<String, String> labels = new HashMap<>();
    if (app.getMetadata().getLabels() != null) {
      labels.putAll(app.getMetadata().getLabels());
    }
    labels.put(Constants.LABEL_SPARK_APPLICATION_NAME, app.getMetadata().getName());
    if (StringUtils.isNotEmpty(queueName)) {
      labels.put(Constants.LABEL_QUEUE_NAME, queueName);
    }

    OwnerReference ownerReference = ModelUtils.buildOwnerReferenceTo(app);
    ownerReference.setController(true);

    Workload workload = new Workload();
    workload.setMetadata(
        new ObjectMetaBuilder()
            .withName(getWorkloadName(app))
            .withNamespace(app.getMetadata().getNamespace())
            .withLabels(labels)
            .withOwnerReferences(ownerReference)
            .build());

    workload.setSpec(
        WorkloadSpec.builder()
            .queueName(queueName)
            .active(active)
            .podSets(podSets)
            .build());

    return workload;
  }

  /**
   * Builds a Kueue Workload from a SparkCluster resource.
   *
   * @param cluster The SparkCluster.
   * @return The constructed Kueue Workload.
   */
  public static Workload buildWorkload(final SparkCluster cluster) {
    String queueName = getQueueName(cluster);
    ClusterSpec clusterSpec = cluster.getSpec();

    // Use the same StatefulSets which the operator creates for the cluster.
    SparkClusterResourceSpec resourceSpec =
        SparkClusterResourceSpecFactory.buildResourceSpec(
            cluster, new SparkClusterSubmissionWorker());
    if (resourceSpec.getHorizontalPodAutoscaler().isPresent()) {
      throw new UnsupportedOperationException(
          "Kueue does not support SparkCluster with HorizontalPodAutoscaler "
              + "(minWorkers < maxWorkers) yet.");
    }

    List<PodSet> podSets = new ArrayList<>();
    podSets.add(buildPodSet(PODSET_MASTER, resourceSpec.getMasterStatefulSet()));
    podSets.add(buildPodSet(PODSET_WORKER, resourceSpec.getWorkerStatefulSet()));

    boolean active = clusterSpec == null || !clusterSpec.isSuspend();

    Map<String, String> labels = new HashMap<>();
    if (cluster.getMetadata().getLabels() != null) {
      labels.putAll(cluster.getMetadata().getLabels());
    }
    labels.put(Constants.LABEL_SPARK_CLUSTER_NAME, cluster.getMetadata().getName());
    if (StringUtils.isNotEmpty(queueName)) {
      labels.put(Constants.LABEL_QUEUE_NAME, queueName);
    }

    OwnerReference ownerReference = ModelUtils.buildOwnerReferenceTo(cluster);
    ownerReference.setController(true);

    Workload workload = new Workload();
    workload.setMetadata(
        new ObjectMetaBuilder()
            .withName(getWorkloadName(cluster))
            .withNamespace(cluster.getMetadata().getNamespace())
            .withLabels(labels)
            .withOwnerReferences(ownerReference)
            .build());

    workload.setSpec(
        WorkloadSpec.builder()
            .queueName(queueName)
            .active(active)
            .podSets(podSets)
            .build());

    return workload;
  }

  private static PodSet buildPodSet(final String name, final StatefulSet statefulSet) {
    return PodSet.builder()
        .name(name)
        .count(statefulSet.getSpec().getReplicas())
        .template(statefulSet.getSpec().getTemplate())
        .build();
  }

  /**
   * Returns the Workload name prefixed with the lower-cased kind of the owner resource, like Kueue
   * built-in integrations, to avoid name collisions between SparkApplication and SparkCluster.
   */
  private static String getWorkloadName(final HasMetadata resource) {
    return resource.getKind().toLowerCase(Locale.ROOT) + "-" + resource.getMetadata().getName();
  }

  /**
   * Extracts the Kueue queue name from the resource metadata labels.
   *
   * @param resource The Kubernetes resource (e.g. SparkApplication or SparkCluster).
   * @return The queue name, or null if not found.
   */
  public static String getQueueName(final HasMetadata resource) {
    if (resource != null
        && resource.getMetadata() != null
        && resource.getMetadata().getLabels() != null) {
      String queue = resource.getMetadata().getLabels().get(Constants.LABEL_QUEUE_NAME);
      if (StringUtils.isNotEmpty(queue)) {
        return queue;
      }
    }
    return null;
  }

  /**
   * Checks whether the resource is configured to use Kueue.
   *
   * @param resource The Kubernetes resource (SparkApplication or SparkCluster).
   * @return true if a Kueue queue name is specified.
   */
  public static boolean hasQueueName(final HasMetadata resource) {
    return StringUtils.isNotEmpty(getQueueName(resource));
  }

  /**
   * Builds the driver PodSet.
   */
  public static PodSet buildDriverPodSet(
      final SparkApplication app, final Map<String, String> sparkConf) {
    PodTemplateSpec templateSpec = null;
    if (app.getSpec() != null
        && app.getSpec().getDriverSpec() != null
        && app.getSpec().getDriverSpec().getPodTemplateSpec() != null) {
      templateSpec = ReconcilerUtils.clone(app.getSpec().getDriverSpec().getPodTemplateSpec());
    }
    if (templateSpec == null) {
      templateSpec = new PodTemplateSpecBuilder().build();
    }
    ensurePodSpec(templateSpec);

    String cpu =
        sparkConf.getOrDefault(
            "spark.kubernetes.driver.request.cores",
            sparkConf.getOrDefault("spark.driver.cores", DEFAULT_CORES));
    long memoryMiB = calculateDriverMemoryMiB(sparkConf, isNonJvmApp(app.getSpec()));
    String gpuAmount = sparkConf.get("spark.driver.resource.gpu.amount");
    String gpuVendor = sparkConf.get("spark.driver.resource.gpu.vendor");

    decorateTemplateResources(
        templateSpec,
        sparkConf.get(Constants.DRIVER_SPARK_CONTAINER_PROP_KEY),
        "spark-driver",
        cpu,
        memoryMiB,
        gpuAmount,
        gpuVendor);

    return PodSet.builder()
        .name(PODSET_DRIVER)
        .count(1)
        .template(templateSpec)
        .build();
  }

  /**
   * Builds the executor PodSet.
   */
  public static PodSet buildExecutorPodSet(
      final SparkApplication app, final Map<String, String> sparkConf) {
    if ("true".equalsIgnoreCase(sparkConf.get("spark.dynamicAllocation.enabled"))) {
      throw new UnsupportedOperationException(
          "Kueue does not support SparkApplication with dynamic allocation "
              + "(spark.dynamicAllocation.enabled=true) yet.");
    }
    PodTemplateSpec templateSpec = null;
    if (app.getSpec() != null
        && app.getSpec().getExecutorSpec() != null
        && app.getSpec().getExecutorSpec().getPodTemplateSpec() != null) {
      templateSpec = ReconcilerUtils.clone(app.getSpec().getExecutorSpec().getPodTemplateSpec());
    }
    if (templateSpec == null) {
      templateSpec = new PodTemplateSpecBuilder().build();
    }
    ensurePodSpec(templateSpec);

    String cpu =
        sparkConf.getOrDefault(
            "spark.kubernetes.executor.request.cores",
            sparkConf.getOrDefault("spark.executor.cores", DEFAULT_CORES));
    long memoryMiB =
        calculateExecutorMemoryMiB(
            sparkConf, isNonJvmApp(app.getSpec()), isPythonApp(app.getSpec()));
    String gpuAmount = sparkConf.get("spark.executor.resource.gpu.amount");
    String gpuVendor = sparkConf.get("spark.executor.resource.gpu.vendor");

    decorateTemplateResources(
        templateSpec,
        sparkConf.get("spark.kubernetes.executor.podTemplateContainerName"),
        "spark-executor",
        cpu,
        memoryMiB,
        gpuAmount,
        gpuVendor);

    return PodSet.builder()
        .name(PODSET_EXECUTOR)
        .count(parseInt(sparkConf.get("spark.executor.instances"), DEFAULT_EXECUTOR_INSTANCES))
        .template(templateSpec)
        .build();
  }

  /**
   * Calculates total driver memory in MiB including overhead.
   */
  public static long calculateDriverMemoryMiB(
      final Map<String, String> sparkConf, final boolean isNonJvm) {
    long memMiB =
        JavaUtils.byteStringAsMb(sparkConf.getOrDefault("spark.driver.memory", DEFAULT_MEMORY));
    return memMiB + calculateMemoryOverheadMiB(sparkConf, "spark.driver", memMiB, isNonJvm);
  }

  /**
   * Calculates total executor memory in MiB including overhead, off-heap memory and PySpark
   * memory.
   */
  public static long calculateExecutorMemoryMiB(
      final Map<String, String> sparkConf, final boolean isNonJvm, final boolean isPython) {
    long memMiB =
        JavaUtils.byteStringAsMb(sparkConf.getOrDefault("spark.executor.memory", DEFAULT_MEMORY));
    long total =
        memMiB + calculateMemoryOverheadMiB(sparkConf, "spark.executor", memMiB, isNonJvm);
    if ("true".equalsIgnoreCase(sparkConf.get("spark.memory.offHeap.enabled"))) {
      // `spark.memory.offHeap.size` is in bytes unless otherwise specified.
      total +=
          JavaUtils.byteStringAsBytes(sparkConf.getOrDefault("spark.memory.offHeap.size", "0"))
              / 1024
              / 1024;
    }
    if (isPython && sparkConf.containsKey("spark.executor.pyspark.memory")) {
      total += JavaUtils.byteStringAsMb(sparkConf.get("spark.executor.pyspark.memory"));
    }
    return total;
  }

  private static long calculateMemoryOverheadMiB(
      final Map<String, String> sparkConf,
      final String prefix,
      final long memMiB,
      final boolean isNonJvm) {
    String overhead = sparkConf.get(prefix + ".memoryOverhead");
    if (StringUtils.isNotEmpty(overhead)) {
      return JavaUtils.byteStringAsMb(overhead);
    }
    // Like Spark's BasicDriverFeatureStep, the deprecated `spark.kubernetes.memoryOverheadFactor`
    // or the default factor (0.4 for non-JVM applications) is used if not set explicitly.
    double defaultFactor =
        parseDouble(
            sparkConf.get("spark.kubernetes.memoryOverheadFactor"),
            isNonJvm ? NON_JVM_MEMORY_OVERHEAD_FACTOR : DEFAULT_MEMORY_OVERHEAD_FACTOR);
    double factor = parseDouble(sparkConf.get(prefix + ".memoryOverheadFactor"), defaultFactor);
    long minOverheadMiB =
        JavaUtils.byteStringAsMb(
            sparkConf.getOrDefault(prefix + ".minMemoryOverhead", DEFAULT_MIN_MEMORY_OVERHEAD));
    return Math.max((int) (factor * memMiB), minOverheadMiB);
  }

  /** Follows the main application resource selection of SparkAppSubmissionWorker. */
  private static boolean isPythonApp(final ApplicationSpec spec) {
    return StringUtils.isEmpty(spec.getJars())
        && ("org.apache.spark.deploy.PythonRunner".equals(spec.getMainClass())
            || StringUtils.isNotEmpty(spec.getPyFiles()));
  }

  private static boolean isNonJvmApp(final ApplicationSpec spec) {
    return isPythonApp(spec)
        || (StringUtils.isEmpty(spec.getJars()) && StringUtils.isNotEmpty(spec.getSparkRFiles()));
  }

  private static void ensurePodSpec(final PodTemplateSpec templateSpec) {
    if (templateSpec.getSpec() == null) {
      templateSpec.setSpec(new PodSpecBuilder().build());
    }
    if (templateSpec.getSpec().getContainers() == null) {
      templateSpec.getSpec().setContainers(new ArrayList<>());
    }
  }

  private static void decorateTemplateResources(
      final PodTemplateSpec templateSpec,
      final String containerName,
      final String defaultContainerName,
      final String cpu,
      final long memoryMiB,
      final String gpuAmount,
      final String gpuVendor) {
    PodSpec podSpec = templateSpec.getSpec();
    Container container;
    if (podSpec.getContainers().isEmpty()) {
      container = new ContainerBuilder().withName(defaultContainerName).build();
      podSpec.getContainers().add(container);
    } else {
      // Like Spark's KubernetesUtils.selectSparkContainer, select the container by name and
      // fall back to the first container.
      container =
          podSpec.getContainers().stream()
              .filter(c -> containerName != null && containerName.equals(c.getName()))
              .findFirst()
              .orElse(podSpec.getContainers().get(0));
    }

    ResourceRequirements resources = container.getResources();
    if (resources == null) {
      resources = new ResourceRequirementsBuilder().build();
      container.setResources(resources);
    }

    Map<String, Quantity> requests = resources.getRequests();
    if (requests == null) {
      requests = new HashMap<>();
      resources.setRequests(requests);
    }

    // Like Spark, overwrite the requests of the pod template.
    requests.put("cpu", new Quantity(cpu));
    requests.put("memory", new Quantity(memoryMiB + "Mi"));

    if (StringUtils.isNotEmpty(gpuAmount)) {
      if (StringUtils.isEmpty(gpuVendor)) {
        throw new IllegalArgumentException(
            "Resource: gpu was requested, but vendor was not specified.");
      }
      // Like Spark's KubernetesConf.buildKubernetesResourceName, e.g., `nvidia.com/gpu`.
      String gpuResourceName = gpuVendor + "/gpu";
      requests.put(gpuResourceName, new Quantity(gpuAmount));
      Map<String, Quantity> limits = resources.getLimits();
      if (limits == null) {
        limits = new HashMap<>();
        resources.setLimits(limits);
      }
      limits.put(gpuResourceName, new Quantity(gpuAmount));
    }
  }

  private static int parseInt(final String str, final int defaultValue) {
    if (StringUtils.isEmpty(str)) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(str.trim());
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private static double parseDouble(final String str, final double defaultValue) {
    if (StringUtils.isEmpty(str)) {
      return defaultValue;
    }
    try {
      return Double.parseDouble(str.trim());
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }
}
