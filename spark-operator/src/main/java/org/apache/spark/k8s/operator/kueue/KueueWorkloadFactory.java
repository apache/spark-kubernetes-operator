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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import io.fabric8.kubernetes.api.model.apps.StatefulSetSpec;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.kueue.v1beta1.PodSet;
import org.apache.spark.k8s.operator.kueue.v1beta1.Workload;
import org.apache.spark.k8s.operator.kueue.v1beta1.WorkloadSpec;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.spec.ClusterSpec;
import org.apache.spark.k8s.operator.spec.MasterSpec;
import org.apache.spark.k8s.operator.spec.WorkerSpec;
import org.apache.spark.k8s.operator.utils.ModelUtils;
import org.apache.spark.k8s.operator.utils.ReconcilerUtils;
import org.apache.spark.k8s.operator.utils.StringUtils;

/**
 * Factory for creating Kueue Workload resources from Spark custom resources.
 * This factory supports both {@link SparkApplication} (driver and executor pod sets)
 * and {@link SparkCluster} (master and worker pod sets).
 */
@Slf4j
@SuppressWarnings("PMD.GodClass")
public final class KueueWorkloadFactory {

  public static final String PODSET_DRIVER = "driver";
  public static final String PODSET_EXECUTOR = "executor";
  public static final String PODSET_MASTER = "master";
  public static final String PODSET_WORKER = "worker";

  public static final String DEFAULT_CORES = "1";
  public static final String DEFAULT_MEMORY = "1g";
  public static final double DEFAULT_MEMORY_OVERHEAD_FACTOR = 0.10;
  public static final long MIN_MEMORY_OVERHEAD_MIB = 384L;
  public static final String DEFAULT_GPU_VENDOR = "nvidia.com/gpu";

  private static final Pattern MEMORY_PATTERN =
      Pattern.compile("^(\\d+)(?:[\\.,](\\d+))?\\s*([a-zA-Z]*)$");

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
            .withName(app.getMetadata().getName())
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

    List<PodSet> podSets = new ArrayList<>();
    PodSet masterPodSet = buildMasterPodSet(cluster);
    if (masterPodSet != null) {
      podSets.add(masterPodSet);
    }
    PodSet workerPodSet = buildWorkerPodSet(cluster);
    if (workerPodSet != null) {
      podSets.add(workerPodSet);
    }

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
            .withName(cluster.getMetadata().getName())
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

  /**
   * Builds the master PodSet for a SparkCluster.
   *
   * @param cluster The SparkCluster.
   * @return The master PodSet.
   */
  public static PodSet buildMasterPodSet(final SparkCluster cluster) {
    if (cluster == null
        || cluster.getSpec() == null
        || cluster.getSpec().getMasterSpec() == null) {
      return null;
    }
    MasterSpec masterSpec = cluster.getSpec().getMasterSpec();
    StatefulSetSpec ssSpec = masterSpec.getStatefulSetSpec();
    int count = ssSpec != null && ssSpec.getReplicas() != null ? ssSpec.getReplicas() : 1;
    PodTemplateSpec template =
        ssSpec != null && ssSpec.getTemplate() != null
            ? ReconcilerUtils.clone(ssSpec.getTemplate())
            : new PodTemplateSpec();

    return PodSet.builder()
        .name(PODSET_MASTER)
        .count(count)
        .template(template)
        .build();
  }

  /**
   * Builds the worker PodSet for a SparkCluster.
   *
   * @param cluster The SparkCluster.
   * @return The worker PodSet.
   */
  public static PodSet buildWorkerPodSet(final SparkCluster cluster) {
    if (cluster == null
        || cluster.getSpec() == null
        || cluster.getSpec().getWorkerSpec() == null) {
      return null;
    }
    WorkerSpec workerSpec = cluster.getSpec().getWorkerSpec();
    StatefulSetSpec ssSpec = workerSpec.getStatefulSetSpec();
    int count = ssSpec != null && ssSpec.getReplicas() != null ? ssSpec.getReplicas() : 1;
    Integer minCount = null;

    if (workerSpec.getHorizontalPodAutoscalerSpec() != null) {
      minCount = workerSpec.getHorizontalPodAutoscalerSpec().getMinReplicas();
      count = workerSpec.getHorizontalPodAutoscalerSpec().getMaxReplicas();
    }

    PodTemplateSpec template =
        ssSpec != null && ssSpec.getTemplate() != null
            ? ReconcilerUtils.clone(ssSpec.getTemplate())
            : new PodTemplateSpec();

    PodSet.PodSetBuilder builder =
        PodSet.builder()
            .name(PODSET_WORKER)
            .count(count)
            .template(template);
    if (minCount != null) {
      builder.minCount(minCount);
    }
    return builder.build();
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
   * Extracts the Kueue queue name from the SparkApplication metadata.
   *
   * @param app The SparkApplication.
   * @return The queue name, or null if not found.
   */
  public static String getQueueName(final SparkApplication app) {
    return getQueueName((HasMetadata) app);
  }

  /**
   * Extracts the Kueue queue name from the SparkCluster metadata.
   *
   * @param cluster The SparkCluster.
   * @return The queue name, or null if not found.
   */
  public static String getQueueName(final SparkCluster cluster) {
    return getQueueName((HasMetadata) cluster);
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
   * Checks whether the SparkApplication is configured to use Kueue.
   *
   * @param app The SparkApplication.
   * @return true if a Kueue queue name is specified.
   */
  public static boolean hasQueueName(final SparkApplication app) {
    return hasQueueName((HasMetadata) app);
  }

  /**
   * Checks whether the SparkCluster is configured to use Kueue.
   *
   * @param cluster The SparkCluster.
   * @return true if a Kueue queue name is specified.
   */
  public static boolean hasQueueName(final SparkCluster cluster) {
    return hasQueueName((HasMetadata) cluster);
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
    long memoryMiB = calculateDriverMemoryMiB(sparkConf);
    String gpuAmount = sparkConf.get("spark.driver.resource.gpu.amount");
    String gpuVendor =
        sparkConf.getOrDefault("spark.driver.resource.gpu.vendor", DEFAULT_GPU_VENDOR);

    decorateTemplateResources(templateSpec, "spark-driver", cpu, memoryMiB, gpuAmount, gpuVendor);

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
    long memoryMiB = calculateExecutorMemoryMiB(sparkConf);
    String gpuAmount = sparkConf.get("spark.executor.resource.gpu.amount");
    String gpuVendor =
        sparkConf.getOrDefault("spark.executor.resource.gpu.vendor", DEFAULT_GPU_VENDOR);

    decorateTemplateResources(
        templateSpec, "spark-executor", cpu, memoryMiB, gpuAmount, gpuVendor);

    boolean dynamicAllocation =
        Boolean.parseBoolean(sparkConf.getOrDefault("spark.dynamicAllocation.enabled", "false"));
    int count;
    Integer minCount = null;
    if (dynamicAllocation) {
      int maxExecutors =
          parseInt(
              sparkConf.get("spark.dynamicAllocation.maxExecutors"),
              parseInt(sparkConf.get("spark.executor.instances"), 1));
      int minExecutors =
          parseInt(
              sparkConf.get("spark.dynamicAllocation.minExecutors"),
              parseInt(sparkConf.get("spark.dynamicAllocation.initialExecutors"), 0));
      count = maxExecutors;
      minCount = minExecutors;
    } else {
      count = parseInt(sparkConf.get("spark.executor.instances"), 1);
    }

    return PodSet.builder()
        .name(PODSET_EXECUTOR)
        .count(count)
        .minCount(minCount)
        .template(templateSpec)
        .build();
  }

  /**
   * Calculates total driver memory in MiB including overhead.
   */
  public static long calculateDriverMemoryMiB(final Map<String, String> sparkConf) {
    String memStr = sparkConf.getOrDefault("spark.driver.memory", DEFAULT_MEMORY);
    long memMiB = parseMemoryToMiB(memStr);
    long overheadMiB;
    if (sparkConf.containsKey("spark.driver.memoryOverhead")) {
      overheadMiB = parseMemoryToMiB(sparkConf.get("spark.driver.memoryOverhead"));
    } else {
      double factor =
          parseDouble(
              sparkConf.get("spark.driver.memoryOverheadFactor"), DEFAULT_MEMORY_OVERHEAD_FACTOR);
      overheadMiB = Math.max((long) Math.ceil(memMiB * factor), MIN_MEMORY_OVERHEAD_MIB);
    }
    return memMiB + overheadMiB;
  }

  /**
   * Calculates total executor memory in MiB including overhead and optional PySpark memory.
   */
  public static long calculateExecutorMemoryMiB(final Map<String, String> sparkConf) {
    String memStr = sparkConf.getOrDefault("spark.executor.memory", DEFAULT_MEMORY);
    long memMiB = parseMemoryToMiB(memStr);
    long overheadMiB;
    if (sparkConf.containsKey("spark.executor.memoryOverhead")) {
      overheadMiB = parseMemoryToMiB(sparkConf.get("spark.executor.memoryOverhead"));
    } else {
      double factor =
          parseDouble(
              sparkConf.get("spark.executor.memoryOverheadFactor"),
              DEFAULT_MEMORY_OVERHEAD_FACTOR);
      overheadMiB = Math.max((long) Math.ceil(memMiB * factor), MIN_MEMORY_OVERHEAD_MIB);
    }
    long total = memMiB + overheadMiB;
    if (sparkConf.containsKey("spark.executor.pyspark.memory")) {
      total += parseMemoryToMiB(sparkConf.get("spark.executor.pyspark.memory"));
    }
    return total;
  }

  /**
   * Parses memory string like '1g', '512m', '2048Mi' to MiB.
   */
  public static long parseMemoryToMiB(final String memoryStr) {
    if (StringUtils.isEmpty(memoryStr)) {
      return parseMemoryToMiB(DEFAULT_MEMORY);
    }
    Matcher matcher = MEMORY_PATTERN.matcher(memoryStr.trim());
    if (!matcher.matches()) {
      log.warn("Unable to parse memory string '{}', using default {}", memoryStr, DEFAULT_MEMORY);
      return parseMemoryToMiB(DEFAULT_MEMORY);
    }
    long integerPart = Long.parseLong(matcher.group(1));
    String fractionPart = matcher.group(2);
    String unit = matcher.group(3).toLowerCase(Locale.ROOT);

    double value = integerPart;
    if (fractionPart != null && !fractionPart.isEmpty()) {
      value += Double.parseDouble("0." + fractionPart);
    }

    return switch (unit) {
      case "b" -> (long) Math.ceil(value / (1024.0 * 1024.0));
      case "k", "kb", "ki", "kib" -> (long) Math.ceil(value / 1024.0);
      case "m", "mb", "mi", "mib", "" -> (long) Math.ceil(value);
      case "g", "gb", "gi", "gib" -> (long) Math.ceil(value * 1024.0);
      case "t", "tb", "ti", "tib" -> (long) Math.ceil(value * 1024.0 * 1024.0);
      case "p", "pb", "pi", "pib" -> (long) Math.ceil(value * 1024.0 * 1024.0 * 1024.0);
      default -> (long) Math.ceil(value);
    };
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
      container = podSpec.getContainers().get(0);
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

    if (!requests.containsKey("cpu")) {
      requests.put("cpu", new Quantity(cpu));
    }
    if (!requests.containsKey("memory")) {
      requests.put("memory", new Quantity(memoryMiB + "Mi"));
    }

    if (StringUtils.isNotEmpty(gpuAmount)) {
      if (!requests.containsKey(gpuVendor)) {
        requests.put(gpuVendor, new Quantity(gpuAmount));
      }
      Map<String, Quantity> limits = resources.getLimits();
      if (limits == null) {
        limits = new HashMap<>();
        resources.setLimits(limits);
      }
      if (!limits.containsKey(gpuVendor)) {
        limits.put(gpuVendor, new Quantity(gpuAmount));
      }
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
