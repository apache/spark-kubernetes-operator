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

  private static final String NODE_SELECTOR_PREFIX = "spark.kubernetes.node.selector.";

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
    if ("true".equalsIgnoreCase(sparkConf.get("spark.dynamicAllocation.enabled"))) {
      throw new UnsupportedOperationException(
          "Kueue does not support SparkApplication with dynamic allocation "
              + "(spark.dynamicAllocation.enabled=true) yet.");
    }

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

  static PodSet buildDriverPodSet(
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

    Container container =
        selectContainer(
            templateSpec.getSpec(),
            sparkConf.get(Constants.DRIVER_SPARK_CONTAINER_PROP_KEY),
            "spark-kubernetes-driver");
    decorateContainerResources(container, sparkConf, "spark.driver.resource.", cpu, memoryMiB);
    decorateNodeSelector(
        templateSpec.getSpec(), sparkConf, "spark.kubernetes.driver.node.selector.");

    return PodSet.builder()
        .name(PODSET_DRIVER)
        .count(1)
        .template(templateSpec)
        .build();
  }

  static PodSet buildExecutorPodSet(
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
    long memoryMiB =
        calculateExecutorMemoryMiB(
            sparkConf, isNonJvmApp(app.getSpec()), isPythonApp(app.getSpec()));

    Container container =
        selectContainer(
            templateSpec.getSpec(),
            sparkConf.get(Constants.EXECUTOR_SPARK_CONTAINER_PROP_KEY),
            "spark-kubernetes-executor");
    decorateContainerResources(container, sparkConf, "spark.executor.resource.", cpu, memoryMiB);
    decorateNodeSelector(
        templateSpec.getSpec(), sparkConf, "spark.kubernetes.executor.node.selector.");

    return PodSet.builder()
        .name(PODSET_EXECUTOR)
        .count(parseInt(sparkConf.get("spark.executor.instances"), DEFAULT_EXECUTOR_INSTANCES))
        .template(templateSpec)
        .build();
  }

  /** Calculates total driver memory in MiB including overhead. */
  static long calculateDriverMemoryMiB(
      final Map<String, String> sparkConf, final boolean isNonJvm) {
    long memMiB =
        JavaUtils.byteStringAsMb(sparkConf.getOrDefault("spark.driver.memory", DEFAULT_MEMORY));
    return memMiB + calculateMemoryOverheadMiB(sparkConf, "spark.driver", memMiB, isNonJvm);
  }

  /**
   * Calculates total executor memory in MiB including overhead, off-heap memory and PySpark
   * memory.
   */
  static long calculateExecutorMemoryMiB(
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

  /**
   * Like Spark's KubernetesUtils.selectSparkContainer, selects the container by name and falls
   * back to the first container. A new container is added if the template has no container.
   */
  private static Container selectContainer(
      final PodSpec podSpec, final String containerName, final String defaultContainerName) {
    if (podSpec.getContainers().isEmpty()) {
      Container container = new ContainerBuilder().withName(defaultContainerName).build();
      podSpec.getContainers().add(container);
      return container;
    }
    return podSpec.getContainers().stream()
        .filter(c -> containerName != null && containerName.equals(c.getName()))
        .findFirst()
        .orElse(podSpec.getContainers().get(0));
  }

  private static void decorateContainerResources(
      final Container container,
      final Map<String, String> sparkConf,
      final String resourcePrefix,
      final String cpu,
      final long memoryMiB) {
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

    // Like Spark's KubernetesConf.buildKubernetesResourceName,
    // `spark.<role>.resource.<name>.amount` and `.vendor` become `<vendor>/<name>`,
    // e.g., `nvidia.com/gpu`.
    for (Map.Entry<String, String> e : sparkConf.entrySet()) {
      if (!e.getKey().startsWith(resourcePrefix) || !e.getKey().endsWith(".amount")) {
        continue;
      }
      String name =
          e.getKey().substring(resourcePrefix.length(), e.getKey().length() - ".amount".length());
      String vendor = sparkConf.get(resourcePrefix + name + ".vendor");
      if (StringUtils.isEmpty(vendor)) {
        throw new IllegalArgumentException(
            "Resource: " + name + " was requested, but vendor was not specified.");
      }
      Map<String, Quantity> limits = resources.getLimits();
      if (limits == null) {
        limits = new HashMap<>();
        resources.setLimits(limits);
      }
      requests.put(vendor + "/" + name, new Quantity(e.getValue()));
      limits.put(vendor + "/" + name, new Quantity(e.getValue()));
    }
  }

  private static void decorateNodeSelector(
      final PodSpec podSpec, final Map<String, String> sparkConf, final String rolePrefix) {
    if (podSpec.getNodeSelector() == null) {
      podSpec.setNodeSelector(new HashMap<>());
    }
    // Like Spark, the role-specific prefix is applied last so that it wins.
    putPrefixedKeyValuePairs(podSpec.getNodeSelector(), sparkConf, NODE_SELECTOR_PREFIX);
    putPrefixedKeyValuePairs(podSpec.getNodeSelector(), sparkConf, rolePrefix);
  }

  private static void putPrefixedKeyValuePairs(
      final Map<String, String> target, final Map<String, String> sparkConf, final String prefix) {
    for (Map.Entry<String, String> e : sparkConf.entrySet()) {
      if (e.getKey().startsWith(prefix)) {
        target.put(e.getKey().substring(prefix.length()), e.getValue());
      }
    }
  }

  private static int parseInt(final String str, final int defaultValue) {
    return StringUtils.isEmpty(str) ? defaultValue : Integer.parseInt(str.trim());
  }

  private static double parseDouble(final String str, final double defaultValue) {
    return StringUtils.isEmpty(str) ? defaultValue : Double.parseDouble(str.trim());
  }
}
