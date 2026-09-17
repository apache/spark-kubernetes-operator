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
import io.fabric8.kubernetes.api.model.EnvVar;
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
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.Constants;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.SparkCluster;
import org.apache.spark.k8s.operator.SparkClusterResourceSpec;
import org.apache.spark.k8s.operator.SparkClusterSubmissionWorker;
import org.apache.spark.k8s.operator.kueue.v1beta2.PodSet;
import org.apache.spark.k8s.operator.kueue.v1beta2.Workload;
import org.apache.spark.k8s.operator.kueue.v1beta2.WorkloadSpec;
import org.apache.spark.k8s.operator.reconciler.SparkClusterResourceSpecFactory;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.spec.BaseApplicationTemplateSpec;
import org.apache.spark.k8s.operator.utils.ModelUtils;
import org.apache.spark.k8s.operator.utils.ReconcilerUtils;
import org.apache.spark.k8s.operator.utils.StringUtils;
import org.apache.spark.network.util.JavaUtils;

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

  private static final String DEFAULT_CORES = "1";
  private static final String DEFAULT_MEMORY = "1g";
  private static final String DEFAULT_MIN_MEMORY_OVERHEAD = "384m";
  private static final double DEFAULT_MEMORY_OVERHEAD_FACTOR = 0.10;
  private static final double NON_JVM_MEMORY_OVERHEAD_FACTOR = 0.40;
  private static final int DEFAULT_EXECUTOR_INSTANCES = 2;

  private static final String NODE_SELECTOR_PREFIX = "spark.kubernetes.node.selector.";

  private KueueWorkloadFactory() {}

  /**
   * Builds a Kueue Workload from a SparkApplication resource.
   *
   * @param app The SparkApplication.
   * @return The constructed Kueue Workload.
   */
  public static Workload buildWorkload(final SparkApplication app) {
    ApplicationSpec spec = app.getSpec();
    Map<String, String> sparkConf = spec.getSparkConf();
    if ("true".equalsIgnoreCase(sparkConf.get("spark.dynamicAllocation.enabled"))) {
      throw new UnsupportedOperationException(
          "Kueue does not support SparkApplication with dynamic allocation "
              + "(spark.dynamicAllocation.enabled=true) yet.");
    }
    // Like Spark's KubernetesClusterManager, `local[*]` runs the driver only without executors.
    boolean driverOnly =
        sparkConf.getOrDefault("spark.kubernetes.driver.master", "").startsWith("local");
    checkNoPodTemplateFile(
        sparkConf,
        Constants.DRIVER_SPARK_TEMPLATE_FILE_PROP_KEY,
        ModelUtils.overrideDriverTemplateEnabled(spec));
    if (!driverOnly) {
      checkNoPodTemplateFile(
          sparkConf,
          Constants.EXECUTOR_SPARK_TEMPLATE_FILE_PROP_KEY,
          ModelUtils.overrideExecutorTemplateEnabled(spec));
    }
    List<PodSet> podSets =
        driverOnly
            ? List.of(buildDriverPodSet(app, sparkConf))
            : List.of(buildDriverPodSet(app, sparkConf), buildExecutorPodSet(app, sparkConf));
    return buildWorkload(app, Constants.LABEL_SPARK_APPLICATION_NAME, spec.isSuspend(), podSets);
  }

  /**
   * Builds a Kueue Workload from a SparkCluster resource.
   *
   * @param cluster The SparkCluster.
   * @return The constructed Kueue Workload.
   */
  public static Workload buildWorkload(final SparkCluster cluster) {
    // Use the same StatefulSets which the operator creates for the cluster.
    SparkClusterResourceSpec resourceSpec =
        SparkClusterResourceSpecFactory.buildResourceSpec(
            cluster, new SparkClusterSubmissionWorker());
    if (resourceSpec.getHorizontalPodAutoscaler().isPresent()) {
      throw new UnsupportedOperationException(
          "Kueue does not support SparkCluster with HorizontalPodAutoscaler "
              + "(minWorkers < maxWorkers) yet.");
    }
    List<PodSet> podSets =
        List.of(
            buildPodSet(PODSET_MASTER, resourceSpec.getMasterStatefulSet()),
            buildPodSet(PODSET_WORKER, resourceSpec.getWorkerStatefulSet()));
    return buildWorkload(
        cluster, Constants.LABEL_SPARK_CLUSTER_NAME, cluster.getSpec().isSuspend(), podSets);
  }

  /**
   * A pod template file is fetched by Spark, not by the operator, so its node selectors,
   * tolerations and extra containers cannot be reflected in the PodSet template. The operator
   * overwrites the file key when the pod template is set in the SparkApplication spec, so the
   * spec wins in that case.
   */
  private static void checkNoPodTemplateFile(
      final Map<String, String> sparkConf,
      final String templateFileKey,
      final boolean specTemplateWins) {
    if (sparkConf.containsKey(templateFileKey) && !specTemplateWins) {
      throw new UnsupportedOperationException(
          "Kueue does not support "
              + templateFileKey
              + " yet. Set the pod template in the SparkApplication spec instead.");
    }
  }

  private static Workload buildWorkload(
      final HasMetadata owner,
      final String nameLabelKey,
      final boolean suspend,
      final List<PodSet> podSets) {
    Map<String, String> labels = new HashMap<>();
    if (owner.getMetadata().getLabels() != null) {
      labels.putAll(owner.getMetadata().getLabels());
    }
    labels.put(nameLabelKey, owner.getMetadata().getName());

    OwnerReference ownerReference = ModelUtils.buildOwnerReferenceTo(owner);
    ownerReference.setController(true);

    Workload workload = new Workload();
    workload.setMetadata(
        new ObjectMetaBuilder()
            .withName(getWorkloadName(owner))
            .withNamespace(owner.getMetadata().getNamespace())
            .withLabels(labels)
            .withOwnerReferences(ownerReference)
            .build());
    workload.setSpec(
        WorkloadSpec.builder()
            .queueName(getQueueName(owner))
            .active(!suspend)
            .podSets(podSets)
            .build());
    return workload;
  }

  /**
   * Builds a PodSet for a Spark standalone role (`master` or `worker`) from the StatefulSet. Unlike
   * the SparkApplication pods, Spark does not set the requests of the master and worker pods, so
   * the missing CPU and memory requests are calculated from the environment variables of the
   * container in the same way as Spark standalone does. Otherwise, Kueue admits the pods without
   * accounting them against the quota.
   */
  private static PodSet buildPodSet(final String role, final StatefulSet statefulSet) {
    PodTemplateSpec templateSpec = statefulSet.getSpec().getTemplate();
    // The operator always creates the container named after the role.
    Container container = selectContainer(templateSpec.getSpec(), role, role);
    Map<String, Quantity> requests = getOrCreateRequests(container);
    Map<String, Quantity> limits = container.getResources().getLimits();
    Map<String, String> env = getEnv(container);
    boolean isWorker = PODSET_WORKER.equals(role);
    String cpu = isWorker ? env.getOrDefault("SPARK_WORKER_CORES", DEFAULT_CORES) : DEFAULT_CORES;
    long memoryMiB =
        calculateDaemonMemoryMiB(
            env.get("SPARK_DAEMON_MEMORY"), isWorker ? env.get("SPARK_WORKER_MEMORY") : null);
    // Like Kubernetes, a missing request defaults to the limit.
    fillMissingRequest(requests, limits, "cpu", new Quantity(cpu));
    fillMissingRequest(requests, limits, "memory", new Quantity(memoryMiB + "Mi"));
    return PodSet.builder()
        .name(role)
        .count(statefulSet.getSpec().getReplicas())
        .template(templateSpec)
        .build();
  }

  private static void fillMissingRequest(
      final Map<String, Quantity> requests,
      final Map<String, Quantity> limits,
      final String name,
      final Quantity defaultValue) {
    if (!requests.containsKey(name)) {
      requests.put(
          name, limits != null && limits.containsKey(name) ? limits.get(name) : defaultValue);
    }
  }

  /**
   * Returns the Workload name prefixed with the lower-cased kind of the owner resource, like Kueue
   * built-in integrations, to avoid name collisions between SparkApplication and SparkCluster.
   */
  static String getWorkloadName(final HasMetadata resource) {
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
    ApplicationSpec spec = app.getSpec();
    return buildPodSet(
        PODSET_DRIVER,
        1,
        spec.getDriverSpec(),
        sparkConf,
        Constants.DRIVER_SPARK_CONTAINER_PROP_KEY,
        calculateDriverMemoryMiB(sparkConf, isNonJvmApp(spec)));
  }

  static PodSet buildExecutorPodSet(
      final SparkApplication app, final Map<String, String> sparkConf) {
    ApplicationSpec spec = app.getSpec();
    return buildPodSet(
        PODSET_EXECUTOR,
        parseInt(sparkConf.get("spark.executor.instances"), DEFAULT_EXECUTOR_INSTANCES),
        spec.getExecutorSpec(),
        sparkConf,
        Constants.EXECUTOR_SPARK_CONTAINER_PROP_KEY,
        calculateExecutorMemoryMiB(sparkConf, isNonJvmApp(spec), isPythonApp(spec)));
  }

  /**
   * Builds a PodSet for a Spark role (`driver` or `executor`) from the pod template of the
   * SparkApplication and the resource configurations in the same way as Spark does.
   */
  private static PodSet buildPodSet(
      final String role,
      final int count,
      final BaseApplicationTemplateSpec roleSpec,
      final Map<String, String> sparkConf,
      final String containerNameKey,
      final long memoryMiB) {
    PodTemplateSpec templateSpec =
        roleSpec != null && roleSpec.getPodTemplateSpec() != null
            ? ReconcilerUtils.clone(roleSpec.getPodTemplateSpec())
            : new PodTemplateSpecBuilder().build();
    if (templateSpec.getSpec() == null) {
      templateSpec.setSpec(new PodSpecBuilder().build());
    }
    PodSpec podSpec = templateSpec.getSpec();
    if (podSpec.getContainers() == null) {
      podSpec.setContainers(new ArrayList<>());
    }

    String cpu =
        sparkConf.getOrDefault(
            "spark.kubernetes." + role + ".request.cores",
            sparkConf.getOrDefault("spark." + role + ".cores", DEFAULT_CORES));
    Container container =
        selectContainer(podSpec, sparkConf.get(containerNameKey), "spark-kubernetes-" + role);
    decorateContainerResources(
        container, sparkConf, "spark." + role + ".resource.", cpu, memoryMiB);
    decorateNodeSelector(podSpec, sparkConf, "spark.kubernetes." + role + ".node.selector.");

    return PodSet.builder().name(role).count(count).template(templateSpec).build();
  }

  /** Calculates total driver memory in MiB including overhead. */
  static long calculateDriverMemoryMiB(
      final Map<String, String> sparkConf, final boolean isNonJvm) {
    long memMiB =
        JavaUtils.byteStringAsMb(sparkConf.getOrDefault("spark.driver.memory", DEFAULT_MEMORY));
    return memMiB + calculateMemoryOverheadMiB(sparkConf, "spark.driver", memMiB, isNonJvm);
  }

  /**
   * Calculates total memory in MiB including overhead for Spark standalone master or worker.
   *
   * @param daemonMemory `SPARK_DAEMON_MEMORY`, the JVM heap of the master or worker daemon.
   * @param workerMemory `SPARK_WORKER_MEMORY`, the memory which the worker gives to executors.
   */
  static long calculateDaemonMemoryMiB(final String daemonMemory, final String workerMemory) {
    // Like Spark, the memory is in bytes unless otherwise specified.
    String memory = StringUtils.isEmpty(daemonMemory) ? DEFAULT_MEMORY : daemonMemory;
    long memMiB = JavaUtils.byteStringAsBytes(memory) / 1024 / 1024;
    if (StringUtils.isNotEmpty(workerMemory)) {
      memMiB += JavaUtils.byteStringAsBytes(workerMemory) / 1024 / 1024;
    }
    long minOverheadMiB = JavaUtils.byteStringAsMb(DEFAULT_MIN_MEMORY_OVERHEAD);
    return memMiB + Math.max((long) (DEFAULT_MEMORY_OVERHEAD_FACTOR * memMiB), minOverheadMiB);
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
      long offHeapMiB =
          JavaUtils.byteStringAsBytes(sparkConf.getOrDefault("spark.memory.offHeap.size", "0"))
              / 1024
              / 1024;
      if (offHeapMiB <= 0) {
        throw new IllegalArgumentException(
            "spark.memory.offHeap.size must be at least 1MiB when "
                + "spark.memory.offHeap.enabled == true");
      }
      total += offHeapMiB;
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

  /**
   * Like Spark's KubernetesUtils.selectSparkContainer, selects the container by name and falls
   * back to the first container. A new container is added if the template has no container.
   */
  private static Container selectContainer(
      final PodSpec podSpec, final String containerName, final String defaultContainerName) {
    List<Container> containers = podSpec.getContainers();
    if (containers.isEmpty()) {
      Container container = new ContainerBuilder().withName(defaultContainerName).build();
      containers.add(container);
      return container;
    }
    if (containerName != null) {
      for (Container container : containers) {
        if (containerName.equals(container.getName())) {
          return container;
        }
      }
      log.warn(
          "Specified container {} not found on pod template, falling back to taking the first "
              + "container",
          containerName);
    }
    return containers.get(0);
  }

  private static void decorateContainerResources(
      final Container container,
      final Map<String, String> sparkConf,
      final String resourcePrefix,
      final String cpu,
      final long memoryMiB) {
    Map<String, Quantity> requests = getOrCreateRequests(container);
    ResourceRequirements resources = container.getResources();

    // Like Spark, overwrite the requests of the pod template.
    requests.put("cpu", new Quantity(cpu));
    requests.put("memory", new Quantity(memoryMiB + "Mi"));

    // Like Spark's KubernetesConf.buildKubernetesResourceName,
    // `spark.<role>.resource.<name>.amount` and `.vendor` become `<vendor>/<name>`,
    // e.g., `nvidia.com/gpu`. Like Spark's ResourceUtils, a non-positive amount is ignored.
    for (Map.Entry<String, String> e : sparkConf.entrySet()) {
      if (!e.getKey().startsWith(resourcePrefix)
          || !e.getKey().endsWith(".amount")
          || parseInt(e.getValue(), 0) <= 0) {
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

  /** Returns the environment variables with the literal values. `valueFrom` is ignored. */
  private static Map<String, String> getEnv(final Container container) {
    Map<String, String> env = new HashMap<>();
    if (container.getEnv() != null) {
      for (EnvVar e : container.getEnv()) {
        if (StringUtils.isNotEmpty(e.getValue())) {
          env.put(e.getName(), e.getValue().trim());
        }
      }
    }
    return env;
  }

  private static Map<String, Quantity> getOrCreateRequests(final Container container) {
    ResourceRequirements resources = container.getResources();
    if (resources == null) {
      resources = new ResourceRequirementsBuilder().build();
      container.setResources(resources);
    }
    if (resources.getRequests() == null) {
      resources.setRequests(new HashMap<>());
    }
    return resources.getRequests();
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
