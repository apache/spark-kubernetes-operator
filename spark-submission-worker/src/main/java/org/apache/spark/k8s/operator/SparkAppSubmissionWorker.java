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

package org.apache.spark.k8s.operator;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

import scala.Option;

import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.commons.collections4.MapUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.deploy.k8s.KubernetesDriverSpec;
import org.apache.spark.deploy.k8s.submit.JavaMainAppResource;
import org.apache.spark.deploy.k8s.submit.KubernetesDriverBuilder;
import org.apache.spark.deploy.k8s.submit.MainAppResource;
import org.apache.spark.deploy.k8s.submit.PythonMainAppResource;
import org.apache.spark.deploy.k8s.submit.RMainAppResource;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;
import org.apache.spark.k8s.operator.spec.ConfigMapSpec;
import org.apache.spark.k8s.operator.spec.DriverServiceIngressSpec;
import org.apache.spark.k8s.operator.utils.ModelUtils;
import org.apache.spark.k8s.operator.utils.StringUtils;

/**
 * Similar to org.apache.spark.deploy.k8s.submit.KubernetesClientApplication. This reads args from
 * SparkApplication instead of starting separate spark-submit process.
 */
public class SparkAppSubmissionWorker {
  /**
   * Default length limit for generated app ID. Generated ID is used as resource-prefix when
   * user-provided ID is too long for this purpose. This applied to all resources associated with
   * the Spark app (including k8s service which has different naming length limit). Thus, we
   * truncate the hash part to 46 chars to leave some margin for spark resource prefix and suffix
   * (e.g. 'spark-', '-driver-svc' . etc)
   */
  public static final int DEFAULT_ID_LENGTH_LIMIT = 46;

  /** Default length limit to be applied to the hash-based part of generated ID. */
  public static final int DEFAULT_HASH_BASED_IDENTIFIER_LENGTH_LIMIT = 36;

  /** Radix value used when generating hash-based identifier. */
  public static final int DEFAULT_ENCODE_BASE = 36;

  /** Default master URL prefix for Kubernetes. */
  public static final String DEFAULT_MASTER_URL_PREFIX = "k8s://";

  /** Property name for the Spark master URL prefix. */
  public static final String MASTER_URL_PREFIX_PROPS_NAME = "spark.master.url.prefix";

  /** SHA256 Message Digest when generating hash-based identifier. */
  private static final ThreadLocal<MessageDigest> SHA_256_THREAD_LOCAL =
      ThreadLocal.withInitial(
          () -> {
            try {
              return MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
              throw new UnsupportedOperationException(e);
            }
          });

  /**
   * Build secondary resource spec for given app with Spark developer API, with defaults / overrides
   * as:
   *
   * <ul>
   *   <li>spark.kubernetes.namespace - if provided and the provided value is different from the
   *       namespace that the SparkApp resides in, it would be force override to the same value as
   *       the SparkApp custom resource.
   *   <li>spark.jars - if not provided, this would be set to the value in .spec.jars
   *   <li>spark.submit.pyFiles - if not provided, this would be set to the value in .spec.PyFiles
   *   <li>spark.master - if not provided, this would be automatically built with
   *       KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT, and prefix 'k8s://' for native Spark
   *       ExternalClusterManager. It is possible to invoke custom ClusterManager: to do so, either
   *       set 'spark.master', or set `spark.master.url.prefix` so submission worker can build
   *       master url based on it.
   * </ul>
   *
   * @param app The SparkApplication resource.
   * @param client The Kubernetes client.
   * @param confOverrides Key-value pairs of configuration overrides for the application.
   * @return SparkAppResourceSpec containing the secondary resources.
   */
  public SparkAppResourceSpec getResourceSpec(
      SparkApplication app, KubernetesClient client, Map<String, String> confOverrides) {
    SparkAppDriverConf appDriverConf = buildDriverConf(app, confOverrides);
    return buildResourceSpec(
        appDriverConf,
        app.getSpec().getDriverServiceIngressList(),
        app.getSpec().getConfigMapSpecs(),
        client);
  }

  /**
   * Builds the SparkAppDriverConf for the given SparkApplication.
   *
   * @param app The SparkApplication.
   * @param confOverrides Configuration overrides for the application.
   * @return A SparkAppDriverConf instance.
   */
  protected SparkAppDriverConf buildDriverConf(
      SparkApplication app, Map<String, String> confOverrides) {
    ApplicationSpec applicationSpec = app.getSpec();
    SparkConf effectiveSparkConf = new SparkConf();
    if (MapUtils.isNotEmpty(applicationSpec.getSparkConf())) {
      for (String confKey : applicationSpec.getSparkConf().keySet()) {
        effectiveSparkConf.set(confKey, applicationSpec.getSparkConf().get(confKey));
      }
    }
    if (MapUtils.isNotEmpty(confOverrides)) {
      for (Map.Entry<String, String> entry : confOverrides.entrySet()) {
        effectiveSparkConf.set(entry.getKey(), entry.getValue());
      }
    }
    effectiveSparkConf.set("spark.kubernetes.namespace", app.getMetadata().getNamespace());
    MainAppResource primaryResource = new JavaMainAppResource(Option.empty());
    if (StringUtils.isNotEmpty(applicationSpec.getJars())) {
      primaryResource = new JavaMainAppResource(Option.apply(applicationSpec.getJars()));
      effectiveSparkConf.setIfMissing("spark.jars", applicationSpec.getJars());
    } else if (StringUtils.isNotEmpty(applicationSpec.getPyFiles())) {
      primaryResource = new PythonMainAppResource(applicationSpec.getPyFiles());
      effectiveSparkConf.setIfMissing("spark.submit.pyFiles", applicationSpec.getPyFiles());
    } else if (StringUtils.isNotEmpty(applicationSpec.getSparkRFiles())) {
      primaryResource = new RMainAppResource(applicationSpec.getSparkRFiles());
    }
    String sparkMasterUrlPrefix =
        effectiveSparkConf.get(MASTER_URL_PREFIX_PROPS_NAME, DEFAULT_MASTER_URL_PREFIX);
    effectiveSparkConf.setIfMissing(
        "spark.master",
        sparkMasterUrlPrefix + "https://$KUBERNETES_SERVICE_HOST:$KUBERNETES_SERVICE_PORT");
    String appId = generateSparkAppId(app);
    effectiveSparkConf.setIfMissing("spark.app.id", appId);
    return SparkAppDriverConf.create(
        effectiveSparkConf,
        effectiveSparkConf.getAppId(),
        primaryResource,
        applicationSpec.getMainClass(),
        applicationSpec.getDriverArgs().toArray(String[]::new),
        Option.apply(applicationSpec.getProxyUser()));
  }

  /**
   * Builds the SparkAppResourceSpec from the driver configuration and other specifications.
   *
   * @param kubernetesDriverConf The SparkAppDriverConf.
   * @param driverServiceIngressList A list of DriverServiceIngressSpec.
   * @param configMapSpecs A list of ConfigMapSpec.
   * @param client The KubernetesClient.
   * @return A SparkAppResourceSpec instance.
   */
  protected SparkAppResourceSpec buildResourceSpec(
      SparkAppDriverConf kubernetesDriverConf,
      List<DriverServiceIngressSpec> driverServiceIngressList,
      List<ConfigMapSpec> configMapSpecs,
      KubernetesClient client) {
    KubernetesDriverBuilder builder = new KubernetesDriverBuilder();
    KubernetesDriverSpec kubernetesDriverSpec =
        builder.buildFromFeatures(kubernetesDriverConf, client);
    return new SparkAppResourceSpec(
        kubernetesDriverConf, kubernetesDriverSpec, driverServiceIngressList, configMapSpecs);
  }

  /**
   * Generates a deterministic Spark application ID based on the application's metadata and attempt
   * ID in order to ensure operator reconciliation idempotency.
   *
   * @param app The SparkApplication object.
   * @return A generated Spark application ID.
   */
  public static String generateSparkAppId(final SparkApplication app) {
    long attemptId = ModelUtils.getAttemptId(app);
    String preferredId = String.format("%s-%d", app.getMetadata().getName(), attemptId);
    if (preferredId.length() > DEFAULT_ID_LENGTH_LIMIT) {
      int preferredIdPrefixLength =
          DEFAULT_ID_LENGTH_LIMIT - DEFAULT_HASH_BASED_IDENTIFIER_LENGTH_LIMIT - 1;
      String preferredIdPrefix = preferredId.substring(0, preferredIdPrefixLength);
      return generateHashBasedId(
          preferredIdPrefix,
          app.getMetadata().getNamespace(),
          app.getMetadata().getName(),
          String.valueOf(attemptId));
    } else {
      return preferredId;
    }
  }

  /**
   * Generates a hash-based ID with a given prefix and identifiers. The hash part would have a
   * length-limit of `DEFAULT_HASH_BASED_IDENTIFIER_LENGTH_LIMIT`.
   *
   * @param prefix String prefix to be applied to the generated hash-based ID.
   * @param identifiers Keys to generate the hash.
   * @return The generated hash-based ID.
   */
  public static String generateHashBasedId(final String prefix, final String... identifiers) {
    final MessageDigest sha256 = SHA_256_THREAD_LOCAL.get();
    String sha256Hash =
        new BigInteger(1, sha256.digest(String.join("/", identifiers).getBytes(UTF_8)))
            .toString(DEFAULT_ENCODE_BASE);
    String truncatedIdentifiersHash =
        sha256Hash.substring(0, DEFAULT_HASH_BASED_IDENTIFIER_LENGTH_LIMIT);
    return String.join("-", prefix, truncatedIdentifiersHash);
  }
}
