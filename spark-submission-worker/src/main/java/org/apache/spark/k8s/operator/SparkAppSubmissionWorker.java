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

import java.math.BigInteger;
import java.util.Map;

import scala.Option;

import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.deploy.k8s.KubernetesDriverSpec;
import org.apache.spark.deploy.k8s.submit.JavaMainAppResource;
import org.apache.spark.deploy.k8s.submit.KubernetesDriverBuilder;
import org.apache.spark.deploy.k8s.submit.MainAppResource;
import org.apache.spark.deploy.k8s.submit.PythonMainAppResource;
import org.apache.spark.deploy.k8s.submit.RMainAppResource;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;

/**
 * Similar to org.apache.spark.deploy.k8s.submit.KubernetesClientApplication. This reads args from
 * SparkApplication instead of starting separate spark-submit process
 */
public class SparkAppSubmissionWorker {
  // Default length limit for generated app id. Generated id is used as resource-prefix when
  // user-provided id is too long for this purpose. This applied to all resources associated with
  // the Spark app (including k8s service which has different naming length limit). This we
  // truncate the hash part to 46 chars to leave some margin for spark resource prefix and suffix
  // (e.g. 'spark-', '-driver-svc' . etc)
  public static final int DEFAULT_ID_LENGTH_LIMIT = 46;
  // Default length limit to be applied to the hash-based part of generated id
  public static final int DEFAULT_HASH_BASED_IDENTIFIER_LENGTH_LIMIT = 36;
  // Radix value used when generating hash-based identifier
  public static final int DEFAULT_ENCODE_BASE = 36;

  public SparkAppResourceSpec getResourceSpec(
      SparkApplication app, KubernetesClient client, Map<String, String> confOverrides) {
    SparkAppDriverConf appDriverConf = buildDriverConf(app, confOverrides);
    return buildResourceSpec(appDriverConf, client);
  }

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
    effectiveSparkConf.setIfMissing(
        "spark.master", "k8s://https://$KUBERNETES_SERVICE_HOST:$KUBERNETES_SERVICE_PORT");
    String appId = createSparkAppId(app);
    effectiveSparkConf.setIfMissing("spark.app.id", appId);
    return SparkAppDriverConf.create(
        effectiveSparkConf,
        appId,
        primaryResource,
        applicationSpec.getMainClass(),
        applicationSpec.getDriverArgs().toArray(new String[0]),
        Option.apply(applicationSpec.getProxyUser()));
  }

  protected SparkAppResourceSpec buildResourceSpec(
      SparkAppDriverConf kubernetesDriverConf, KubernetesClient client) {
    KubernetesDriverBuilder builder = new KubernetesDriverBuilder();
    KubernetesDriverSpec kubernetesDriverSpec =
        builder.buildFromFeatures(kubernetesDriverConf, client);
    return new SparkAppResourceSpec(kubernetesDriverConf, kubernetesDriverSpec);
  }

  /**
   * Spark application id need to be deterministic per attempt per Spark App. This is to ensure
   * operator reconciliation idempotency.
   */
  protected String createSparkAppId(final SparkApplication app) {
    long attemptId = getAttemptId(app);
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

  protected long getAttemptId(final SparkApplication app) {
    long attemptId = 0L;
    if (app.getStatus() != null && app.getStatus().getCurrentAttemptSummary() != null) {
      attemptId = app.getStatus().getCurrentAttemptSummary().getAttemptInfo().getId();
    }
    return attemptId;
  }

  public String generateHashBasedId(final String prefix, final String... identifiers) {
    return generateHashBasedId(
        prefix, DEFAULT_ENCODE_BASE, DEFAULT_HASH_BASED_IDENTIFIER_LENGTH_LIMIT, identifiers);
  }

  public String generateHashBasedId(
      final String prefix,
      final int hashEncodeBaseRadix,
      final int identifiersHashLengthLimit,
      final String... identifiers) {
    String sha256Hash =
        new BigInteger(1, DigestUtils.sha256(String.join("/", identifiers)))
            .toString(hashEncodeBaseRadix);
    String truncatedIdentifiersHash = sha256Hash.substring(0, identifiersHashLengthLimit);
    return String.join("-", prefix, truncatedIdentifiersHash);
  }
}
