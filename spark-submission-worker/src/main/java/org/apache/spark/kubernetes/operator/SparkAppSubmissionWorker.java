/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.kubernetes.operator;

import java.util.Map;

import scala.Option;

import io.fabric8.kubernetes.client.KubernetesClient;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import org.apache.spark.SparkConf;
import org.apache.spark.deploy.k8s.KubernetesDriverSpec;
import org.apache.spark.deploy.k8s.submit.JavaMainAppResource;
import org.apache.spark.deploy.k8s.submit.KubernetesDriverBuilder;
import org.apache.spark.deploy.k8s.submit.MainAppResource;
import org.apache.spark.deploy.k8s.submit.PythonMainAppResource;
import org.apache.spark.deploy.k8s.submit.RMainAppResource;
import org.apache.spark.k8s.operator.SparkApplication;
import org.apache.spark.k8s.operator.spec.ApplicationSpec;

/**
 * Similar to org.apache.spark.deploy.k8s.submit.KubernetesClientApplication. This reads args from
 * SparkApplication instead of starting separate spark-submit process
 */
public class SparkAppSubmissionWorker {

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
    return SparkAppDriverConf.create(
        effectiveSparkConf,
        createSparkAppId(app),
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
   * operator reconciliation idempotency
   */
  protected String createSparkAppId(final SparkApplication app) {
    long attemptId = 0L;
    if (app.getStatus() != null && app.getStatus().getCurrentAttemptSummary() != null) {
      attemptId = app.getStatus().getCurrentAttemptSummary().getAttemptInfo().getId();
    }
    return String.format("%s-%d", app.getMetadata().getName(), attemptId);
  }
}
