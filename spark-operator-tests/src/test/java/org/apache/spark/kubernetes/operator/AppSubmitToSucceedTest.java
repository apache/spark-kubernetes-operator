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
 *
 */

package org.apache.spark.kubernetes.operator;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.kubernetes.operator.status.ApplicationStateSummary;

class AppSubmitToSucceedTest {
  private static final Logger logger = LoggerFactory.getLogger(AppSubmitToSucceedTest.class);

  /**
   * Create Spark app(s) & wait them for complete.
   * This sample would check apps periodically, force delete them after timeout if they have
   * not completed.
   * Exit 0 iff all given app(s) isTerminated successfully.
   * E.g. when test cluster is up and kube config is configured, this can be invoked as
   * java -cp /path/to/test.jar -Dspark.operator.test.app.yaml.files.dir=/path/to/e2e-tests/
   * org.apache.spark.kubernetes.operator.AppSubmitToSucceedTest
   *
   * @param args directory path(s) to load SparkApp yaml file(s) from
   */
  public static void main(String[] args) throws InterruptedException {
    KubernetesClient client = new KubernetesClientBuilder().build();

    Duration observeInterval = Duration.ofMinutes(
        Long.parseLong(
            System.getProperty("spark.operator.test.observe.interval.min", "1")));
    Duration appExecTimeout = Duration.ofMinutes(
        Long.parseLong(
            System.getProperty("spark.operator.test.app.timeout.min", "10")));
    Duration testTimeout = Duration.ofMinutes(
        Long.parseLong(
            System.getProperty("spark.operator.test.timeout.min", "30")));
    Integer execParallelism = Integer.parseInt(
        System.getProperty("spark.operator.test.exec.parallelism", "2"));
    String testAppYamlFilesDir = System.getProperty("spark.operator.test.app.yaml.files.dir",
        "e2e-tests/spark-apps/");
    String testAppNamespace = System.getProperty("spark.operator.test.app.namespace",
        "spark-test");

    Set<SparkApplication> testApps =
        loadSparkAppsFromFile(client, new File(testAppYamlFilesDir));
    ConcurrentMap<String, String> failedApps = new ConcurrentHashMap<>();

    ExecutorService execPool = Executors.newFixedThreadPool(execParallelism);
    List<Callable<Void>> todos = new ArrayList<>(testApps.size());

    for (SparkApplication app : testApps) {
      todos.add(() -> {
        try {
          Instant timeoutTime = Instant.now().plus(appExecTimeout);
          SparkApplication updatedApp =
              client.resource(app).inNamespace(testAppNamespace).create();
          if (logger.isInfoEnabled()) {
            logger.info("Submitting app {}", updatedApp.getMetadata().getName());
          }
          while (Instant.now().isBefore(timeoutTime)) {
            Thread.sleep(observeInterval.toMillis());
            updatedApp = client.resource(app).inNamespace(testAppNamespace).get();
            if (appCompleted(updatedApp)) {
              boolean succeeded = updatedApp.getStatus().getStateTransitionHistory()
                  .entrySet()
                  .stream()
                  .anyMatch(e -> ApplicationStateSummary.SUCCEEDED.equals(
                      e.getValue().getCurrentStateSummary()));
              if (succeeded) {
                if (logger.isInfoEnabled()) {
                  logger.info("App succeeded: {}",
                      updatedApp.getMetadata().getName());
                }
              } else {
                if (logger.isErrorEnabled()) {
                  logger.error("App failed: {}",
                      updatedApp.getMetadata().getName());
                }
                failedApps.put(updatedApp.getMetadata().getName(),
                    updatedApp.getStatus().toString());
              }
              return null;
            } else {
              if (logger.isInfoEnabled()) {
                logger.info("Application {} not completed...",
                    app.getMetadata().getName());
              }
            }
          }
          if (logger.isInfoEnabled()) {
            logger.info("App {} timed out.", app.getMetadata().getName());
          }
          failedApps.put(updatedApp.getMetadata().getName(),
              "timed out: " + updatedApp.getStatus().toString());
          return null;
        } catch (Exception e) {
          failedApps.put(app.getMetadata().getName(), "failed: " + e.getMessage());
          return null;
        }
      });
    }

    int testSucceeded = 1;
    try {
      execPool.invokeAll(todos, testTimeout.toMillis(), TimeUnit.MILLISECONDS);
      if (failedApps.isEmpty()) {
        if (logger.isInfoEnabled()) {
          logger.info("Test completed successfully");
        }
        testSucceeded = 0;
      } else {
        if (logger.isErrorEnabled()) {
          logger.error("Failed apps found. ");
          failedApps.forEach((k, v) -> {
            logger.error("Application failed: {}", k);
            logger.error("\t status: {}", v);
          });
        }
      }
    } finally {
      for (SparkApplication app : testApps) {
        try {
          client.resource(app).inNamespace(testAppNamespace).delete();
        } catch (Exception e) {
          if (logger.isErrorEnabled()) {
            logger.error("Failed to remove app {}", app.getMetadata().getName());
          }
        }
      }
    }
    System.exit(testSucceeded);
  }

  private static Set<SparkApplication> loadSparkAppsFromFile(KubernetesClient client,
                                                             File appsFile) {
    if (appsFile.exists()) {
      if (appsFile.isFile()) {
        return Collections.singleton(
            client.resources(SparkApplication.class).load(appsFile).item());
      } else {
        Set<SparkApplication> applications = new HashSet<>();
        File[] subDirs = appsFile.listFiles();
        if (subDirs != null) {
          for (File file : subDirs) {
            applications.addAll(loadSparkAppsFromFile(client, file));
          }
        }
        return applications;
      }
    }
    if (logger.isErrorEnabled()) {
      logger.error("No SparkApp found at {}", appsFile.getAbsolutePath());
    }
    return Collections.emptySet();
  }

  private static boolean appCompleted(SparkApplication app) {
    return app != null && app.getStatus() != null && app.getStatus().getCurrentState() != null
        && app.getStatus().getStateTransitionHistory() != null
        && app.getStatus().getCurrentState().getCurrentStateSummary().isTerminated();
  }
}
