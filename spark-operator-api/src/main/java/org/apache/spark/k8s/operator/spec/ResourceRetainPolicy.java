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

package org.apache.spark.k8s.operator.spec;

/**
 * Configure operator to delete / retain resources for an app after it terminates. This controls
 * the resources created directly by Operator (spark-submit), including driver pod and its
 * configmap. Resources created by driver are still controlled by driver (via SparkConf). Tuning
 * 'spark.kubernetes.driver.service.deleteOnTermination' and `spark.kubernetes.executor
 * .deleteOnTermination`in application for resource retaining policy is still necessary. Default
 * to 'Never' so operator deletes all created Spark resources after app terminates. Setting this
 * to 'Always' / 'OnFailure' to indicate operator not to delete resources based on app state, this
 * can be helpful in dev phase to debug failed Spark pod behavior. Driver has owner reference
 * configured to make sure it's garbage collected by k8s. Please be advised that resource would
 * not be retained if application is configured to restart, this is to avoid resource quota usage
 * increase unexpectedly or resource conflicts among multiple attempts.
 */
public enum ResourceRetainPolicy {
  Always,
  OnFailure,
  Never
}
