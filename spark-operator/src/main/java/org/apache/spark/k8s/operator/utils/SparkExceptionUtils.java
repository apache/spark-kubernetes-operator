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

package org.apache.spark.k8s.operator.utils;

import static java.net.HttpURLConnection.HTTP_CONFLICT;

import io.fabric8.kubernetes.client.KubernetesClientException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

/** Utility class for Spark exceptions. */
public final class SparkExceptionUtils {

  private SparkExceptionUtils() {}

  /**
   * Checks if a KubernetesClientException is a conflict due to an already existing resource.
   *
   * @param e The KubernetesClientException to check.
   * @return True if it's a conflict for an existing resource, false otherwise.
   */
  public static boolean isConflictForExistingResource(KubernetesClientException e) {
    return e != null
        && e.getCode() == HTTP_CONFLICT
        && e.getStatus() != null
        && StringUtils.isNotEmpty(e.getStatus().toString())
        && e.getStatus().toString().toLowerCase().contains("alreadyexists");
  }

  /**
   * Builds a general error message from an Exception, including its stack trace.
   *
   * @param e The Exception to build the message from.
   * @return A string containing the stack trace of the exception.
   */
  public static String buildGeneralErrorMessage(Exception e) {
    return ExceptionUtils.getStackTrace(e);
  }
}
