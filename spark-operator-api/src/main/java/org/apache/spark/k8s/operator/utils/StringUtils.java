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

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Utility class for string operations.
 *
 * @since 0.6.0
 */
public final class StringUtils {
  private StringUtils() {}

  /**
   * Checks whether the given string is null, empty, or contains only whitespace.
   *
   * @since 0.6.0
   */
  public static boolean isBlank(final String str) {
    return str == null || str.isBlank();
  }

  /**
   * Checks whether the given string is not null and contains at least one non-whitespace character.
   *
   * @since 0.6.0
   */
  public static boolean isNotBlank(final String str) {
    return !isBlank(str);
  }

  /**
   * Checks whether the given string is null or empty.
   *
   * @since 0.6.0
   */
  public static boolean isEmpty(final String str) {
    return str == null || str.length() == 0;
  }

  /**
   * Checks whether the given string is not null and not empty.
   *
   * @since 0.6.0
   */
  public static boolean isNotEmpty(final String str) {
    return !isEmpty(str);
  }

  /**
   * Finds the index of the n-th occurrence of a substring within the given string, or -1 if absent.
   *
   * @since 0.6.0
   */
  public static int ordinalIndexOf(final String str, final String substr, final int ordinal) {
    if (str == null || substr == null || ordinal <= 0) {
      return -1;
    }
    if (substr.isEmpty()) {
      return 0;
    }

    int index = -1;
    for (int i = 0; i < ordinal; i++) {
      index = str.indexOf(substr, index + 1);
      if (index == -1) {
        return -1;
      }
    }
    return index;
  }

  /**
   * Returns the stack trace of the given throwable as a string, or an empty string if it is null.
   *
   * @since 0.6.0
   */
  public static String getStackTrace(Throwable throwable) {
    if (throwable == null) {
      return "";
    }

    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    throwable.printStackTrace(pw);
    return sw.toString();
  }
}
