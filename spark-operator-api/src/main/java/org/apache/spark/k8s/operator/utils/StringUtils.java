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

/** Utility class for string operations. */
public final class StringUtils {
  private StringUtils() {}

  public static boolean isBlank(final String str) {
    return str == null || str.isBlank();
  }

  public static boolean isNotBlank(final String str) {
    return !isBlank(str);
  }

  public static boolean isEmpty(final String str) {
    return str == null || str.length() == 0;
  }

  public static boolean isNotEmpty(final String str) {
    return !isEmpty(str);
  }

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
