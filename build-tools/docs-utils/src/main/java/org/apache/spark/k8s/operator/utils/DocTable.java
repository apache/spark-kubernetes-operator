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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
@Builder
public class DocTable {
  // Separator among cells
  public static final String ROW_SEPARATOR = " | ";
  // Separator for header line
  public static final String HEADER_SEPARATOR = "---";
  private final List<String> headers;
  private final int columns;
  @Builder.Default private final List<List<String>> rows = new ArrayList<>();

  public void addRow(List<String> row) {
    rows.add(row);
  }

  public void flush(PrintWriter writer) {
    writer.println(joinRow(headers));
    writer.println(joinRow(Collections.nCopies(columns, HEADER_SEPARATOR)));
    for (List<String> row : rows) {
      writer.println(joinRow(row));
    }
    writer.println();
    writer.flush();
  }

  private String joinRow(List<String> elements) {
    StringBuilder stringBuilder = new StringBuilder(ROW_SEPARATOR);
    for (String element : elements) {
      stringBuilder.append(element).append(ROW_SEPARATOR);
    }
    if (elements.size() < columns) {
      // Append empty cells to end if needed
      stringBuilder.append(ROW_SEPARATOR.repeat(columns - elements.size()));
    }
    return stringBuilder.toString();
  }
}
