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

package org.apache.spark.kubernetes.operator.decorators;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import lombok.RequiredArgsConstructor;

import org.apache.spark.kubernetes.operator.SparkApplication;

import static org.apache.spark.kubernetes.operator.reconciler.SparkReconcilerUtils.sparkAppResourceLabels;
import static org.apache.spark.kubernetes.operator.utils.ModelUtils.buildOwnerReferenceTo;

/**
 * Decorates driver (pod) to make sure its metadata matches event source
 * Also adds owner reference to the owner SparkApplication for garbage collection
 */
@RequiredArgsConstructor
public class DriverDecorator implements ResourceDecorator {

  private final SparkApplication app;

  /**
   * Add labels and owner references to the app for all secondary resources
   */
  @Override
  public <T extends HasMetadata> T decorate(T resource) {
    ObjectMeta metaData = new ObjectMetaBuilder(resource.getMetadata())
        .addToOwnerReferences(buildOwnerReferenceTo(app))
        .addToLabels(sparkAppResourceLabels(app))
        .withNamespace(app.getMetadata().getNamespace())
        .build();
    resource.setMetadata(metaData);
    return resource;
  }
}
