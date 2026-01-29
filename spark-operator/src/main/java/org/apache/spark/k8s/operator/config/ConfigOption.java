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

package org.apache.spark.k8s.operator.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.utils.ModelUtils;
import org.apache.spark.k8s.operator.utils.StringUtils;

/**
 * Config options for Spark Operator. Supports primitive and serialized JSON.
 *
 * @param <T> The type of the config option's value.
 */
@RequiredArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
@ToString
@Builder
@Slf4j
public class ConfigOption<T> {
  @Getter @Builder.Default private final boolean enableDynamicOverride = true;
  @Getter private String key;
  @Getter private String description;
  @Getter private T defaultValue;
  @Getter private Class<T> typeParameterClass;

  /**
   * Returns the resolved value of the config option.
   *
   * @return The resolved value.
   */
  public T getValue() {
    T resolvedValue = resolveValue();
    if (log.isDebugEnabled()) {
      log.debug("Resolved value for property {}={}", key, resolvedValue);
    }
    return resolvedValue;
  }

  private T resolveValue() {
    try {
      String value = SparkOperatorConfManager.INSTANCE.getValue(key);
      if (!enableDynamicOverride) {
        value = SparkOperatorConfManager.INSTANCE.getInitialValue(key);
      }
      if (StringUtils.isNotEmpty(value)) {
        if (typeParameterClass.isPrimitive() || typeParameterClass == String.class) {
          return (T) resolveValueToPrimitiveType(typeParameterClass, value);
        } else {
          return ModelUtils.objectMapper.readValue(value, typeParameterClass);
        }
      } else {
        return defaultValue;
      }
    } catch (NumberFormatException | JsonProcessingException t) {
      log.error(
          "Failed to resolve value for config key {}, using default value {}",
          key,
          defaultValue,
          t);
      return defaultValue;
    }
  }

  /**
   * Resolves a string value to a primitive type or String.
   *
   * @param clazz The class of the target type.
   * @param value The string value to resolve.
   * @return The resolved value as an Object.
   */
  public static Object resolveValueToPrimitiveType(Class<?> clazz, String value) {
    if (Boolean.class == clazz || Boolean.TYPE == clazz) {
      return Boolean.parseBoolean(value);
    }
    if (Byte.class == clazz || Byte.TYPE == clazz) {
      return Byte.parseByte(value);
    }
    if (Short.class == clazz || Short.TYPE == clazz) {
      return Short.parseShort(value);
    }
    if (Integer.class == clazz || Integer.TYPE == clazz) {
      return Integer.parseInt(value);
    }
    if (Long.class == clazz || Long.TYPE == clazz) {
      return Long.parseLong(value);
    }
    if (Float.class == clazz || Float.TYPE == clazz) {
      return Float.parseFloat(value);
    }
    if (Double.class == clazz || Double.TYPE == clazz) {
      return Double.parseDouble(value);
    }
    return value;
  }
}
