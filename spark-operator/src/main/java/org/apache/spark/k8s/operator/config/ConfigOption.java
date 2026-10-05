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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.k8s.operator.utils.ModelUtils;
import org.apache.spark.k8s.operator.utils.StringUtils;

/**
 * Config options for Spark Operator. Supports primitive and serialized JSON.
 *
 * <p>Every option registers itself, keyed by {@link #key}, into a static registry on construction.
 * {@link SparkOperatorConfManager#refresh(Map)} consults {@link #dynamicOverrideEnabledKeys()} to
 * drop any dynamic-config update that targets an unknown key or one whose {@link
 * #enableDynamicOverride} is {@code false}.
 *
 * @param <T> The type of the config option's value.
 */
@EqualsAndHashCode
@ToString
@Slf4j
public class ConfigOption<T> {
  /** Indexes every declared option so dynamic-config refresh can check overridability. */
  private static final Map<String, ConfigOption<?>> REGISTRY = new ConcurrentHashMap<>();

  /** Invalid values already logged, as {@code key=value}, so that each is logged only once. */
  private static final Set<String> LOGGED_INVALID_VALUES = ConcurrentHashMap.newKeySet();

  /**
   * Whether this option may be overridden at runtime via dynamic config. Defaults to {@code false}
   * (opt-in): dynamic override must be explicitly enabled through the builder.
   */
  @Getter private final boolean enableDynamicOverride;
  @Getter private final String key;
  @Getter private final String description;
  @Getter private final T defaultValue;
  @Getter private final Class<T> typeParameterClass;

  @Builder
  ConfigOption(
      boolean enableDynamicOverride,
      String key,
      String description,
      T defaultValue,
      Class<T> typeParameterClass) {
    this.enableDynamicOverride = enableDynamicOverride;
    this.key = key;
    this.description = description;
    this.defaultValue = defaultValue;
    this.typeParameterClass = typeParameterClass;
    if (StringUtils.isNotEmpty(key)) {
      REGISTRY.put(key, this);
    }
  }

  /**
   * Returns the keys of all declared options that permit runtime dynamic override. Used as the
   * allow-list when refreshing dynamic config.
   *
   * <p><b>Note:</b> the returned set only includes options that have already been constructed.
   * Options are {@code static final} fields (e.g. on {@link SparkOperatorConf}) that register
   * themselves lazily on class initialization, so a caller that reads this before those classes
   * are initialized would see an empty or partial set. {@link
   * SparkOperatorConfManager#refresh(Map)} forces initialization via {@link
   * SparkOperatorConf#ensureOptionsRegistered()} first to avoid that; other callers must ensure
   * the relevant option classes are initialized.
   *
   * @return An immutable set of dynamically overridable config keys.
   */
  public static Set<String> dynamicOverrideEnabledKeys() {
    return REGISTRY.values().stream()
        .filter(ConfigOption::isEnableDynamicOverride)
        .map(ConfigOption::getKey)
        .collect(Collectors.toUnmodifiableSet());
  }

  /**
   * Returns the resolved value of the config option. An unset or empty value resolves to the
   * default value, and so does an invalid value, e.g. one which cannot be parsed or which resolves
   * to null like {@code null}, for which a warning is logged once.
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
    String value = SparkOperatorConfManager.INSTANCE.getValue(key);
    if (!enableDynamicOverride) {
      value = SparkOperatorConfManager.INSTANCE.getInitialValue(key);
    }
    try {
      if (StringUtils.isNotEmpty(value)) {
        if (typeParameterClass.isPrimitive()
            || typeParameterClass == String.class
            || typeParameterClass == Boolean.class) {
          return (T) resolveValueToPrimitiveType(typeParameterClass, value);
        } else {
          T resolvedValue = ModelUtils.objectMapper.readValue(value, typeParameterClass);
          // A JSON null like 'null' is not a valid value either.
          return resolvedValue == null ? defaultValueInsteadOf(value) : resolvedValue;
        }
      } else {
        return defaultValue;
      }
    } catch (IllegalArgumentException | JsonProcessingException e) {
      return defaultValueInsteadOf(value);
    }
  }

  /**
   * Returns the default value in place of the given invalid value. The warning is logged only the
   * first time the invalid value is seen for this key, since an option may be read on every
   * reconciliation, or even on every request to the API server.
   *
   * @param invalidValue The invalid value.
   * @return The default value.
   */
  private T defaultValueInsteadOf(String invalidValue) {
    if (LOGGED_INVALID_VALUES.add(key + "=" + invalidValue)) {
      log.warn(
          "Invalid {} value '{}' for config key {}, using default value {}",
          typeParameterClass.getSimpleName(),
          invalidValue,
          key,
          defaultValue);
    }
    return defaultValue;
  }

  /**
   * Resolves a string value to a primitive type or String. Like Apache Spark, a boolean value must
   * be 'true' or 'false' in any case, ignoring surrounding whitespace.
   *
   * @param clazz The class of the target type.
   * @param value The string value to resolve.
   * @return The resolved value as an Object.
   * @throws IllegalArgumentException If the value is invalid for the target type.
   */
  public static Object resolveValueToPrimitiveType(Class<?> clazz, String value) {
    if (Boolean.class == clazz || Boolean.TYPE == clazz) {
      String trimmed = value.trim();
      if ("true".equalsIgnoreCase(trimmed) || "false".equalsIgnoreCase(trimmed)) {
        return Boolean.parseBoolean(trimmed);
      }
      throw new IllegalArgumentException("Invalid boolean value: " + value);
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
