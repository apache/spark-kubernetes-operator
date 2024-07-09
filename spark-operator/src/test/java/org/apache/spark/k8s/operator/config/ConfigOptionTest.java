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

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConfigOptionTest {
  @Test
  void testResolveValueWithoutOverride() {
    byte defaultByteValue = 9;
    short defaultShortValue = 9;
    long defaultLongValue = 9;
    int defaultIntValue = 9;
    float defaultFloatValue = 9.0f;
    double defaultDoubleValue = 9.0;
    boolean defaultBooleanValue = false;
    String defaultStringValue = "bar";
    ConfigOption<String> testStrConf =
        ConfigOption.<String>builder()
            .key("foo")
            .typeParameterClass(String.class)
            .description("foo foo.")
            .defaultValue(defaultStringValue)
            .build();
    ConfigOption<Integer> testIntConf =
        ConfigOption.<Integer>builder()
            .key("fooint")
            .typeParameterClass(Integer.class)
            .description("foo foo.")
            .defaultValue(defaultIntValue)
            .build();
    ConfigOption<Short> testShortConf =
        ConfigOption.<Short>builder()
            .key("fooshort")
            .typeParameterClass(Short.class)
            .description("foo foo.")
            .defaultValue(defaultShortValue)
            .build();
    ConfigOption<Long> testLongConf =
        ConfigOption.<Long>builder()
            .key("foolong")
            .typeParameterClass(Long.class)
            .description("foo foo.")
            .defaultValue(defaultLongValue)
            .build();
    ConfigOption<Boolean> testBooleanConf =
        ConfigOption.<Boolean>builder()
            .key("foobool")
            .typeParameterClass(Boolean.class)
            .description("foo foo.")
            .defaultValue(defaultBooleanValue)
            .build();
    ConfigOption<Float> testFloatConf =
        ConfigOption.<Float>builder()
            .key("foofloat")
            .typeParameterClass(Float.class)
            .description("foo foo.")
            .defaultValue(defaultFloatValue)
            .build();
    ConfigOption<Double> testDoubleConf =
        ConfigOption.<Double>builder()
            .key("foodouble")
            .typeParameterClass(Double.class)
            .description("foo foo.")
            .defaultValue(defaultDoubleValue)
            .build();
    ConfigOption<Byte> testByteConf =
        ConfigOption.<Byte>builder()
            .key("foobyte")
            .typeParameterClass(Byte.class)
            .description("foo foo.")
            .defaultValue(defaultByteValue)
            .build();
    Assertions.assertEquals(defaultStringValue, testStrConf.getValue());
    Assertions.assertEquals(defaultIntValue, testIntConf.getValue());
    Assertions.assertEquals(defaultLongValue, testLongConf.getValue());
    Assertions.assertEquals(defaultBooleanValue, testBooleanConf.getValue());
    Assertions.assertEquals(defaultFloatValue, testFloatConf.getValue());
    Assertions.assertEquals(defaultByteValue, testByteConf.getValue());
    Assertions.assertEquals(defaultShortValue, testShortConf.getValue());
    Assertions.assertEquals(defaultDoubleValue, testDoubleConf.getValue());
  }

  @Test
  void testResolveValueWithOverride() {
    byte overrideByteValue = 10;
    short overrideShortValue = 10;
    long overrideLongValue = 10;
    int overrideIntValue = 10;
    float overrideFloatValue = 10.0f;
    double overrideDoubleValue = 10.0;
    boolean overrideBooleanValue = true;
    String overrideStringValue = "barbar";
    byte defaultByteValue = 9;
    short defaultShortValue = 9;
    long defaultLongValue = 9;
    int defaultIntValue = 9;
    float defaultFloatValue = 9.0f;
    double defaultDoubleValue = 9.0;
    boolean defaultBooleanValue = false;
    String defaultStringValue = "bar";
    Map<String, String> configOverride = new HashMap<>();
    configOverride.put("foobyte", "10");
    configOverride.put("fooshort", "10");
    configOverride.put("foolong", "10");
    configOverride.put("fooint", "10");
    configOverride.put("foofloat", "10.0");
    configOverride.put("foodouble", "10.0");
    configOverride.put("foobool", "true");
    configOverride.put("foo", "barbar");
    SparkOperatorConfManager.INSTANCE.refresh(configOverride);
    ConfigOption<String> testStrConf =
        ConfigOption.<String>builder()
            .key("foo")
            .typeParameterClass(String.class)
            .description("foo foo.")
            .defaultValue(defaultStringValue)
            .build();
    ConfigOption<Integer> testIntConf =
        ConfigOption.<Integer>builder()
            .key("fooint")
            .typeParameterClass(Integer.class)
            .description("foo foo.")
            .defaultValue(defaultIntValue)
            .build();
    ConfigOption<Short> testShortConf =
        ConfigOption.<Short>builder()
            .key("fooshort")
            .typeParameterClass(Short.class)
            .description("foo foo.")
            .defaultValue(defaultShortValue)
            .build();
    ConfigOption<Long> testLongConf =
        ConfigOption.<Long>builder()
            .key("foolong")
            .typeParameterClass(Long.class)
            .description("foo foo.")
            .defaultValue(defaultLongValue)
            .build();
    ConfigOption<Boolean> testBooleanConf =
        ConfigOption.<Boolean>builder()
            .key("foobool")
            .typeParameterClass(Boolean.class)
            .description("foo foo.")
            .defaultValue(defaultBooleanValue)
            .build();
    ConfigOption<Float> testFloatConf =
        ConfigOption.<Float>builder()
            .key("foofloat")
            .typeParameterClass(Float.class)
            .description("foo foo.")
            .defaultValue(defaultFloatValue)
            .build();
    ConfigOption<Double> testDoubleConf =
        ConfigOption.<Double>builder()
            .key("foodouble")
            .typeParameterClass(Double.class)
            .description("foo foo.")
            .defaultValue(defaultDoubleValue)
            .build();
    ConfigOption<Byte> testByteConf =
        ConfigOption.<Byte>builder()
            .key("foobyte")
            .typeParameterClass(Byte.class)
            .description("foo foo.")
            .defaultValue(defaultByteValue)
            .build();
    Assertions.assertEquals(overrideStringValue, testStrConf.getValue());
    Assertions.assertEquals(overrideIntValue, testIntConf.getValue());
    Assertions.assertEquals(overrideLongValue, testLongConf.getValue());
    Assertions.assertEquals(overrideBooleanValue, testBooleanConf.getValue());
    Assertions.assertEquals(overrideFloatValue, testFloatConf.getValue());
    Assertions.assertEquals(overrideByteValue, testByteConf.getValue());
    Assertions.assertEquals(overrideShortValue, testShortConf.getValue());
    Assertions.assertEquals(overrideDoubleValue, testDoubleConf.getValue());
  }
}
