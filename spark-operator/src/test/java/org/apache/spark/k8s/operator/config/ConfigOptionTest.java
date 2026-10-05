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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConfigOptionTest {
  @AfterEach
  void resetConf() {
    SparkOperatorConfManager.INSTANCE.refresh(Map.of());
  }

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
    ConfigOption<String> testStrConf =
        ConfigOption.<String>builder()
            .key("foo")
            .enableDynamicOverride(true)
            .typeParameterClass(String.class)
            .description("foo foo.")
            .defaultValue(defaultStringValue)
            .build();
    ConfigOption<Integer> testIntConf =
        ConfigOption.<Integer>builder()
            .key("fooint")
            .enableDynamicOverride(true)
            .typeParameterClass(Integer.class)
            .description("foo foo.")
            .defaultValue(defaultIntValue)
            .build();
    ConfigOption<Short> testShortConf =
        ConfigOption.<Short>builder()
            .key("fooshort")
            .enableDynamicOverride(true)
            .typeParameterClass(Short.class)
            .description("foo foo.")
            .defaultValue(defaultShortValue)
            .build();
    ConfigOption<Long> testLongConf =
        ConfigOption.<Long>builder()
            .key("foolong")
            .enableDynamicOverride(true)
            .typeParameterClass(Long.class)
            .description("foo foo.")
            .defaultValue(defaultLongValue)
            .build();
    ConfigOption<Boolean> testBooleanConf =
        ConfigOption.<Boolean>builder()
            .key("foobool")
            .enableDynamicOverride(true)
            .typeParameterClass(Boolean.class)
            .description("foo foo.")
            .defaultValue(defaultBooleanValue)
            .build();
    ConfigOption<Float> testFloatConf =
        ConfigOption.<Float>builder()
            .key("foofloat")
            .enableDynamicOverride(true)
            .typeParameterClass(Float.class)
            .description("foo foo.")
            .defaultValue(defaultFloatValue)
            .build();
    ConfigOption<Double> testDoubleConf =
        ConfigOption.<Double>builder()
            .key("foodouble")
            .enableDynamicOverride(true)
            .typeParameterClass(Double.class)
            .description("foo foo.")
            .defaultValue(defaultDoubleValue)
            .build();
    ConfigOption<Byte> testByteConf =
        ConfigOption.<Byte>builder()
            .key("foobyte")
            .enableDynamicOverride(true)
            .typeParameterClass(Byte.class)
            .description("foo foo.")
            .defaultValue(defaultByteValue)
            .build();
    // The options above opt in to dynamic override, so refreshing after they are built lets the
    // allow-list filter retain these keys.
    SparkOperatorConfManager.INSTANCE.refresh(configOverride);
    Assertions.assertEquals(overrideStringValue, testStrConf.getValue());
    Assertions.assertEquals(overrideIntValue, testIntConf.getValue());
    Assertions.assertEquals(overrideLongValue, testLongConf.getValue());
    Assertions.assertEquals(overrideBooleanValue, testBooleanConf.getValue());
    Assertions.assertEquals(overrideFloatValue, testFloatConf.getValue());
    Assertions.assertEquals(overrideByteValue, testByteConf.getValue());
    Assertions.assertEquals(overrideShortValue, testShortConf.getValue());
    Assertions.assertEquals(overrideDoubleValue, testDoubleConf.getValue());
  }

  @Test
  void testResolveBooleanValueIgnoringCaseAndSurroundingWhitespace() {
    ConfigOption<Boolean> falseByDefault = dynamicOption("fooboolcasetrue", Boolean.class, false);
    for (String value : List.of("true", "TRUE", "True", " true ", "true ")) {
      SparkOperatorConfManager.INSTANCE.refresh(Map.of(falseByDefault.getKey(), value));
      Assertions.assertTrue(falseByDefault.getValue(), value);
    }
    ConfigOption<Boolean> trueByDefault = dynamicOption("fooboolcasefalse", Boolean.class, true);
    for (String value : List.of("false", "FALSE", "False", " false ", "false ")) {
      SparkOperatorConfManager.INSTANCE.refresh(Map.of(trueByDefault.getKey(), value));
      Assertions.assertFalse(trueByDefault.getValue(), value);
    }
  }

  @Test
  void testResolveInvalidValueToDefaultValue() {
    // Unlike Boolean.parseBoolean, an invalid value does not turn off an option enabled by default.
    ConfigOption<Boolean> trueByDefault = dynamicOption("fooboolinvalidtrue", Boolean.class, true);
    for (String value : List.of("0", "off", "no", "f", "\"false\"", "null")) {
      SparkOperatorConfManager.INSTANCE.refresh(Map.of(trueByDefault.getKey(), value));
      Assertions.assertTrue(trueByDefault.getValue(), value);
    }
    ConfigOption<Boolean> falseByDefault =
        dynamicOption("fooboolinvalidfalse", Boolean.class, false);
    for (String value : List.of("1", "-1", "on", "yes", "t", "\"true\"", "true,false")) {
      SparkOperatorConfManager.INSTANCE.refresh(Map.of(falseByDefault.getKey(), value));
      Assertions.assertFalse(falseByDefault.getValue(), value);
    }
    // A value which resolves to null is invalid for any type.
    ConfigOption<Long> longConf = dynamicOption("foolonginvalid", Long.class, 9L);
    for (String value : List.of("null", "\"\"", "\"null\"", "abc")) {
      SparkOperatorConfManager.INSTANCE.refresh(Map.of(longConf.getKey(), value));
      Assertions.assertEquals(9L, longConf.getValue(), value);
    }
  }

  @Test
  void testLogEachInvalidValueOnce() {
    ConfigOption<Boolean> testBooleanConf = dynamicOption("fooboolwarn", Boolean.class, true);
    TestLogAppender appender = new TestLogAppender();
    appender.start();
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    ctx.getConfiguration().getRootLogger().addAppender(appender, Level.WARN, null);
    ctx.updateLoggers();
    try {
      for (String value : List.of("yes", "no")) {
        SparkOperatorConfManager.INSTANCE.refresh(Map.of(testBooleanConf.getKey(), value));
        Assertions.assertTrue(testBooleanConf.getValue());
        Assertions.assertTrue(testBooleanConf.getValue());
      }
      Assertions.assertEquals(
          List.of(
              "Invalid Boolean value 'yes' for config key fooboolwarn, using default value true",
              "Invalid Boolean value 'no' for config key fooboolwarn, using default value true"),
          appender.messages);
    } finally {
      ctx.getConfiguration().getRootLogger().removeAppender(appender.getName());
      ctx.updateLoggers();
      appender.stop();
    }
  }

  private static <T> ConfigOption<T> dynamicOption(String key, Class<T> clazz, T defaultValue) {
    return ConfigOption.<T>builder()
        .key(key)
        .enableDynamicOverride(true)
        .typeParameterClass(clazz)
        .description("foo foo.")
        .defaultValue(defaultValue)
        .build();
  }

  /** Collects the messages logged by {@link ConfigOption}. */
  private static final class TestLogAppender extends AbstractAppender {
    private final List<String> messages = new CopyOnWriteArrayList<>();

    private TestLogAppender() {
      super(
          "ConfigOptionTestLogAppender",
          null,
          PatternLayout.createDefaultLayout(),
          false,
          Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent event) {
      if (ConfigOption.class.getName().equals(event.getLoggerName())) {
        messages.add(event.getMessage().getFormattedMessage());
      }
    }
  }
}
