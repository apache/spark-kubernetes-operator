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

package org.apache.spark.kubernetes.operator.config;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

class SparkOperatorConfManagerTest {
    @Test
    void testLoadPropertiesFromInitFile() throws IOException {
        String propBackUp = System.getProperty("spark.operator.base.property.file.name");
        try {
            String propsFilePath = SparkOperatorConfManagerTest.class.getClassLoader()
                    .getResource("spark-operator.properties").getPath();
            System.setProperty("spark.operator.base.property.file.name", propsFilePath);
            SparkOperatorConfManager confManager = new SparkOperatorConfManager();
            Assertions.assertEquals("bar", confManager.getValue("spark.operator.foo"));
        } finally {
            if (StringUtils.isNotEmpty(propBackUp)) {
                System.setProperty("spark.operator.base.property.file.name", propBackUp);
            } else {
                System.clearProperty("spark.operator.base.property.file.name");
            }
        }
    }

    @Test
    void testOverrideProperties() {
        String propBackUp = System.getProperty("spark.operator.foo");
        System.setProperty("spark.operator.foo", "bar");
        try {
            SparkOperatorConfManager confManager = new SparkOperatorConfManager();
            Assertions.assertEquals("bar", confManager.getInitialValue("spark.operator.foo"));
            Assertions.assertEquals("bar", confManager.getValue("spark.operator.foo"));

            confManager.refresh(Collections.singletonMap("spark.operator.foo", "barbar"));
            Assertions.assertEquals("bar", confManager.getInitialValue("spark.operator.foo"));
            Assertions.assertEquals("barbar", confManager.getValue("spark.operator.foo"));

            confManager.refresh(Collections.singletonMap("spark.operator.foo", "barbarbar"));
            Assertions.assertEquals("bar", confManager.getInitialValue("spark.operator.foo"));
            Assertions.assertEquals("barbarbar", confManager.getValue("spark.operator.foo"));

        } finally {
            if (StringUtils.isNotEmpty(propBackUp)) {
                System.setProperty("spark.operator.foo", propBackUp);
            } else {
                System.clearProperty("spark.operator.foo");
            }
        }
    }
}
