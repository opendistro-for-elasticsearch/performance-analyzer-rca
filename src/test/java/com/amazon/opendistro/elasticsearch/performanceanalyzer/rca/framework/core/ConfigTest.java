/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

import static org.junit.Assert.assertEquals;

import java.util.Map;
import org.junit.Test;

public class ConfigTest {

  public static final String configStr =
      "{"
          + "\"action-config-settings\": { "
              + "\"test-config\": { "
                  + "\"test-key-string\": \"value-1\", "
                  + "\"test-key-double\": 0.5, "
                  + "\"test-key-int\": 7 "
              + "} "
          + "}"
      + "}";

  private final RcaConf conf;
  private NestedConfig testConfig;

  public ConfigTest() throws Exception {
    conf = new RcaConf();
    conf.readConfigFromString(configStr);
    Map<String, Object> actionConfigSettings = conf.getActionConfigSettings();
    testConfig = new NestedConfig("test-config", actionConfigSettings);
  }

  @Test
  public void testConfig() {
    Config<String> testKeyString = new Config<>("test-key-string", testConfig.getValue(), "default-val", String.class);
    assertEquals("test-key-string", testKeyString.getKey());
    assertEquals("value-1", testKeyString.getValue());

    Config<Double> testKeyDouble = new Config<>("test-key-double", testConfig.getValue(), 0.9, Double.class);
    assertEquals("test-key-double", testKeyDouble.getKey());
    assertEquals(0.5, testKeyDouble.getValue(), 0.000001);

    Config<Integer> testKeyInt = new Config<>("test-key-int", testConfig.getValue(), 11, Integer.class);
    assertEquals("test-key-int", testKeyInt.getKey());
    assertEquals(7, (int) testKeyInt.getValue());
  }

  @Test
  public void testDefaults() {
    Config<String> testString = new Config<>("random-key", testConfig.getValue(), "default-val", String.class);
    assertEquals("random-key", testString.getKey());
    assertEquals("default-val", testString.getValue());

    Config<Double> testDouble = new Config<>("abc", testConfig.getValue(), 0.15, Double.class);
    assertEquals("abc", testDouble.getKey());
    assertEquals(0.15, testDouble.getValue(), 0.0000001);

    Config<Integer> testInt = new Config<>("def", testConfig.getValue(), 42, Integer.class);
    assertEquals("def", testInt.getKey());
    assertEquals(42, (int) testInt.getValue());
  }

  @Test
  public void testTypeMismatch() {
    Config<Double> testKey = new Config<>("test-key-string", testConfig.getValue(), 0.15, Double.class);
    assertEquals(0.15, testKey.getValue(), 0.000001); // In case of type mismatch, we use default value
  }

  @Test
  public void testInvalidConfig() {
    Config<Integer> test = new Config<>("test-key-int", testConfig.getValue(), 15, (v) -> (v >= 10), Integer.class);
    assertEquals(15, (int) test.getValue());
  }

}
