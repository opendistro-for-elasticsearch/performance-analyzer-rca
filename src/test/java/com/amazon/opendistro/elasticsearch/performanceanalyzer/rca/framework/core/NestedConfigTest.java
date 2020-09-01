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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import org.junit.Test;

public class NestedConfigTest {

  public static final String configStr =
      "{"
          + "\"action-config-settings\": { "
              + "\"cache-settings\": { "
                  + "\"fielddata\": { "
                      + "\"upper-bound\": 0.4, "
                      + "\"lower-bound\": 0.1, "
                      + "\"abcd\": { "
                          + "\"qwe\": { "
                              + "\"poi\": 4 "
                          + "} "
                      + "} "
                  + "}, "
                  + "\"shard-request\": { "
                      + "\"upper-bound\": 0.05, "
                      + "\"lower-bound\": 0.01 "
                  + "} "
              + "} "
          + "}"
      + "}";

  private final RcaConf conf;

  public NestedConfigTest() throws Exception {
    conf = new RcaConf();
    conf.readConfigFromString(configStr);
  }

  @Test
  public void testNoParentConfig() {
    final String testKey = "test-key";
    NestedConfig config = new NestedConfig(testKey, null);
    assertEquals(testKey, config.getKey());
    assertNull(config.getValue());
  }

  @Test
  public void testNestedConfig() {
    Map<String, Object> actionConfigSettings = conf.getActionConfigSettings();
    NestedConfig cacheSettings = new NestedConfig("cache-settings", actionConfigSettings);
    assertEquals(2, cacheSettings.getValue().size());
    assertTrue(cacheSettings.getValue().containsKey("fielddata"));
    assertTrue(cacheSettings.getValue().containsKey("shard-request"));

    NestedConfig fielddataConfig = new NestedConfig("fielddata", cacheSettings.getValue());
    assertEquals(3, fielddataConfig.getValue().size());
    assertTrue(fielddataConfig.getValue().containsKey("upper-bound"));
    assertTrue(fielddataConfig.getValue().containsKey("lower-bound"));
    assertTrue(fielddataConfig.getValue().containsKey("abcd"));

    NestedConfig abcdConfig = new NestedConfig("abcd", fielddataConfig.getValue());
    assertEquals(1, abcdConfig.getValue().size());
    assertTrue(abcdConfig.getValue().containsKey("qwe"));

    NestedConfig shardRequestConfig = new NestedConfig("shard-request", cacheSettings.getValue());
    assertEquals(2, shardRequestConfig.getValue().size());
    assertTrue(shardRequestConfig.getValue().containsKey("upper-bound"));
    assertTrue(shardRequestConfig.getValue().containsKey("lower-bound"));
  }

  @Test
  public void testConfigNotPresent() {
    Map<String, Object> actionConfigSettings = conf.getActionConfigSettings();
    NestedConfig cacheSettings = new NestedConfig("cache-settings", actionConfigSettings);
    NestedConfig testConfig = new NestedConfig("missing-test-key", cacheSettings.getValue());
    assertEquals("missing-test-key", testConfig.getKey());
    assertNull(testConfig.getValue());
  }

  @Test
  public void testNonNestedConfigValue() {
    Map<String, Object> actionConfigSettings = conf.getActionConfigSettings();
    NestedConfig cacheSettings = new NestedConfig("cache-settings", actionConfigSettings);
    NestedConfig shardRequestConfig = new NestedConfig("shard-request", cacheSettings.getValue());
    NestedConfig shardRequestUpperBound = new NestedConfig("upper-bound", shardRequestConfig.getValue());
    assertNull(shardRequestUpperBound.getValue());
  }
}
