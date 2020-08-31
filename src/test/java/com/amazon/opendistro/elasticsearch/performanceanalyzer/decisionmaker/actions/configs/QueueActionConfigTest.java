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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.configs;

import static org.junit.Assert.assertEquals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import org.junit.Test;

public class QueueActionConfigTest {

  @Test
  public void testConfigOverrides() throws Exception {
    final String configStr =
      "{"
          + "\"action-config-settings\": { "
              + "\"queue-settings\": { "
                  + "\"search\": { "
                      + "\"upper-bound\": 500, "
                      + "\"lower-bound\": 100 "
                  + "}, "
                  + "\"write\": { "
                      + "\"upper-bound\": 50, "
                      + "\"lower-bound\": 10 "
                  + "} "
              + "} "
          + "} "
      + "}";
    RcaConf conf = new RcaConf();
    conf.readConfigFromString(configStr);
    QueueActionConfig queueActionConfig = new QueueActionConfig(conf);
    assertEquals(500, (int) queueActionConfig.getThresholdConfig(ResourceEnum.SEARCH_THREADPOOL).upperBound());
    assertEquals(100, (int) queueActionConfig.getThresholdConfig(ResourceEnum.SEARCH_THREADPOOL).lowerBound());
    assertEquals(50, (int) queueActionConfig.getThresholdConfig(ResourceEnum.WRITE_THREADPOOL).upperBound());
    assertEquals(10, (int) queueActionConfig.getThresholdConfig(ResourceEnum.WRITE_THREADPOOL).lowerBound());
  }

  @Test
  public void testDefaults() throws Exception {
    final String configStr = "{}";
    RcaConf conf = new RcaConf();
    conf.readConfigFromString(configStr);
    QueueActionConfig queueActionConfig = new QueueActionConfig(conf);
    assertEquals(3000, (int) queueActionConfig.getThresholdConfig(ResourceEnum.SEARCH_THREADPOOL).upperBound());
    assertEquals(500, (int) queueActionConfig.getThresholdConfig(ResourceEnum.SEARCH_THREADPOOL).lowerBound());
    assertEquals(1000, (int) queueActionConfig.getThresholdConfig(ResourceEnum.WRITE_THREADPOOL).upperBound());
    assertEquals(50, (int) queueActionConfig.getThresholdConfig(ResourceEnum.WRITE_THREADPOOL).lowerBound());
  }

  @Test
  public void testInvalidConfigValues() throws Exception {
    final String configStr =
      "{"
          + "\"action-config-settings\": { "
              + "\"queue-settings\": { "
                  + "\"search\": { "
                      + "\"upper-bound\": -1, "
                      + "\"lower-bound\": \"abc\" "
                  + "}, "
                  + "\"write\": { "
                      + "\"upper-bound\": -1, "
                      + "\"lower-bound\": -1 "
                  + "} "
              + "} "
          + "} "
      + "}";
    RcaConf conf = new RcaConf();
    conf.readConfigFromString(configStr);

    // Invalid values in config, should resolve back to defaults
    QueueActionConfig queueActionConfig = new QueueActionConfig(conf);
    assertEquals(3000, (int) queueActionConfig.getThresholdConfig(ResourceEnum.SEARCH_THREADPOOL).upperBound());
    assertEquals(500, (int) queueActionConfig.getThresholdConfig(ResourceEnum.SEARCH_THREADPOOL).lowerBound());
    assertEquals(1000, (int) queueActionConfig.getThresholdConfig(ResourceEnum.WRITE_THREADPOOL).upperBound());
    assertEquals(50, (int) queueActionConfig.getThresholdConfig(ResourceEnum.WRITE_THREADPOOL).lowerBound());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidThreadpool() throws Exception {
    final String configStr = "{}";
    RcaConf conf = new RcaConf();
    conf.readConfigFromString(configStr);
    QueueActionConfig queueActionConfig = new QueueActionConfig(conf);
    queueActionConfig.getThresholdConfig(ResourceEnum.FIELD_DATA_CACHE).upperBound();
  }
}
