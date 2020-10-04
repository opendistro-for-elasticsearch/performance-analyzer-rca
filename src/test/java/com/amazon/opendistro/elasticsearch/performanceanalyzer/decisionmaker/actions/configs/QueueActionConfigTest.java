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
    assertEquals(QueueActionConfig.DEFAULT_SEARCH_QUEUE_UPPER_BOUND,
        (int) queueActionConfig.getThresholdConfig(ResourceEnum.SEARCH_THREADPOOL).upperBound());
    assertEquals(QueueActionConfig.DEFAULT_SEARCH_QUEUE_LOWER_BOUND,
        (int) queueActionConfig.getThresholdConfig(ResourceEnum.SEARCH_THREADPOOL).lowerBound());
    assertEquals(QueueActionConfig.DEFAULT_WRITE_QUEUE_UPPER_BOUND,
        (int) queueActionConfig.getThresholdConfig(ResourceEnum.WRITE_THREADPOOL).upperBound());
    assertEquals(QueueActionConfig.DEFAULT_WRITE_QUEUE_LOWER_BOUND,
        (int) queueActionConfig.getThresholdConfig(ResourceEnum.WRITE_THREADPOOL).lowerBound());
    assertEquals(QueueActionConfig.DEFAULT_COOL_OFF_PERIOD_IN_SECONDS, queueActionConfig.getCoolOffPeriodInSeconds());
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
    assertEquals(QueueActionConfig.DEFAULT_SEARCH_QUEUE_UPPER_BOUND,
        (int) queueActionConfig.getThresholdConfig(ResourceEnum.SEARCH_THREADPOOL).upperBound());
    assertEquals(QueueActionConfig.DEFAULT_SEARCH_QUEUE_LOWER_BOUND,
        (int) queueActionConfig.getThresholdConfig(ResourceEnum.SEARCH_THREADPOOL).lowerBound());
    assertEquals(QueueActionConfig.DEFAULT_WRITE_QUEUE_UPPER_BOUND,
        (int) queueActionConfig.getThresholdConfig(ResourceEnum.WRITE_THREADPOOL).upperBound());
    assertEquals(QueueActionConfig.DEFAULT_WRITE_QUEUE_LOWER_BOUND,
        (int) queueActionConfig.getThresholdConfig(ResourceEnum.WRITE_THREADPOOL).lowerBound());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidThreadpool() throws Exception {
    final String configStr = "{}";
    RcaConf conf = new RcaConf();
    conf.readConfigFromString(configStr);
    QueueActionConfig queueActionConfig = new QueueActionConfig(conf);
    queueActionConfig.getThresholdConfig(ResourceEnum.FIELD_DATA_CACHE).upperBound();
  }

  @Test
  public void testGetStepSize() throws Exception {
    String configStr =
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
    assertEquals(20, queueActionConfig.getStepSize(ResourceEnum.SEARCH_THREADPOOL));
    assertEquals(2, queueActionConfig.getStepSize(ResourceEnum.WRITE_THREADPOOL));

    configStr =
      "{"
          + "\"action-config-settings\": { "
              + "\"queue-settings\": { "
                  + "\"total-step-count\": 10,"
                  + "\"cool-off-period-in-seconds\": 5,"
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
    conf.readConfigFromString(configStr);
    queueActionConfig = new QueueActionConfig(conf);
    assertEquals(5, queueActionConfig.getCoolOffPeriodInSeconds());
    assertEquals(40, queueActionConfig.getStepSize(ResourceEnum.SEARCH_THREADPOOL));
    assertEquals(4, queueActionConfig.getStepSize(ResourceEnum.WRITE_THREADPOOL));
  }
}
