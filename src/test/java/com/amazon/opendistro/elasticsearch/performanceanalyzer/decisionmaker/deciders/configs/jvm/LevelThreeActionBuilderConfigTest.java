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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.configs.jvm;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.configs.DeciderConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import org.junit.Assert;
import org.junit.Test;

public class LevelThreeActionBuilderConfigTest {

  @Test
  public void testConfigOverrides() throws Exception {
    final String configStr =
        "{"
            + "\"decider-config-settings\": { "
              + "\"old-gen-decision-policy-config\": { "
                + "\"level-three-config\": { "
                  + "\"fielddata-cache-step-size\": 5, "
                  + "\"shard-request-cache-step-size\": 6, "
                  + "\"write-queue-step-size\": 7, "
                  + "\"search-queue-step-size\": 8 "
                + "} "
              + "} "
            + "} "
        + "} ";
    RcaConf conf = new RcaConf();
    conf.readConfigFromString(configStr);
    DeciderConfig deciderConfig = new DeciderConfig(conf);
    LevelThreeActionBuilderConfig actionBuilderConfig =
        deciderConfig.getOldGenDecisionPolicyConfig().levelThreeActionBuilderConfig();
    Assert.assertNotNull(actionBuilderConfig);
    Assert.assertEquals(7, actionBuilderConfig.writeQueueStepSize());
    Assert.assertEquals(8, actionBuilderConfig.searchQueueStepSize());
  }

  @Test
  public void testDefaults() throws Exception {
    final String configStr = "{}";
    RcaConf conf = new RcaConf();
    conf.readConfigFromString(configStr);
    DeciderConfig deciderConfig = new DeciderConfig(conf);
    LevelThreeActionBuilderConfig actionBuilderConfig =
        deciderConfig.getOldGenDecisionPolicyConfig().levelThreeActionBuilderConfig();
    Assert.assertNotNull(actionBuilderConfig);
    Assert.assertEquals(LevelThreeActionBuilderConfig.DEFAULT_WRITE_QUEUE_STEP_SIZE,
        actionBuilderConfig.writeQueueStepSize());
    Assert.assertEquals(LevelThreeActionBuilderConfig.DEFAULT_SEARCH_QUEUE_STEP_SIZE,
        actionBuilderConfig.searchQueueStepSize());
  }
}
