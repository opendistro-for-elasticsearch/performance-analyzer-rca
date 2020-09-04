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

public class OldGenDecisionPolicyConfigTest {

  @Test
  public void testConfigOverrides() throws Exception {
    final String configStr =
        "{"
            + "\"decider-config-settings\": { "
              + "\"old-gen-decision-policy-config\": { "
                + "\"queue-bucket-size\": 20, "
                + "\"old-gen-threshold-level-one\": 0.1, "
                + "\"old-gen-threshold-level-two\": 0.3, "
                + "\"old-gen-threshold-level-three\": 0.5 "
              + "} "
            + "} "
        + "} ";
    RcaConf conf = new RcaConf();
    conf.readConfigFromString(configStr);
    DeciderConfig deciderConfig = new DeciderConfig(conf);
    OldGenDecisionPolicyConfig oldGenDecisionPolicyConfig = deciderConfig.getOldGenDecisionPolicyConfig();
    Assert.assertNotNull(oldGenDecisionPolicyConfig);
    Assert.assertEquals(20, oldGenDecisionPolicyConfig.queueBucketSize());
    Assert.assertEquals(0.1, oldGenDecisionPolicyConfig.oldGenThresholdLevelOne(), 0.01);
    Assert.assertEquals(0.3, oldGenDecisionPolicyConfig.oldGenThresholdLevelTwo(), 0.01);
    Assert.assertEquals(0.5, oldGenDecisionPolicyConfig.oldGenThresholdLevelThree(), 0.01);
  }

  @Test
  public void testInvalidConfig() throws Exception {
    final String configStr =
        "{"
            + "\"decider-config-settings\": { "
              + "\"old-gen-decision-policy-config\": { "
                + "\"queue-bucket-size\": 0, "
                + "\"old-gen-threshold-level-one\": 0.5, "
                + "\"old-gen-threshold-level-two\": 0.4, "
                + "\"old-gen-threshold-level-three\": 0.3 "
              + "} "
            + "} "
        + "} ";
    RcaConf conf = new RcaConf();
    conf.readConfigFromString(configStr);
    DeciderConfig deciderConfig = new DeciderConfig(conf);
    OldGenDecisionPolicyConfig oldGenDecisionPolicyConfig = deciderConfig.getOldGenDecisionPolicyConfig();
    Assert.assertNotNull(oldGenDecisionPolicyConfig);
    Assert.assertEquals(OldGenDecisionPolicyConfig.DEFAULT_QUEUE_BUCKET_SIZE,
        oldGenDecisionPolicyConfig.queueBucketSize());
    Assert.assertEquals(0.5,
        oldGenDecisionPolicyConfig.oldGenThresholdLevelOne(), 0.01);
    Assert.assertEquals(OldGenDecisionPolicyConfig.DEFAULT_OLD_GEN_THRESHOLD_LEVEL_TWO,
        oldGenDecisionPolicyConfig.oldGenThresholdLevelTwo(), 0.01);
    Assert.assertEquals(OldGenDecisionPolicyConfig.DEFAULT_OLD_GEN_THRESHOLD_LEVEL_THREE,
        oldGenDecisionPolicyConfig.oldGenThresholdLevelThree(), 0.01);
  }

  @Test
  public void testDefaults() throws Exception {
    final String configStr = "{}";
    RcaConf conf = new RcaConf();
    conf.readConfigFromString(configStr);
    DeciderConfig deciderConfig = new DeciderConfig(conf);
    OldGenDecisionPolicyConfig oldGenDecisionPolicyConfig = deciderConfig
        .getOldGenDecisionPolicyConfig();
    Assert.assertNotNull(oldGenDecisionPolicyConfig);
    Assert.assertEquals(OldGenDecisionPolicyConfig.DEFAULT_QUEUE_BUCKET_SIZE,
        oldGenDecisionPolicyConfig.queueBucketSize());
    Assert.assertEquals(OldGenDecisionPolicyConfig.DEFAULT_OLD_GEN_THRESHOLD_LEVEL_ONE,
        oldGenDecisionPolicyConfig.oldGenThresholdLevelOne(), 0.01);
    Assert.assertEquals(OldGenDecisionPolicyConfig.DEFAULT_OLD_GEN_THRESHOLD_LEVEL_TWO,
        oldGenDecisionPolicyConfig.oldGenThresholdLevelTwo(), 0.01);
    Assert.assertEquals(OldGenDecisionPolicyConfig.DEFAULT_OLD_GEN_THRESHOLD_LEVEL_THREE,
        oldGenDecisionPolicyConfig.oldGenThresholdLevelThree(), 0.01);
  }
}
