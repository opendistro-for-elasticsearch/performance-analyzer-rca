/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HotNodeClusterRcaConfig.RCA_CONF_KEY_CONSTANTS.UNBALANCED_RESOURCE_THRES;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class HotNodeClusterRcaConfigTest {


  @Test
  public void testHotNodeClusterRcaConfigTest() {
    HotNodeClusterRcaConfig config = new HotNodeClusterRcaConfig(null);
    Assert.assertEquals(HotNodeClusterRcaConfig.DEFAULT_UNBALANCED_RESOURCE_THRES, config.getUnbalancedResourceThreshold(), 0.01);

    Map<String, Object> settings = new HashMap<>();
    RcaConfigTestUtil.putToRcaMap(settings, config.getRcaName(), UNBALANCED_RESOURCE_THRES, 0.2);
    config = new HotNodeClusterRcaConfig(settings);
    Assert.assertEquals(0.2, config.getUnbalancedResourceThreshold(), 0.01);

    RcaConfigTestUtil.putToRcaMap(settings, config.getRcaName(), UNBALANCED_RESOURCE_THRES, 1);
    config = new HotNodeClusterRcaConfig(settings);
    Assert.assertEquals(HotNodeClusterRcaConfig.DEFAULT_UNBALANCED_RESOURCE_THRES, config.getUnbalancedResourceThreshold(), 0.01);

    RcaConfigTestUtil.putToRcaMap(settings, config.getRcaName(), UNBALANCED_RESOURCE_THRES, null);
    config = new HotNodeClusterRcaConfig(settings);
    Assert.assertEquals(HotNodeClusterRcaConfig.DEFAULT_UNBALANCED_RESOURCE_THRES, config.getUnbalancedResourceThreshold(), 0.01);

    RcaConfigTestUtil.clearRcaMap(settings, config.getRcaName());
    RcaConfigTestUtil.putToRcaMap(settings, config.getRcaName(), "test", 0.2);
    config = new HotNodeClusterRcaConfig(settings);
    Assert.assertEquals(HotNodeClusterRcaConfig.DEFAULT_UNBALANCED_RESOURCE_THRES, config.getUnbalancedResourceThreshold(), 0.01);
  }
}
