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

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HighHeapUsageOldGenRcaConfig.RCA_CONF_KEY_CONSTANTS.TOP_K;

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class HighHeapUsageOldGenRcaConfigTest {

  @Test
  public void testHighHeapUsageOldGenRcaConfig() {
    HighHeapUsageOldGenRcaConfig config = new HighHeapUsageOldGenRcaConfig(null);
    Assert.assertEquals(HighHeapUsageOldGenRcaConfig.DEFAULT_TOP_K, config.getTopK());

    Map<String, Object> settings = new HashMap<>();
    RcaConfigTestUtil.putToRcaMap(settings, config.getRcaName(), TOP_K, 5);
    config = new HighHeapUsageOldGenRcaConfig(settings);
    Assert.assertEquals(5, config.getTopK());

    RcaConfigTestUtil.putToRcaMap(settings, config.getRcaName(), TOP_K, 5.8);
    config = new HighHeapUsageOldGenRcaConfig(settings);
    Assert.assertEquals(HighHeapUsageOldGenRcaConfig.DEFAULT_TOP_K, config.getTopK());

    RcaConfigTestUtil.putToRcaMap(settings, config.getRcaName(), TOP_K, null);
    config = new HighHeapUsageOldGenRcaConfig(settings);
    Assert.assertEquals(HighHeapUsageOldGenRcaConfig.DEFAULT_TOP_K, config.getTopK());

    RcaConfigTestUtil.clearRcaMap(settings, config.getRcaName());
    RcaConfigTestUtil.putToRcaMap(settings, config.getRcaName(), "test", 2);
    config = new HighHeapUsageOldGenRcaConfig(settings);
    Assert.assertEquals(HighHeapUsageOldGenRcaConfig.DEFAULT_TOP_K, config.getTopK());
  }
}
