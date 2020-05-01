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

import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class HighHeapUsageOldGenRcaConfigTest {

  private static final String TOP_K_RCA_CONF = "top-k";

  @Test
  public void testHighHeapUsageOldGenRcaConfig() {
    HighHeapUsageOldGenRcaConfig config = new HighHeapUsageOldGenRcaConfig(null);
    Assert.assertEquals(HighHeapUsageOldGenRcaConfig.DEFAULT_TOP_K, config.getTopK());

    Map<String, String> settings = new HashMap<>();
    settings.put(TOP_K_RCA_CONF, "5");
    config = new HighHeapUsageOldGenRcaConfig(settings);
    Assert.assertEquals(5, config.getTopK());

    settings.put(TOP_K_RCA_CONF, "5.8");
    config = new HighHeapUsageOldGenRcaConfig(settings);
    Assert.assertEquals(HighHeapUsageOldGenRcaConfig.DEFAULT_TOP_K, config.getTopK());

    settings.clear();
    settings.put("test", "2");
    config = new HighHeapUsageOldGenRcaConfig(settings);
    Assert.assertEquals(HighHeapUsageOldGenRcaConfig.DEFAULT_TOP_K, config.getTopK());
  }
}
