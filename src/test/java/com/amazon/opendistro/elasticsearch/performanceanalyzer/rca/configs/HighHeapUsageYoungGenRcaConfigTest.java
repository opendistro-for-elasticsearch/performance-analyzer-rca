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

public class HighHeapUsageYoungGenRcaConfigTest {
  private static final String PROMOTION_RATE_THRES = "promotion-rate-threshold";

  @Test
  public void testHighHeapUsageYoungGenRcaConfig() {
    HighHeapUsageYoungGenRcaConfig config = new HighHeapUsageYoungGenRcaConfig(null);
    Assert.assertEquals(HighHeapUsageYoungGenRcaConfig.DEFAULT_PROMOTION_RATE_THRESHOLD_IN_MB_PER_SEC, config.getPromotionRateThreshold());

    Map<String, Object> settings = new HashMap<>();
    settings.put(PROMOTION_RATE_THRES, 700);
    config = new HighHeapUsageYoungGenRcaConfig(settings);
    Assert.assertEquals(700, config.getPromotionRateThreshold());

    settings.put(PROMOTION_RATE_THRES, 800.0);
    config = new HighHeapUsageYoungGenRcaConfig(settings);
    Assert.assertEquals(HighHeapUsageYoungGenRcaConfig.DEFAULT_PROMOTION_RATE_THRESHOLD_IN_MB_PER_SEC, config.getPromotionRateThreshold());

    settings.put(PROMOTION_RATE_THRES, null);
    config = new HighHeapUsageYoungGenRcaConfig(settings);
    Assert.assertEquals(HighHeapUsageYoungGenRcaConfig.DEFAULT_PROMOTION_RATE_THRESHOLD_IN_MB_PER_SEC, config.getPromotionRateThreshold());

    settings.clear();
    settings.put("test", 700);
    config = new HighHeapUsageYoungGenRcaConfig(settings);
    Assert.assertEquals(HighHeapUsageYoungGenRcaConfig.DEFAULT_PROMOTION_RATE_THRESHOLD_IN_MB_PER_SEC, config.getPromotionRateThreshold());
  }
}
