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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HighHeapUsageYoungGenRcaConfig {
  private static final Logger LOG = LogManager.getLogger(HighHeapUsageYoungGenRcaConfig.class);
  public static final String CONFIG_NAME = "high-heap-usage-young-gen-rca";
  private Integer promotionRateThreshold;
  private Integer youngGenGcTimeThreshold;
  //promotion rate threshold is 500 Mb/s
  public static final int DEFAULT_PROMOTION_RATE_THRESHOLD_IN_MB_PER_SEC = 500;
  //young gc time threshold is 400 ms per second
  public static final int DEFAULT_YOUNG_GEN_GC_TIME_THRESHOLD_IN_MS_PER_SEC = 400;

  public HighHeapUsageYoungGenRcaConfig(final RcaConf rcaConf) {
    promotionRateThreshold = rcaConf.readRcaConfig(CONFIG_NAME,
            RCA_CONF_KEY_CONSTANTS.PROMOTION_RATE_THRES, DEFAULT_PROMOTION_RATE_THRESHOLD_IN_MB_PER_SEC, (s) -> (s > 0), Integer.class);
    youngGenGcTimeThreshold = rcaConf.readRcaConfig(CONFIG_NAME,
            RCA_CONF_KEY_CONSTANTS.YOUNG_GEN_GC_TIME_THRES, DEFAULT_YOUNG_GEN_GC_TIME_THRESHOLD_IN_MS_PER_SEC,
            (s) -> (s > 0), Integer.class);
  }

  public int getPromotionRateThreshold() {
    return promotionRateThreshold;
  }

  public int getYoungGenGcTimeThreshold() {
    return youngGenGcTimeThreshold;
  }

  public static class RCA_CONF_KEY_CONSTANTS {
    public static final String PROMOTION_RATE_THRES = "promotion-rate-mb-per-second";
    public static final String YOUNG_GEN_GC_TIME_THRES = "young-gen-gc-time-ms-per-second";
  }
}
