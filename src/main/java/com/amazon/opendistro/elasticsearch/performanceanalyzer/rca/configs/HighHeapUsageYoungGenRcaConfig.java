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

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HighHeapUsageYoungGenRcaConfig extends GenericRcaConfig {
  private static final Logger LOG = LogManager.getLogger(HighHeapUsageYoungGenRcaConfig.class);
  public static final String CONFIG_NAME = "high-heap-usage-young-gen-rca";
  private int promotionRateThreshold;
  private int youngGenGcTimeThreshold;
  //promotion rate threshold is 500 Mb/s
  public static final int DEFAULT_PROMOTION_RATE_THRESHOLD_IN_MB_PER_SEC = 500;
  //young gc time threshold is 400 ms per second
  public static final int DEFAULT_YOUNG_GEN_GC_TIME_THRESHOLD_IN_MS_PER_SEC = 400;

  public HighHeapUsageYoungGenRcaConfig(final Map<String, Object> rcaConfigSettings) {
    this.promotionRateThreshold = DEFAULT_PROMOTION_RATE_THRESHOLD_IN_MB_PER_SEC;
    this.youngGenGcTimeThreshold = DEFAULT_YOUNG_GEN_GC_TIME_THRESHOLD_IN_MS_PER_SEC;
    parseConfig(rcaConfigSettings);
  }

  @Override
  public String getRcaName() {
    return CONFIG_NAME;
  }

  private void parseConfig(final Map<String, Object> rcaConfigSettings) {
    try {
      Map<String, Object> rcaMapObject = getRcaMapObject(rcaConfigSettings);
      if (rcaMapObject != null) {
        Object obj;
        obj = rcaMapObject.getOrDefault(RCA_CONF_KEY_CONSTANTS.PROMOTION_RATE_THRES, null);
        if (obj != null) {
          promotionRateThreshold = (Integer) obj;
        }
        obj = rcaMapObject.getOrDefault(RCA_CONF_KEY_CONSTANTS.YOUNG_GEN_GC_TIME_THRES, null);
        if (obj != null) {
          youngGenGcTimeThreshold = (Integer) obj;
        }
      }
    }
    catch (ClassCastException ne) {
      LOG.error("rca.conf contains value in invalid format, trace : {}", ne.getMessage());
    }
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
