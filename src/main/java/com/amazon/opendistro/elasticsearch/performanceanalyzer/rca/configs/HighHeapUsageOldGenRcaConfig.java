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

/**
 * config object to store rca config settings in rca.conf
 */
public class HighHeapUsageOldGenRcaConfig {

  private static final Logger LOG = LogManager.getLogger(HighHeapUsageOldGenRcaConfig.class);
  private Integer topK;
  public static final int DEFAULT_TOP_K = 3;
  public static final String CONFIG_NAME = "high-heap-usage-old-gen-rca";

  public HighHeapUsageOldGenRcaConfig(final RcaConf rcaConf) {
    topK = rcaConf.readRcaConfig(CONFIG_NAME, RCA_CONF_KEY_CONSTANTS.TOP_K, DEFAULT_TOP_K, (s) -> (s > 0), Integer.class);
    if (topK == null) {
      topK = DEFAULT_TOP_K;
    }
  }

  public int getTopK() {
    return topK;
  }

  public static class RCA_CONF_KEY_CONSTANTS {
    public static final String TOP_K = "top-k";
  }
}
