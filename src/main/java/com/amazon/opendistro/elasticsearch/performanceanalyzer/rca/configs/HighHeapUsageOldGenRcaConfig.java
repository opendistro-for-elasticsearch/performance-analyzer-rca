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

/**
 * config object to store rca config settings in rca.conf
 */
public class HighHeapUsageOldGenRcaConfig {

  private static final Logger LOG = LogManager.getLogger(HighHeapUsageOldGenRcaConfig.class);
  private int topK;
  public static final int DEFAULT_TOP_K = 3;
  private static final String TOP_K_RCA_CONF = "top-k";

  public HighHeapUsageOldGenRcaConfig(final Map<String, String> settings) {
    this.topK = DEFAULT_TOP_K;
    parseConfig(settings);
  }

  private void parseConfig(final Map<String, String> settings) {
    if (settings != null && settings.containsKey(TOP_K_RCA_CONF)) {
      try {
        topK = Integer.parseInt(settings.get(TOP_K_RCA_CONF));
      }
      catch (NumberFormatException ne) {
        LOG.error("rca.conf contains invalid top-k number");
      }
    }
  }

  public int getTopK() {
    return topK;
  }
}
