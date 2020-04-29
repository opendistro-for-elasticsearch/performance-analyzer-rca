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
 * config object to store rca config settings for HotNodeClusterRca
 */
public class HotNodeClusterRcaConfig {
  private static final Logger LOG = LogManager.getLogger(HotNodeClusterRcaConfig.class);
  public static final String CONFIG_NAME = "hot-node-cluster-rca";
  private double unbalancedResourceThreshold;
  private double resourceUsageLowerBoundThreshold;
  public static final double DEFAULT_UNBALANCED_RESOURCE_THRES = 0.3;
  public static final double DEFAULT_RESOURCE_USAGE_LOWER_BOUND_THRES = 0.1;

  public HotNodeClusterRcaConfig(final Map<String, Object> settings) {
    this.unbalancedResourceThreshold = DEFAULT_UNBALANCED_RESOURCE_THRES;
    this.resourceUsageLowerBoundThreshold = DEFAULT_RESOURCE_USAGE_LOWER_BOUND_THRES;
    parseConfig(settings);
  }

  private void parseConfig(final Map<String, Object> settings) {
    if (settings == null) {
      return;
    }
    try {
      Object obj;
      obj = settings.getOrDefault(RCA_CONF_KEY_CONSTANTS.UNBALANCED_RESOURCE_THRES, null);
      if (obj != null) {
        unbalancedResourceThreshold = (Double) obj;
      }
      obj = settings.getOrDefault(RCA_CONF_KEY_CONSTANTS.RESOURCE_USAGE_LOWER_BOUND_THRES, null);
      if (obj != null) {
        resourceUsageLowerBoundThreshold = (Double) obj;
      }
    }
    catch (ClassCastException ne) {
      LOG.error("rca.conf contains value in invalid format, trace : {}", ne.getMessage());
    }
  }

  public double getUnbalancedResourceThreshold() {
    return unbalancedResourceThreshold;
  }

  public double getResourceUsageLowerBoundThreshold() {
    return resourceUsageLowerBoundThreshold;
  }

  private static class RCA_CONF_KEY_CONSTANTS {
    private static final String UNBALANCED_RESOURCE_THRES = "unbalanced-resource-threshold";
    private static final String RESOURCE_USAGE_LOWER_BOUND_THRES = "resource-usage-lower-bound-threshold";
  }
}
