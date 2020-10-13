/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;

public class HeapSizeIncreasePolicyConfig {

  private static final String POLICY_NAME = "heap-size-increase-policy";
  public static final int DEFAULT_UNHEALTHY_NODE_PERCENTAGE = 50;
  public static final int DEFAULT_MIN_UNHEALTHY_MINUTES = 2 * 24 * 60;
  private static final int DEFAULT_DAY_BREACH_THRESHOLD = 8;
  private static final int DEFAULT_WEEK_BREACH_THRESHOLD = 3;
  private final int unhealthyNodePercentage;
  private final int dayBreachThreshold;
  private final int weekBreachThreshold;

  public HeapSizeIncreasePolicyConfig(final RcaConf rcaConf) {
    this.unhealthyNodePercentage = rcaConf.readRcaConfig(POLICY_NAME,
        HeapSizeIncreasePolicyKeys.UNHEALTHY_NODE_PERCENTAGE_KEY.toString(),
        DEFAULT_UNHEALTHY_NODE_PERCENTAGE, Integer.class);
    this.dayBreachThreshold = rcaConf.readRcaConfig(POLICY_NAME,
        HeapSizeIncreasePolicyKeys.DAY_BREACH_THRESHOLD_KEY.toString(), DEFAULT_DAY_BREACH_THRESHOLD,
        Integer.class);
    this.weekBreachThreshold = rcaConf
        .readRcaConfig(POLICY_NAME, HeapSizeIncreasePolicyKeys.WEEK_BREACH_THRESHOLD_KEY
            .toString(), DEFAULT_WEEK_BREACH_THRESHOLD, Integer.class);
  }

  enum HeapSizeIncreasePolicyKeys {
    UNHEALTHY_NODE_PERCENTAGE_KEY("unhealthy-node-percentage"),
    DAY_BREACH_THRESHOLD_KEY("day-breach-threshold"),
    WEEK_BREACH_THRESHOLD_KEY("week-breach-threshold");

    private final String value;

    HeapSizeIncreasePolicyKeys(final String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  public int getUnhealthyNodePercentage() {
    return unhealthyNodePercentage;
  }

  public int getDayBreachThreshold() {
    return dayBreachThreshold;
  }

  public int getWeekBreachThreshold() {
    return weekBreachThreshold;
  }
}
