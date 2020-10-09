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

  private static final String POLICY_NAME = "HeapSizeIncreasePolicy";
  public static final int DEFAULT_UNHEALTHY_NODE_PERCENTAGE = 50;
  public static final int DEFAULT_MIN_UNHEALTHY_MINUTES = 2 * 24 * 60;
  private final int unhealthyNodePercentage;

  private final int minUnhealthyMinutes;

  public HeapSizeIncreasePolicyConfig(final RcaConf rcaConf) {
    this.unhealthyNodePercentage = rcaConf.readRcaConfig(POLICY_NAME,
        HeapSizeIncreasePolicyKeys.UNHEALTHY_NODE_PERCENTAGE_KEY.toString(),
        DEFAULT_UNHEALTHY_NODE_PERCENTAGE, Integer.class);
    this.minUnhealthyMinutes = rcaConf.readRcaConfig(POLICY_NAME,
        HeapSizeIncreasePolicyKeys.MIN_UNHEALTHY_MINUTES_KEY.toString(), DEFAULT_MIN_UNHEALTHY_MINUTES,
        Integer.class);
  }

  enum HeapSizeIncreasePolicyKeys {
    UNHEALTHY_NODE_PERCENTAGE_KEY("unhealthy-node-percentage"),
    MIN_UNHEALTHY_MINUTES_KEY("min-unhealthy-minutes");

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

  public int getMinUnhealthyMinutes() {
    return minUnhealthyMinutes;
  }
}
