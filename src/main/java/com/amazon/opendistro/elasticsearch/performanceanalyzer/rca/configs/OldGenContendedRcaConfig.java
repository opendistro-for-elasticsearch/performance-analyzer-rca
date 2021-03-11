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

public class OldGenContendedRcaConfig {
  private static final String CONFIG_NAME = "old-gen-contended-rca-config";
  public static final int DEFAULT_MIN_TOTAL_MEMORY_IN_GB = 200; // default: Need at least 200GB mem

  private final int minTotalMemoryThresholdInGB;

  public OldGenContendedRcaConfig(final RcaConf conf) {
    this.minTotalMemoryThresholdInGB = conf.readRcaConfig(CONFIG_NAME,
        OldGenContendedRcaKeys.MIN_TOTAL_MEMORY_THRESHOLD_IN_GB.toString(),
        DEFAULT_MIN_TOTAL_MEMORY_IN_GB, Integer.class);
  }

  enum OldGenContendedRcaKeys {
    MIN_TOTAL_MEMORY_THRESHOLD_IN_GB("min-total-memory-threshold-in-gb");

    private final String value;

    OldGenContendedRcaKeys(final String value) {
      this.value = value;
    }


    @Override
    public String toString() {
      return value;
    }
  }

  public int getMinTotalMemoryThresholdInGb() {
    return minTotalMemoryThresholdInGB;
  }
}
