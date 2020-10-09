/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

public class DiskUnderUtilizedRcaConfig {

  private static final String CONFIG_NAME = "disk-under-utilized-config";
  public static final double DEFAULT_SHARD_DISK_SPACE_UTILIZATION_PERCENT = 20D;
  private final double shardDiskSpaceUtilizationThreshold;

  public DiskUnderUtilizedRcaConfig(final RcaConf rcaConf) {
    this.shardDiskSpaceUtilizationThreshold = rcaConf.readRcaConfig(CONFIG_NAME,
        DiskUnderUtilizedConfigKeys.SHARD_DISK_SPACE_UTILIZATION_THRESHOLD.toString(),
        DEFAULT_SHARD_DISK_SPACE_UTILIZATION_PERCENT, Double.class);
  }

  public double getShardDiskSpaceUtilizationThreshold() {
    return shardDiskSpaceUtilizationThreshold;
  }

  enum DiskUnderUtilizedConfigKeys {
    SHARD_DISK_SPACE_UTILIZATION_THRESHOLD("shard-disk-space-utilization-threshold");

    private final String value;

    DiskUnderUtilizedConfigKeys(final String value) {
      this.value = value;
    }


    @Override
    public String toString() {
      return this.value;
    }
  }
}
