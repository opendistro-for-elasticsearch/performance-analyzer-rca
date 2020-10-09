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

public class CpuUnderUtilizedRcaConfig {

  private static final String CONFIG_NAME = "cpu-underutilized-rca-config";
  private static final double DEFAULT_MIN_CPU_UTILIZATION = 20D;
  private final double cpuUtilizationThreshold;

  public CpuUnderUtilizedRcaConfig(final RcaConf rcaConf) {
    this.cpuUtilizationThreshold = rcaConf.readRcaConfig(CONFIG_NAME,
        CpuUnderUtilizedRcaConfigKeys.MIN_CPU_UTILIZATION_THRESHOLD.toString(),
        DEFAULT_MIN_CPU_UTILIZATION, Double.class);
  }

  public double getCpuUtilizationThreshold() {
    return this.cpuUtilizationThreshold;
  }

  enum CpuUnderUtilizedRcaConfigKeys {
    MIN_CPU_UTILIZATION_THRESHOLD("min-cpu-utilization-threshold");

    private String value;

    CpuUnderUtilizedRcaConfigKeys(final String value) {
      this.value = value;
    }


    @Override
    public String toString() {
      return this.value;
    }
  }
}
