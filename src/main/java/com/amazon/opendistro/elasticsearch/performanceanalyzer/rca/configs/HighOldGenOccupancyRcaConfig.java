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

public class HighOldGenOccupancyRcaConfig {

  private static final String RCA_NAME = "HighOldGenOccupancyRca";
  public static final long DEFAULT_UTILIZATION = 75;
  public static final long DEFAULT_EVALUATION_INTERVAL_IN_S = 60;
  private final Long heapUtilizationThreshold;

  private final long evaluationIntervalInS;

  public HighOldGenOccupancyRcaConfig(final RcaConf conf) {
    this.evaluationIntervalInS = conf.readRcaConfig(RCA_NAME,
        HighOldGenOccupancyRcaConfigKeys.EVALUATION_INTERVAL_IN_S.toString(),
        DEFAULT_EVALUATION_INTERVAL_IN_S, Long.class);

    this.heapUtilizationThreshold = conf
        .readRcaConfig(RCA_NAME, HighOldGenOccupancyRcaConfigKeys.HEAP_UTILIZATION_THRESHOLD
            .toString(), DEFAULT_UTILIZATION, Long.class);
  }

  enum HighOldGenOccupancyRcaConfigKeys {
    HEAP_UTILIZATION_THRESHOLD("heap-utilization-threshold"),
    EVALUATION_INTERVAL_IN_S("eval-interval-in-s");

    private final String value;

    HighOldGenOccupancyRcaConfigKeys(final String value) {
      this.value = value;
    }


    @Override
    public String toString() {
      return this.value;
    }

  }

  public Long getHeapUtilizationThreshold() {
    return heapUtilizationThreshold;
  }

  public long getEvaluationIntervalInS() {
    return evaluationIntervalInS;
  }
}