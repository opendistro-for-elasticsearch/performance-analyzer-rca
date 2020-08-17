/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

public class QueueRejectionRcaConfig {
  private Integer rejectionTimePeriodInSeconds;
  public static final int DEFAULT_REJECTION_TIME_PERIOD_IN_SECONDS = 300;
  public static final String CONFIG_NAME = "queue-rejection-rca";

  public QueueRejectionRcaConfig(final RcaConf rcaConf) {
    rejectionTimePeriodInSeconds = rcaConf.readRcaConfig(
        CONFIG_NAME, RCA_CONF_KEY_CONSTANTS.REJECTION_TIME_PERIOD_IN_SECONDS, Integer.class);
    if (rejectionTimePeriodInSeconds == null) {
      rejectionTimePeriodInSeconds = DEFAULT_REJECTION_TIME_PERIOD_IN_SECONDS;
    }
  }

  public int getRejectionTimePeriodInSeconds() {
    return rejectionTimePeriodInSeconds;
  }

  public static class RCA_CONF_KEY_CONSTANTS {
    public static final String REJECTION_TIME_PERIOD_IN_SECONDS = "rejection-time-period-in-seconds";
  }
}
