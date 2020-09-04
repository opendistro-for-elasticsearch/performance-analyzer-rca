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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.decider.jvm;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Config;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NestedConfig;

/**
 * "level-three-config": {
 *    "write-queue-step-size": 2,
 *    "search-queue-step-size":2
 * }
 */
public class LevelThreeActionBuilderConfig {
  public static final int DEFAULT_WRITE_QUEUE_STEP_SIZE = 2;
  public static final int DEFAULT_SEARCH_QUEUE_STEP_SIZE = 2;
  private static final String WRITE_QUEUE_STEP_SIZE_CONFIG_NAME = "write-queue-step-size";
  private static final String SEARCH_QUEUE_STEP_SIZE_CONFIG_NAME = "search-queue-step-size";
  private Config<Integer> writeQueueStepSize;
  private Config<Integer> searchQueueStepSize;

  public LevelThreeActionBuilderConfig(NestedConfig configs) {
    writeQueueStepSize = new Config<>(WRITE_QUEUE_STEP_SIZE_CONFIG_NAME, configs.getValue(),
        DEFAULT_WRITE_QUEUE_STEP_SIZE, (s) -> (s >= 0), Integer.class);
    searchQueueStepSize = new Config<>(SEARCH_QUEUE_STEP_SIZE_CONFIG_NAME, configs.getValue(),
        DEFAULT_SEARCH_QUEUE_STEP_SIZE, (s) -> (s >= 0), Integer.class);
  }

  public int writeQueueStepSize() {
    return writeQueueStepSize.getValue();
  }

  public int searchQueueStepSize() {
    return searchQueueStepSize.getValue();
  }
}
