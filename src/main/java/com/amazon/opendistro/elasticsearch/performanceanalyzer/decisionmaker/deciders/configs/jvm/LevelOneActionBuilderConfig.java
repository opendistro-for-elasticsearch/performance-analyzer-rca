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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.configs.jvm;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Config;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NestedConfig;

/**
 * "level-one-config": {
 *    "fielddata-cache-step-size": 1,
 *    "shard-request-cache-step-size": 1
 * }
 */
public class LevelOneActionBuilderConfig {
  public static final int DEFAULT_FIELD_DATA_CACHE_STEP_SIZE = 1;
  public static final int DEFAULT_SHARD_REQUEST_CACHE_STEP_SIZE = 1;
  private static final String FIELD_DATA_CACHE_STEP_SIZE_CONFIG_NAME = "fielddata-cache-step-size";
  private static final String SHARD_REQUEST_CACHE_STEP_SIZE_CONFIG_NAME = "shard-request-cache-step-size";
  private Config<Integer> fieldDataCacheStepSize;
  private Config<Integer> shardRequestCacheStepSize;

  public LevelOneActionBuilderConfig(NestedConfig configs) {
    fieldDataCacheStepSize = new Config<>(FIELD_DATA_CACHE_STEP_SIZE_CONFIG_NAME, configs.getValue(),
        DEFAULT_FIELD_DATA_CACHE_STEP_SIZE, (s) -> (s >= 0), Integer.class);
    shardRequestCacheStepSize = new Config<>(SHARD_REQUEST_CACHE_STEP_SIZE_CONFIG_NAME, configs.getValue(),
        DEFAULT_SHARD_REQUEST_CACHE_STEP_SIZE, (s) -> (s >= 0), Integer.class);
  }

  public int fieldDataCacheStepSize() {
    return fieldDataCacheStepSize.getValue();
  }

  public int shardRequestCacheStepSize() {
    return shardRequestCacheStepSize.getValue();
  }
}
