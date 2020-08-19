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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.decider;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.ConfigUtils;
import java.util.Map;

public class CacheBoundConfig {
  public static final double DEFAULT_FIELD_DATA_CACHE_UPPER_BOUND = 0.4;
  public static final double DEFAULT_FIELD_DATA_CACHE_LOWER_BOUND = 0.01;
  public static final double DEFAULT_SHARD_REQUEST_CACHE_UPPER_BOUND = 0.05;
  public static final double DEFAULT_SHARD_REQUEST_CACHE_LOWER_BOUND = 0.005;
  private static final String FIELD_DATA_CACHE_UPPER_BOUND_NAME = "field-data-cache-upper-bound";
  private static final String FIELD_DATA_CACHE_LOWER_BOUND_NAME = "field-data-cache-lower-bound";
  private static final String SHARD_REQUEST_CACHE_UPPER_BOUND_NAME = "shard-request-cache-upper-bound";
  private static final String SHARD_REQUEST_CACHE_LOWER_BOUND_NAME = "shard-request-cache-lower-bound";
  private Double fieldDataCacheUpperBound;
  private Double fieldDataCacheLowerBound;
  private Double shardRequestCacheUpperBound;
  private Double shardRequestCacheLowerBound;

  public CacheBoundConfig(Map<String, Object> configs) {
    fieldDataCacheLowerBound = ConfigUtils
        .readConfig(configs, FIELD_DATA_CACHE_LOWER_BOUND_NAME, Double.class);
    if (fieldDataCacheLowerBound == null) {
      fieldDataCacheLowerBound = DEFAULT_FIELD_DATA_CACHE_LOWER_BOUND;
    }
    fieldDataCacheUpperBound = ConfigUtils
        .readConfig(configs, FIELD_DATA_CACHE_UPPER_BOUND_NAME, Double.class);
    if (fieldDataCacheUpperBound == null) {
      fieldDataCacheUpperBound = DEFAULT_FIELD_DATA_CACHE_UPPER_BOUND;
    }
    shardRequestCacheLowerBound = ConfigUtils
        .readConfig(configs, SHARD_REQUEST_CACHE_LOWER_BOUND_NAME, Double.class);
    if (shardRequestCacheLowerBound == null) {
      shardRequestCacheLowerBound = DEFAULT_SHARD_REQUEST_CACHE_LOWER_BOUND;
    }
    shardRequestCacheUpperBound = ConfigUtils
        .readConfig(configs, SHARD_REQUEST_CACHE_UPPER_BOUND_NAME, Double.class);
    if (shardRequestCacheUpperBound == null) {
      shardRequestCacheUpperBound = DEFAULT_SHARD_REQUEST_CACHE_UPPER_BOUND;
    }
  }

  public double fieldDataCacheUpperBound() {
    return fieldDataCacheUpperBound;
  }

  public double fieldDataCacheLowerBound() {
    return fieldDataCacheLowerBound;
  }

  public double shardRequestCacheUpperBound() {
    return shardRequestCacheUpperBound;
  }

  public double shardRequestCacheLowerBound() {
    return shardRequestCacheLowerBound;
  }
}
