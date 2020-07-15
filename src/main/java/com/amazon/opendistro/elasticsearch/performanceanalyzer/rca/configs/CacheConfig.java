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

public class CacheConfig {
    public static final String CONFIG_NAME = "cache-config";

    private Double fieldDataCacheSizeThreshold;
    private Double shardRequestCacheSizeThreshold;

    public static final double DEFAULT_FIELD_DATA_CACHE_SIZE_THRESHOLD = 0.8;
    public static final double DEFAULT_SHARD_REQUEST_CACHE_SIZE_THRESHOLD = 0.9;

    public CacheConfig(final RcaConf rcaConf) {
        fieldDataCacheSizeThreshold = rcaConf.readRcaConfig(CONFIG_NAME,
                RCA_CONF_KEY_CONSTANTS.FIELD_DATA_CACHE_SIZE_THRESHOLD, Double.class);
        shardRequestCacheSizeThreshold = rcaConf.readRcaConfig(CONFIG_NAME,
                RCA_CONF_KEY_CONSTANTS.SHARD_REQUEST_CACHE_SIZE_THRESHOLD, Double.class);
        if (fieldDataCacheSizeThreshold == null) {
            fieldDataCacheSizeThreshold = DEFAULT_FIELD_DATA_CACHE_SIZE_THRESHOLD;
        }
        if (shardRequestCacheSizeThreshold == null) {
            shardRequestCacheSizeThreshold = DEFAULT_SHARD_REQUEST_CACHE_SIZE_THRESHOLD;
        }
    }

    public double getFieldDataCacheSizeThreshold() {
        return fieldDataCacheSizeThreshold;
    }

    public double getShardRequestCacheSizeThreshold() {
        return shardRequestCacheSizeThreshold;
    }

    public static class RCA_CONF_KEY_CONSTANTS {
        public static final String FIELD_DATA_CACHE_SIZE_THRESHOLD = "field-data-cache-size-threshold";
        public static final String SHARD_REQUEST_CACHE_SIZE_THRESHOLD = "shard-request-cache-threshold";
    }
}
