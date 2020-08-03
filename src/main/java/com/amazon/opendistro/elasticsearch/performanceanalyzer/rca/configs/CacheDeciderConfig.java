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

public class CacheDeciderConfig {
    public static final String CONFIG_NAME = "cache-decider-config";

    private Double fieldDataCacheUpperBound;
    private Double shardRequestCacheUpperBound;

    public static final double DEFAULT_FIELD_DATA_CACHE_UPPER_BOUND = 0.4;
    public static final double DEFAULT_SHARD_REQUEST_CACHE_UPPER_BOUND = 0.05;

    public CacheDeciderConfig(final RcaConf rcaConf) {
        fieldDataCacheUpperBound = rcaConf.readDeciderConfig(CONFIG_NAME,
                RCA_CONF_KEY_CONSTANTS.FIELD_DATA_CACHE_UPPER_BOUND, Double.class);
        shardRequestCacheUpperBound = rcaConf.readDeciderConfig(CONFIG_NAME,
                RCA_CONF_KEY_CONSTANTS.SHARD_REQUEST_CACHE_UPPER_BOUND, Double.class);
        if (fieldDataCacheUpperBound == null) {
            fieldDataCacheUpperBound = DEFAULT_FIELD_DATA_CACHE_UPPER_BOUND;
        }
        if (shardRequestCacheUpperBound == null) {
            shardRequestCacheUpperBound = DEFAULT_SHARD_REQUEST_CACHE_UPPER_BOUND;
        }
    }

    public double getFieldDataCacheUpperBound() {
        return fieldDataCacheUpperBound;
    }

    public double getShardRequestCacheUpperBound() {
        return shardRequestCacheUpperBound;
    }

    public static class RCA_CONF_KEY_CONSTANTS {
        public static final String FIELD_DATA_CACHE_UPPER_BOUND = "field-data-cache-upper-bound";
        public static final String SHARD_REQUEST_CACHE_UPPER_BOUND = "shard-request-cache-upper-bound";
    }
}
