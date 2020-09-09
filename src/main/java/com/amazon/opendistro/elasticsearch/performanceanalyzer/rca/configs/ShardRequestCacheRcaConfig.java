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

/**
 * config object to store rca config settings for ShardRequestCacheRca
 */
public class ShardRequestCacheRcaConfig {
    public static final String CONFIG_NAME = "shard-request-cache-rca-config";

    private Double shardRequestCacheSizeThreshold;
    private Integer shardRequestCollectorTimePeriodInSec;

    // Shard request cache size threshold is 90%
    public static final double DEFAULT_SHARD_REQUEST_CACHE_SIZE_THRESHOLD = 0.9;
    // Metrics like eviction, hits are collected every 300 sec in shard request cache rca
    public static final int DEFAULT_SHARD_REQUEST_COLLECTOR_TIME_PERIOD_IN_SEC = 300;

    public ShardRequestCacheRcaConfig(final RcaConf rcaConf) {
        shardRequestCacheSizeThreshold =
                rcaConf.readRcaConfig(
                        CONFIG_NAME,
                        RCA_CONF_KEY_CONSTANTS.SHARD_REQUEST_CACHE_SIZE_THRESHOLD,
                        DEFAULT_SHARD_REQUEST_CACHE_SIZE_THRESHOLD,
                        (s) -> (s > 0),
                        Double.class);
        shardRequestCollectorTimePeriodInSec =
                rcaConf.readRcaConfig(
                        CONFIG_NAME,
                        RCA_CONF_KEY_CONSTANTS.SHARD_REQUEST_COLLECTOR_TIME_PERIOD_IN_SEC,
                        DEFAULT_SHARD_REQUEST_COLLECTOR_TIME_PERIOD_IN_SEC,
                        (s) -> (s > 0),
                        Integer.class);
    }

    public double getShardRequestCacheSizeThreshold() {
        return shardRequestCacheSizeThreshold;
    }

    public int getShardRequestCollectorTimePeriodInSec() {
        return shardRequestCollectorTimePeriodInSec;
    }

    public static class RCA_CONF_KEY_CONSTANTS {
        public static final String SHARD_REQUEST_CACHE_SIZE_THRESHOLD = "shard-request-cache-threshold";
        public static final String SHARD_REQUEST_COLLECTOR_TIME_PERIOD_IN_SEC = "shard-request-collector-time-period-in-sec";
    }
}
