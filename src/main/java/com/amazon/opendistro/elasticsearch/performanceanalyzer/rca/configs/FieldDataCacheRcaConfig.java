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
 * config object to store rca config settings for FieldDataCacheRca
 */
public class FieldDataCacheRcaConfig {
    public static final String CONFIG_NAME = "field-data-cache-rca-config";

    private Double fieldDataCacheSizeThreshold;
    private Integer fieldDataCollectorTimePeriodInSec;

    // Field data cache size threshold is 80%
    public static final double DEFAULT_FIELD_DATA_CACHE_SIZE_THRESHOLD = 0.8;
    // Metrics like eviction, hits are collected every 300 sec in field data cache rca
    public static final int DEFAULT_FIELD_DATA_COLLECTOR_TIME_PERIOD_IN_SEC = 300;

    public FieldDataCacheRcaConfig(final RcaConf rcaConf) {
        fieldDataCacheSizeThreshold =
                rcaConf.readRcaConfig(
                        CONFIG_NAME,
                        RCA_CONF_KEY_CONSTANTS.FIELD_DATA_CACHE_SIZE_THRESHOLD,
                        DEFAULT_FIELD_DATA_CACHE_SIZE_THRESHOLD,
                        (s) -> (s > 0),
                        Double.class);
        fieldDataCollectorTimePeriodInSec =
                rcaConf.readRcaConfig(
                        CONFIG_NAME,
                        RCA_CONF_KEY_CONSTANTS.FIELD_DATA_COLLECTOR_TIME_PERIOD_IN_SEC,
                        DEFAULT_FIELD_DATA_COLLECTOR_TIME_PERIOD_IN_SEC,
                        (s) -> (s > 0),
                        Integer.class);
        if (fieldDataCacheSizeThreshold == null) {
            fieldDataCacheSizeThreshold = DEFAULT_FIELD_DATA_CACHE_SIZE_THRESHOLD;
        }
        if (fieldDataCollectorTimePeriodInSec == null) {
            fieldDataCollectorTimePeriodInSec = DEFAULT_FIELD_DATA_COLLECTOR_TIME_PERIOD_IN_SEC;
        }
    }

    public double getFieldDataCacheSizeThreshold() {
        return fieldDataCacheSizeThreshold;
    }

    public int getFieldDataCollectorTimePeriodInSec() {
        return fieldDataCollectorTimePeriodInSec;
    }

    public static class RCA_CONF_KEY_CONSTANTS {
        public static final String FIELD_DATA_CACHE_SIZE_THRESHOLD = "field-data-cache-size-threshold";
        public static final String FIELD_DATA_COLLECTOR_TIME_PERIOD_IN_SEC = "field-data-collector-time-period-in-sec";
    }
}
