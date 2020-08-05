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

import java.util.Arrays;
import java.util.List;

public class DeciderConfig {

    private static final String CACHE_BOUNDS_CONFIG_NAME = "cache-bounds";
    private static final String CACHE_CONFIG_NAME = "cache-type";
    private static final String WORKLOAD_CONFIG_NAME = "workload-type";
    private static final String PRIORITY_ORDER_CONFIG_NAME = "priority-order";
    private static final String FIELD_DATA_CACHE_UPPER_BOUND_NAME = "field-data-cache-upper-bound";
    private static final String SHARD_REQUEST_CACHE_UPPER_BOUND_NAME = "shard-request-cache-upper-bound";
    private static final double DEFAULT_FIELD_DATA_CACHE_UPPER_BOUND = 0.4;
    private static final double DEFAULT_SHARD_REQUEST_CACHE_UPPER_BOUND = 0.05;
    // Defaults based on prioritising Stability over performance.
    private static final List<String> DEFAULT_WORKLOAD_PRIORITY = Arrays.asList("ingest", "search");
    private static final List<String> DEFAULT_CACHE_PRIORITY = Arrays.asList("read-heavy", "write-heavy");

    private Double fieldDataCacheUpperBound;
    private Double shardRequestCacheUpperBound;
    private List<String> cachePriorityOrder;
    private List<String> workloadPriorityOrder;

    public DeciderConfig(final RcaConf rcaConf) {
        fieldDataCacheUpperBound = rcaConf.readDeciderConfig(CACHE_BOUNDS_CONFIG_NAME,
                FIELD_DATA_CACHE_UPPER_BOUND_NAME, Double.class);
        shardRequestCacheUpperBound = rcaConf.readDeciderConfig(CACHE_BOUNDS_CONFIG_NAME,
                SHARD_REQUEST_CACHE_UPPER_BOUND_NAME, Double.class);
        cachePriorityOrder = rcaConf.readDeciderConfig(CACHE_CONFIG_NAME,
                PRIORITY_ORDER_CONFIG_NAME, List.class);
        workloadPriorityOrder = rcaConf.readDeciderConfig(WORKLOAD_CONFIG_NAME,
                PRIORITY_ORDER_CONFIG_NAME, List.class);
        if (fieldDataCacheUpperBound == null) {
            fieldDataCacheUpperBound = DEFAULT_FIELD_DATA_CACHE_UPPER_BOUND;
        }
        if (shardRequestCacheUpperBound == null) {
            shardRequestCacheUpperBound = DEFAULT_SHARD_REQUEST_CACHE_UPPER_BOUND;
        }
        if (cachePriorityOrder == null) {
            cachePriorityOrder = DEFAULT_CACHE_PRIORITY;
        }
        if (workloadPriorityOrder == null) {
            workloadPriorityOrder = DEFAULT_WORKLOAD_PRIORITY;
        }
    }

    public Double getFieldDataCacheUpperBound() {
        return fieldDataCacheUpperBound;
    }

    public Double getShardRequestCacheUpperBound() {
        return shardRequestCacheUpperBound;
    }

    public List<String> getCachePriorityOrder() {
        return cachePriorityOrder;
    }

    public List<String> getWorkloadPriorityOrder() {
        return workloadPriorityOrder;
    }

    public static List<String> getDefaultWorkloadPriority() {
        return DEFAULT_WORKLOAD_PRIORITY;
    }

    public static List<String> getDefaultCachePriority() {
        return DEFAULT_CACHE_PRIORITY;
    }

    public static Double getDefaultShardRequestCacheUpperBound() {
        return DEFAULT_SHARD_REQUEST_CACHE_UPPER_BOUND;
    }

    public static Double getDefaultFieldDataCacheUpperBound() {
        return DEFAULT_FIELD_DATA_CACHE_UPPER_BOUND;
    }

    public static String getCacheBoundsConfigName() {
        return CACHE_BOUNDS_CONFIG_NAME;
    }

    public static String getCacheConfigName() {
        return CACHE_CONFIG_NAME;
    }

    public static String getWorkloadConfigName() {
        return WORKLOAD_CONFIG_NAME;
    }

    public static String getPriorityOrderConfigName() {
        return PRIORITY_ORDER_CONFIG_NAME;
    }

    public static String getFieldDataCacheUpperBoundName() {
        return FIELD_DATA_CACHE_UPPER_BOUND_NAME;
    }

    public static String getShardRequestCacheUpperBoundName() {
        return SHARD_REQUEST_CACHE_UPPER_BOUND_NAME;
    }
}
