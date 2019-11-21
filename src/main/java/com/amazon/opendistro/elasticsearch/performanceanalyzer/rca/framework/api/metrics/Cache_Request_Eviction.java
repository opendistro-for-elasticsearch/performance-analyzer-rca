package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Cache_Request_Eviction extends Metric {
    public Cache_Request_Eviction(long evaluationIntervalSeconds) {
        super(AllMetrics.ShardStatsValue.CACHE_REQUEST_EVICTION.name(), evaluationIntervalSeconds);
    }
}

