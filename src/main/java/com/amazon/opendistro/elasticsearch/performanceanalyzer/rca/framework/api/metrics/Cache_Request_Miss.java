package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Cache_Request_Miss extends Metric {
  public Cache_Request_Miss(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.CACHE_REQUEST_MISS.name(), evaluationIntervalSeconds);
  }
}
