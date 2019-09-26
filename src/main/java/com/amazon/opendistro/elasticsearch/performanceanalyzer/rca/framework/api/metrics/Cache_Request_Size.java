package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Cache_Request_Size extends Metric {
  public Cache_Request_Size(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.CACHE_REQUEST_SIZE.name(), evaluationIntervalSeconds);
  }
}
