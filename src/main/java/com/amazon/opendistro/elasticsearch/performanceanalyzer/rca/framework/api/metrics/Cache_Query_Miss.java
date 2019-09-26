package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Cache_Query_Miss extends Metric {
  public Cache_Query_Miss(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.CACHE_QUERY_MISS.name(), evaluationIntervalSeconds);
  }
}
