package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Cache_FieldData_Eviction extends Metric {
  public Cache_FieldData_Eviction(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.CACHE_FIELDDATA_EVICTION.name(), evaluationIntervalSeconds);
  }
}
