package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class StoredFields_Memory extends Metric {
  public StoredFields_Memory(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.STORED_FIELDS_MEMORY.name(), evaluationIntervalSeconds);
  }
}
