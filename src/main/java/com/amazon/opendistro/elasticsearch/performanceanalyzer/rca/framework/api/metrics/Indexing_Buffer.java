package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Indexing_Buffer extends Metric {
  public Indexing_Buffer(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.INDEXING_BUFFER.name(), evaluationIntervalSeconds);
  }
}
