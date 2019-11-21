package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class IndexWriter_Memory extends Metric {
  public IndexWriter_Memory(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.INDEX_WRITER_MEMORY.name(), evaluationIntervalSeconds);
  }
}
