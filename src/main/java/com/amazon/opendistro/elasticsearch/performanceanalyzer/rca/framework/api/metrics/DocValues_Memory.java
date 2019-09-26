package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class DocValues_Memory extends Metric {
  public DocValues_Memory(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.DOC_VALUES_MEMORY.name(), evaluationIntervalSeconds);
  }
}
