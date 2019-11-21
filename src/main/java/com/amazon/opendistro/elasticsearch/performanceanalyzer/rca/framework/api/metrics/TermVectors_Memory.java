package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class TermVectors_Memory extends Metric {
  public TermVectors_Memory(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.TERM_VECTOR_MEMORY.name(), evaluationIntervalSeconds);
  }
}
