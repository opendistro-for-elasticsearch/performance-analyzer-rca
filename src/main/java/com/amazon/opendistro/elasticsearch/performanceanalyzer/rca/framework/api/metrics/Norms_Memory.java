package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Norms_Memory extends Metric {
  public Norms_Memory(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.NORMS_MEMORY.name(), evaluationIntervalSeconds);
  }
}
