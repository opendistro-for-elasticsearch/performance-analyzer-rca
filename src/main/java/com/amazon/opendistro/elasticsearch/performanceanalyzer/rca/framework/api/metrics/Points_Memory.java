package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Points_Memory extends Metric {
  public Points_Memory(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.POINTS_MEMORY.name(), evaluationIntervalSeconds);
  }
}
