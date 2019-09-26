package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Segments_Memory extends Metric {
  public Segments_Memory(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.SEGMENTS_MEMORY.name(), evaluationIntervalSeconds);
  }
}
