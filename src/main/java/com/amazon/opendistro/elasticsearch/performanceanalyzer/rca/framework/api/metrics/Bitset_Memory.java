package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Bitset_Memory extends Metric {
  public Bitset_Memory(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.BITSET_MEMORY.name(), evaluationIntervalSeconds);
  }
}
