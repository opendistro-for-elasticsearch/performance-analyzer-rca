package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class VersionMap_Memory extends Metric {
  public VersionMap_Memory(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.VERSION_MAP_MEMORY.name(), evaluationIntervalSeconds);
  }
}
