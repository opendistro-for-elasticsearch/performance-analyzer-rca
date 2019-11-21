package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Merge_CurrentEvent extends Metric {
  public Merge_CurrentEvent(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.MERGE_CURRENT_EVENT.name(), evaluationIntervalSeconds);
  }
}
