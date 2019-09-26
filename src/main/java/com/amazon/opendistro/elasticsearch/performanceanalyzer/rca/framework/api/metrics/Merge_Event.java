package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Merge_Event extends Metric {
  public Merge_Event(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.MERGE_EVENT.name(), evaluationIntervalSeconds);
  }
}
