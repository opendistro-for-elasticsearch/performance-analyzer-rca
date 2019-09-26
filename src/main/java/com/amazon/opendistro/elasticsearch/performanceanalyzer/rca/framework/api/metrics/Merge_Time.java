package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Merge_Time extends Metric {
  public Merge_Time(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.MERGE_TIME.name(), evaluationIntervalSeconds);
  }
}
