package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Flush_Time extends Metric {
  public Flush_Time(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.FLUSH_TIME.name(), evaluationIntervalSeconds);
  }
}
