package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Refresh_Time extends Metric {
  public Refresh_Time(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.REFRESH_TIME.name(), evaluationIntervalSeconds);
  }
}
