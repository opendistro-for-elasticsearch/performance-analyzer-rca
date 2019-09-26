package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Refresh_Event extends Metric {
  public Refresh_Event(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.REFRESH_EVENT.name(), evaluationIntervalSeconds);
  }
}
