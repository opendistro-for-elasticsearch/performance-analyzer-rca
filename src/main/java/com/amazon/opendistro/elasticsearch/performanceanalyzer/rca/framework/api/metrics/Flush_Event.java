package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Flush_Event extends Metric {
  public Flush_Event(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.FLUSH_EVENT.name(), evaluationIntervalSeconds);
  }
}
