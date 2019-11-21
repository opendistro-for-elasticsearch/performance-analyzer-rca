package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Segments_Total extends Metric {
  public Segments_Total(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.SEGMENTS_TOTAL.name(), evaluationIntervalSeconds);
  }
}
