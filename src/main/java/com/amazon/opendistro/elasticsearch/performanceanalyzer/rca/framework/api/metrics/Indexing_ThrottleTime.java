package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Indexing_ThrottleTime extends Metric {
  public Indexing_ThrottleTime(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardStatsValue.INDEXING_THROTTLE_TIME.name(), evaluationIntervalSeconds);
  }
}
