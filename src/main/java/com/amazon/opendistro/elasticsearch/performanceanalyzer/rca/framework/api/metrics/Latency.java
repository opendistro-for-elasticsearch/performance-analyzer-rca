package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Latency extends Metric {
  public Latency(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardBulkMetric.LATENCY.name(), evaluationIntervalSeconds);
  }
}
