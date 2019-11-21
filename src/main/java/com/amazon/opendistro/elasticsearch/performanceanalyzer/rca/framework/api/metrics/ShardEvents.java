package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class ShardEvents extends Metric {
  public ShardEvents(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardOperationMetric.SHARD_OP_COUNT.name(), evaluationIntervalSeconds);
  }
}
