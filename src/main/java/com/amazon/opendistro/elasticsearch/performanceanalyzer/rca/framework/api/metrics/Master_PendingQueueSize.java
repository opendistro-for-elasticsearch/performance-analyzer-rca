package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Master_PendingQueueSize extends Metric {
  public Master_PendingQueueSize(long evaluationIntervalSeconds) {
    super(
        AllMetrics.MasterPendingValue.MASTER_PENDING_QUEUE_SIZE.name(), evaluationIntervalSeconds);
  }
}
