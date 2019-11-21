package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class ShardBulkDocs extends Metric {
  public ShardBulkDocs(long evaluationIntervalSeconds) {
    super(AllMetrics.ShardBulkMetric.DOC_COUNT.name(), evaluationIntervalSeconds);
  }
}
