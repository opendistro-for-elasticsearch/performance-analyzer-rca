package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class CB_EstimatedSize extends Metric {
  public CB_EstimatedSize(long evaluationIntervalSeconds) {
    super(AllMetrics.CircuitBreakerValue.CB_ESTIMATED_SIZE.name(), evaluationIntervalSeconds);
  }
}
