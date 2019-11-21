package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class CB_ConfiguredSize extends Metric {
  public CB_ConfiguredSize(long evaluationIntervalSeconds) {
    super(AllMetrics.CircuitBreakerValue.CB_CONFIGURED_SIZE.name(), evaluationIntervalSeconds);
  }
}
