package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class CB_TrippedEvents extends Metric {
  public CB_TrippedEvents(long evaluationIntervalSeconds) {
    super(AllMetrics.CircuitBreakerValue.CB_TRIPPED_EVENTS.name(), evaluationIntervalSeconds);
  }
}
