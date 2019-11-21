package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Sched_Runtime extends Metric {
  public Sched_Runtime(long evaluationIntervalSeconds) {
    super(AllMetrics.OSMetrics.SCHED_RUNTIME.name(), evaluationIntervalSeconds);
  }
}
