package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Heap_AllocRate extends Metric {
  public Heap_AllocRate(long evaluationIntervalSeconds) {
    super(AllMetrics.OSMetrics.HEAP_ALLOC_RATE.name(), evaluationIntervalSeconds);
  }
}
