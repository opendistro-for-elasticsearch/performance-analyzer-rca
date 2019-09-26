package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Heap_Used extends Metric {
  public Heap_Used(long evaluationIntervalSeconds) {
    super(AllMetrics.HeapValue.HEAP_USED.name(), evaluationIntervalSeconds);
  }
}
