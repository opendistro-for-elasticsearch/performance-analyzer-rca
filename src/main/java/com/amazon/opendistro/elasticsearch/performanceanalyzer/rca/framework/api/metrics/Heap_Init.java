package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Heap_Init extends Metric {
  public Heap_Init(long evaluationIntervalSeconds) {
    super(AllMetrics.HeapValue.HEAP_INIT.name(), evaluationIntervalSeconds);
  }
}
