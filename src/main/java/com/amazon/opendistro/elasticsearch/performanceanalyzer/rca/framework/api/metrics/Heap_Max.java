package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Heap_Max extends Metric {
  public Heap_Max(long evaluationIntervalSeconds) {
    super(AllMetrics.HeapValue.HEAP_MAX.name(), evaluationIntervalSeconds);
  }
}
