package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class GC_Collection_Event extends Metric {
  public GC_Collection_Event(long evaluationIntervalSeconds) {
    super(AllMetrics.HeapValue.GC_COLLECTION_EVENT.name(), evaluationIntervalSeconds);
  }
}
