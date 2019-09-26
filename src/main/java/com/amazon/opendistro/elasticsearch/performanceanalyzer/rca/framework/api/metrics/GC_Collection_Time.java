package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class GC_Collection_Time extends Metric {
  public GC_Collection_Time(long evaluationIntervalSeconds) {
    super(AllMetrics.HeapValue.GC_COLLECTION_TIME.name(), evaluationIntervalSeconds);
  }
}
