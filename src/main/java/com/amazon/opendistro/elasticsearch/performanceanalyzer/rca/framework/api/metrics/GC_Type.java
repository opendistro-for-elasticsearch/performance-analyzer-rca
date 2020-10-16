package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCInfoValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class GC_Type extends Metric {

  public static final String NAME = GCInfoValue.GARBAGE_COLLECTOR_TYPE.toString();

  public GC_Type(long evaluationIntervalSeconds) {
    super(NAME, evaluationIntervalSeconds);
  }
}
