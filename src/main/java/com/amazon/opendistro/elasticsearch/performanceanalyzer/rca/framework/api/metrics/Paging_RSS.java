package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Paging_RSS extends Metric {
  public Paging_RSS(long evaluationIntervalSeconds) {
    super(AllMetrics.OSMetrics.PAGING_RSS.name(), evaluationIntervalSeconds);
  }
}
