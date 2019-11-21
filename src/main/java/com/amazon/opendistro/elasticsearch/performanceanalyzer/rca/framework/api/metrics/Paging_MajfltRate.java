package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Paging_MajfltRate extends Metric {
  public Paging_MajfltRate(long evaluationIntervalSeconds) {
    super(AllMetrics.OSMetrics.PAGING_MAJ_FLT_RATE.name(), evaluationIntervalSeconds);
  }
}
