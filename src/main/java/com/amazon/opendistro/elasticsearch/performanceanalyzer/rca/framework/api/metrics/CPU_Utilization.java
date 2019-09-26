package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class CPU_Utilization extends Metric {
  public static final String NAME = AllMetrics.OSMetrics.CPU_UTILIZATION.name();

  public CPU_Utilization(long evaluationIntervalSeconds) {
    super(AllMetrics.OSMetrics.CPU_UTILIZATION.name(), evaluationIntervalSeconds);
  }
}
