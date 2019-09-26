package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Disk_Utilization extends Metric {
  public Disk_Utilization(long evaluationIntervalSeconds) {
    super(AllMetrics.DiskValue.DISK_UTILIZATION.name(), evaluationIntervalSeconds);
  }
}
