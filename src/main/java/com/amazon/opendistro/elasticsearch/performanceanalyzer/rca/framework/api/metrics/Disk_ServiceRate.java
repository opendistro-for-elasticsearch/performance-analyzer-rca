package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Disk_ServiceRate extends Metric {
  public Disk_ServiceRate(long evaluationIntervalSeconds) {
    super(AllMetrics.DiskValue.DISK_SERVICE_RATE.name(), evaluationIntervalSeconds);
  }
}
