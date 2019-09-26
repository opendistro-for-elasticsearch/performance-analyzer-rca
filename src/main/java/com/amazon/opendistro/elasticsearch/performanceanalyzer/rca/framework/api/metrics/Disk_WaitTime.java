package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Disk_WaitTime extends Metric {
  public Disk_WaitTime(long evaluationIntervalSeconds) {
    super(AllMetrics.DiskValue.DISK_WAITTIME.name(), evaluationIntervalSeconds);
  }
}
