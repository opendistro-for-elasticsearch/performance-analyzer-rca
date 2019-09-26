package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class IO_TotalSyscallRate extends Metric {
  public IO_TotalSyscallRate(long evaluationIntervalSeconds) {
    super(AllMetrics.OSMetrics.IO_TOTAL_SYSCALL_RATE.name(), evaluationIntervalSeconds);
  }
}
