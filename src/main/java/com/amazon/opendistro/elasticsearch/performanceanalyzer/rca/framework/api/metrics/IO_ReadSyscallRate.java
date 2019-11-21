package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class IO_ReadSyscallRate extends Metric {
  public IO_ReadSyscallRate(long evaluationIntervalSeconds) {
    super(AllMetrics.OSMetrics.IO_READ_SYSCALL_RATE.name(), evaluationIntervalSeconds);
  }
}
