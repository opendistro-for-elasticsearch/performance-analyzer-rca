package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class IO_WriteSyscallRate extends Metric {
  public IO_WriteSyscallRate(long evaluationIntervalSeconds) {
    super(AllMetrics.OSMetrics.IO_WRITE_SYSCALL_RATE.name(), evaluationIntervalSeconds);
  }
}
