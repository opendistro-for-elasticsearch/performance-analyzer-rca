package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class IO_ReadThroughput extends Metric {
  public IO_ReadThroughput(long evaluationIntervalSeconds) {
    super(AllMetrics.OSMetrics.IO_READ_THROUGHPUT.name(), evaluationIntervalSeconds);
  }
}
