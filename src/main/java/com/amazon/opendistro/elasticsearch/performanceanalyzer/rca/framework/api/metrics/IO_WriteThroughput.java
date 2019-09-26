package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class IO_WriteThroughput extends Metric {
  public IO_WriteThroughput(long evaluationIntervalSeconds) {
    super(AllMetrics.OSMetrics.IO_WRITE_THROUGHPUT.name(), evaluationIntervalSeconds);
  }
}
