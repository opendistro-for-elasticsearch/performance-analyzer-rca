package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class IO_TotThroughput extends Metric {
  public IO_TotThroughput(long evaluationIntervalSeconds) {
    super(AllMetrics.OSMetrics.IO_TOT_THROUGHPUT.name(), evaluationIntervalSeconds);
  }
}
