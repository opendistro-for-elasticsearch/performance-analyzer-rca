package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Net_Throughput extends Metric {
  public Net_Throughput(long evaluationIntervalSeconds) {
    super(AllMetrics.IPValue.NET_THROUGHPUT.name(), evaluationIntervalSeconds);
  }
}
