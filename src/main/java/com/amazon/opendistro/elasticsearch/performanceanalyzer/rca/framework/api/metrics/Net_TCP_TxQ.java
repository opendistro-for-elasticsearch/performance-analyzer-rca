package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Net_TCP_TxQ extends Metric {
  public Net_TCP_TxQ(long evaluationIntervalSeconds) {
    super(AllMetrics.TCPValue.Net_TCP_TXQ.name(), evaluationIntervalSeconds);
  }
}
