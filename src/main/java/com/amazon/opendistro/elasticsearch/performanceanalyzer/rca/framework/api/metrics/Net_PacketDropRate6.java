package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Net_PacketDropRate6 extends Metric {
  public Net_PacketDropRate6(long evaluationIntervalSeconds) {
    super(AllMetrics.IPValue.NET_PACKET_DROP_RATE6.name(), evaluationIntervalSeconds);
  }
}
