package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;

public class Net_PacketDropRate4 extends Metric {
  public Net_PacketDropRate4(long evaluationIntervalSeconds) {
    super(AllMetrics.IPValue.NET_PACKET_DROP_RATE4.name(), evaluationIntervalSeconds);
  }
}
