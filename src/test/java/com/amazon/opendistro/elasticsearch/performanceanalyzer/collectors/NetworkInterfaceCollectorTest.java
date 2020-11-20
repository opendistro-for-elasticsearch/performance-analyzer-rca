package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.NetInterfaceSummary.Direction;
import org.junit.Before;

public class NetworkInterfaceCollectorTest extends AbstractCollectorTest {
  @Before
  public void setup() {
    setUut(new NetworkInterfaceCollector());
  }

  @Override
  public void validateMetric(String metric) throws Exception {
    NetInterfaceSummary interfaceSummary = mapper.readValue(metric, NetInterfaceSummary.class);
    // TODO implement further validation of the MetricStatus
    Direction direction = interfaceSummary.getDirection();
    double packetRate4 = interfaceSummary.getPacketRate4();
    double dropRate4 = interfaceSummary.getDropRate4();
    double packetRate6 = interfaceSummary.getPacketRate6();
    double dropRate6 = interfaceSummary.getPacketRate6();
    double bps = interfaceSummary.getBps();
  }
}
