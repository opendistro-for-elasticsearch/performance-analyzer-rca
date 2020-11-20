package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import org.junit.Assert;
import org.junit.Before;

public class NetworkE2ECollectorTest extends AbstractCollectorTest {
  @Before
  public void setup() {
    setUut(new NetworkE2ECollector());
  }

  @Override
  public void validateMetric(String metric) throws Exception{
    TCPStatus tcpStatus = mapper.readValue(metric, TCPStatus.class);
    Assert.assertFalse(tcpStatus.getDest().isEmpty());
    // TODO implement further validation of the MetricStatus
    int numFlows = tcpStatus.getNumFlows();
    Assert.assertTrue(numFlows >= 0);
    double txQ = tcpStatus.getTxQ();
    Assert.assertTrue(txQ >= 0);
    double rxQ = tcpStatus.getRxQ();
    Assert.assertTrue(rxQ >= 0);
    double curLost = tcpStatus.getCurLost();
    Assert.assertTrue(curLost >= 0);
    double sndCWND = tcpStatus.getSndCWND();
    Assert.assertTrue(sndCWND >= 0);
    double ssThresh = tcpStatus.getSSThresh();
    Assert.assertTrue(ssThresh >= 0);
  }
}
