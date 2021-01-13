package com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;

public class NetworkE2ECollectorTest extends AbstractCollectorTest {
  private static final Logger LOG = LogManager.getLogger(NetworkE2ECollectorTest.class);
  @Before
  public void setup() {
    setUut(new NetworkE2ECollector());
  }

  @Override
  public void validateMetric(String metric) throws Exception {
    TCPStatus tcpStatus = mapper.readValue(metric, TCPStatus.class);
    Assert.assertFalse(tcpStatus.getDest().isEmpty());
    // TODO implement further validation of the MetricStatus
    int numFlows = tcpStatus.getNumFlows();
    double txQ = tcpStatus.getTxQ();
    double rxQ = tcpStatus.getRxQ();
    double curLost = tcpStatus.getCurLost();
    double sndCWND = tcpStatus.getSndCWND();
    double ssThresh = tcpStatus.getSsThresh();
    LOG.info("numFlows {}, txQ {}, rxQ {}, curLost {}, sendCWND {}, ssThresh {}",
        numFlows, txQ, rxQ, curLost, sndCWND, ssThresh);
  }
}
