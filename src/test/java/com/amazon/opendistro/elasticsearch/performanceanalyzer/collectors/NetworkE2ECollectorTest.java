/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

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
