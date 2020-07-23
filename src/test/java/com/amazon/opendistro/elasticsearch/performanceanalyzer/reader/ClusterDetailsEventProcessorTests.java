/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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


package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader;

import static org.junit.Assert.assertEquals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor.NodeDetails;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class ClusterDetailsEventProcessorTests {

  @Test
  public void testProcessEvent() throws Exception {

    String nodeId1 = "s7gDCVnCSiuBgHoYLji1gw";
    String address1 = "10.212.49.140";
    boolean isMasterNode1 = true;

    String nodeId2 = "Zn1QcSUGT--DciD1Em5wRg";
    String address2 = "10.212.52.241";
    boolean isMasterNode2 = false;

    ClusterDetailsEventProcessor clusterDetailsEventProcessor;
    try {
      ClusterDetailsEventProcessorTestHelper clusterDetailsEventProcessorTestHelper = new ClusterDetailsEventProcessorTestHelper();
      clusterDetailsEventProcessorTestHelper.addNodeDetails(nodeId1, address1, isMasterNode1);
      clusterDetailsEventProcessorTestHelper.addNodeDetails(nodeId2, address2, isMasterNode2);
      clusterDetailsEventProcessor = clusterDetailsEventProcessorTestHelper.generateClusterDetailsEvent();
    } catch (Exception e) {
      Assert.assertTrue("got exception when generating cluster details event", false);
      return;
    }

    List<NodeDetails> nodes = clusterDetailsEventProcessor.getNodesDetails();

    assertEquals(nodeId1, nodes.get(0).getId());
    assertEquals(address1, nodes.get(0).getHostAddress());
    assertEquals(isMasterNode1, nodes.get(0).getIsMasterNode());

    assertEquals(nodeId2, nodes.get(1).getId());
    assertEquals(address2, nodes.get(1).getHostAddress());
    assertEquals(isMasterNode2, nodes.get(1).getIsMasterNode());
  }
}
