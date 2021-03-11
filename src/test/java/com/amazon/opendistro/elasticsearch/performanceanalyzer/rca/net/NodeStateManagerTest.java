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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse.SubscriptionStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessorTestHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NodeStateManagerTest {

  private static final String TEST_HOST_1 = "host1";
  private static final String IP_1 = "127.0.0.1";
  private static final String TEST_NODE_1 = "node1";

  private static final String TEST_HOST_2 = "host2";
  private static final String IP_2 = "127.0.0.2";

  private static final String TEST_HOST_3 = "host3";
  private static final String IP_3 = "127.0.0.3";

  private static final String TEST_HOST_4 = "host4";

  private static final int MS_IN_S = 1000;
  private static final int TEN_S_IN_MILLIS = 10 * MS_IN_S;
  private static final ClusterDetailsEventProcessor.NodeDetails EMPTY_DETAILS =
          ClusterDetailsEventProcessorTestHelper.newNodeDetails("idid", "0.0.0.0", false);

  private NodeStateManager testNodeStateManager;

  @Before
  public void setUp() {
    this.testNodeStateManager = new NodeStateManager(new AppContext());
  }

  @Test
  public void updateReceiveTime() {
    final long currentTime = System.currentTimeMillis();
    testNodeStateManager.updateReceiveTime(new InstanceDetails.Id(TEST_HOST_1), TEST_NODE_1, currentTime);
    Assert.assertEquals(currentTime, testNodeStateManager.getLastReceivedTimestamp(TEST_NODE_1,
        new InstanceDetails.Id(TEST_HOST_1)));
  }

  @Test
  public void getLastReceivedTimestamp() {
    final long currentTime = System.currentTimeMillis();
    Assert.assertEquals(0, testNodeStateManager.getLastReceivedTimestamp(TEST_NODE_1, new InstanceDetails.Id(TEST_HOST_2)));

    testNodeStateManager.updateReceiveTime(new InstanceDetails.Id(TEST_HOST_2), TEST_NODE_1, currentTime);
    Assert.assertEquals(currentTime, testNodeStateManager.getLastReceivedTimestamp(TEST_NODE_1,
        new InstanceDetails.Id(TEST_HOST_2)));
  }

  @Test
  public void testNewNodesAddedToCluster() {
    final long currentTime = System.currentTimeMillis();
    testNodeStateManager.updateReceiveTime(new InstanceDetails.Id(TEST_HOST_1), TEST_NODE_1, currentTime);

    testNodeStateManager
        .updateSubscriptionState(TEST_NODE_1, new InstanceDetails.Id(TEST_HOST_1), SubscriptionStatus.SUCCESS);

    ClusterDetailsEventProcessor clusterDetailsEventProcessor = new ClusterDetailsEventProcessor();
    clusterDetailsEventProcessor.setNodesDetails(Lists.newArrayList(
            EMPTY_DETAILS,
            ClusterDetailsEventProcessorTestHelper.newNodeDetails(TEST_HOST_1, IP_1, false),
            ClusterDetailsEventProcessorTestHelper.newNodeDetails(TEST_HOST_2, IP_2, false)
    ));

    testNodeStateManager.getAppContext().setClusterDetailsEventProcessor(clusterDetailsEventProcessor);

    ImmutableList<InstanceDetails> hostsToSubscribeTo = testNodeStateManager.getStaleOrNotSubscribedNodes(
            TEST_NODE_1,
            TEN_S_IN_MILLIS,
            ImmutableSet.of(new InstanceDetails.Id(TEST_HOST_1)));

    Assert.assertEquals(1, hostsToSubscribeTo.size());
    Assert.assertEquals(TEST_HOST_2, hostsToSubscribeTo.get(0).getInstanceId().toString());
  }

  @Test
  public void testStaleNodesAndNewNodesAddedToCluster() {
    final long currentTime = System.currentTimeMillis();

    testNodeStateManager.updateReceiveTime(new InstanceDetails.Id(TEST_HOST_1), TEST_NODE_1,
        currentTime - 2 * TEN_S_IN_MILLIS);
    testNodeStateManager
        .updateSubscriptionState(TEST_NODE_1, new InstanceDetails.Id(TEST_HOST_1), SubscriptionStatus.SUCCESS);

    testNodeStateManager.updateReceiveTime(new InstanceDetails.Id(TEST_HOST_2), TEST_NODE_1, currentTime);
    testNodeStateManager.updateSubscriptionState(TEST_NODE_1, new InstanceDetails.Id(TEST_HOST_2), SubscriptionStatus.SUCCESS);

    ClusterDetailsEventProcessor clusterDetailsEventProcessor = new ClusterDetailsEventProcessor();
    clusterDetailsEventProcessor.setNodesDetails(Lists.newArrayList(
            EMPTY_DETAILS,
            ClusterDetailsEventProcessorTestHelper.newNodeDetails(TEST_HOST_1, IP_1, false),
            ClusterDetailsEventProcessorTestHelper.newNodeDetails(TEST_HOST_2, IP_2,  false),
            ClusterDetailsEventProcessorTestHelper.newNodeDetails(TEST_HOST_3, IP_3, false)
    ));
    testNodeStateManager.getAppContext().setClusterDetailsEventProcessor(clusterDetailsEventProcessor);

    ImmutableSet<InstanceDetails.Id> hostSet = ImmutableSet.of(
            new InstanceDetails.Id(TEST_HOST_1), // We will send subscription as it has become stale.
            new InstanceDetails.Id(TEST_HOST_2),  // We don't expect to send subscription to host2 as we received one recently.
            new InstanceDetails.Id(TEST_HOST_4));  // Although this is in the publisher list but not in ClusterDetails, so we will
    // exclude it
    ImmutableList<InstanceDetails> hostsToSubscribeTo = testNodeStateManager.getStaleOrNotSubscribedNodes(
            TEST_NODE_1,
            TEN_S_IN_MILLIS,
            hostSet);

    Assert.assertEquals(2, hostsToSubscribeTo.size());

    Set<String> expectedIs =
            hostsToSubscribeTo.stream().map(InstanceDetails::getInstanceId).map(InstanceDetails.Id::toString).collect(Collectors.toSet());
    Assert.assertTrue(expectedIs.contains(TEST_HOST_1));
    Assert.assertTrue(expectedIs.contains(TEST_HOST_3));
  }
}