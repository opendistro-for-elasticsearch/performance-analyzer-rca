package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.SubscribeResponse.SubscriptionStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.util.ClusterUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@Category(GradleTaskForRca.class)
@RunWith(PowerMockRunner.class)
@PrepareForTest(ClusterUtils.class)
public class NodeStateManagerTest {

  private static final String TEST_HOST_1 = "host1";
  private static final String TEST_NODE_1 = "node1";
  private static final String TEST_HOST_2 = "host2";
  private static final String TEST_HOST_3 = "host3";
  private static final int MS_IN_S = 1000;
  private static final int TEN_S_IN_MILLIS = 10 * MS_IN_S;

  private NodeStateManager testNodeStateManager;

  @Before
  public void setUp() {
    this.testNodeStateManager = new NodeStateManager();
    mockStatic(ClusterUtils.class);
  }

  @Test
  public void updateReceiveTime() {
    final long currentTime = System.currentTimeMillis();
    testNodeStateManager.updateReceiveTime(TEST_HOST_1, TEST_NODE_1, currentTime);
    Assert.assertEquals(currentTime, testNodeStateManager.getLastReceivedTimestamp(TEST_NODE_1,
        TEST_HOST_1));
  }

  @Test
  public void getLastReceivedTimestamp() {
    final long currentTime = System.currentTimeMillis();
    Assert.assertEquals(0, testNodeStateManager.getLastReceivedTimestamp(TEST_NODE_1, TEST_HOST_2));

    testNodeStateManager.updateReceiveTime(TEST_HOST_2, TEST_NODE_1, currentTime);
    Assert.assertEquals(currentTime, testNodeStateManager.getLastReceivedTimestamp(TEST_NODE_1,
        TEST_HOST_2));
  }

  @Test
  public void testNewNodesAddedToCluster() {
    final long currentTime = System.currentTimeMillis();
    testNodeStateManager.updateReceiveTime(TEST_HOST_1, TEST_NODE_1, currentTime);

    testNodeStateManager
        .updateSubscriptionState(TEST_NODE_1, TEST_HOST_1, SubscriptionStatus.SUCCESS);
    when(ClusterUtils.getAllPeerHostAddresses())
        .thenReturn(Arrays.asList(TEST_HOST_1, TEST_HOST_2));
    when(ClusterUtils.isHostAddressInCluster(TEST_HOST_1)).thenReturn(true);
    when(ClusterUtils.isHostAddressInCluster(TEST_HOST_2)).thenReturn(true);

    ImmutableList<String> hostsToSubscribeTo =
        testNodeStateManager.getStaleOrNotSubscribedNodes(TEST_NODE_1, TEN_S_IN_MILLIS,
            ImmutableSet.of(TEST_HOST_1));

    Assert.assertEquals(1, hostsToSubscribeTo.size());
    Assert.assertEquals(TEST_HOST_2, hostsToSubscribeTo.get(0));
  }

  @Test
  public void testStaleNodesAndNewNodesAddedToCluster() {
    final long currentTime = System.currentTimeMillis();

    testNodeStateManager.updateReceiveTime(TEST_HOST_1, TEST_NODE_1,
        currentTime - 2 * TEN_S_IN_MILLIS);
    testNodeStateManager
        .updateSubscriptionState(TEST_NODE_1, TEST_HOST_1, SubscriptionStatus.SUCCESS);

    testNodeStateManager.updateReceiveTime(TEST_HOST_2, TEST_NODE_1, currentTime);
    testNodeStateManager.updateSubscriptionState(TEST_NODE_1, TEST_HOST_2, SubscriptionStatus.SUCCESS);

    when(ClusterUtils.getAllPeerHostAddresses())
        .thenReturn(Arrays.asList(TEST_HOST_1, TEST_HOST_2, TEST_HOST_3));
    when(ClusterUtils.isHostAddressInCluster(TEST_HOST_1)).thenReturn(true);
    when(ClusterUtils.isHostAddressInCluster(TEST_HOST_2)).thenReturn(true);

    ImmutableList<String> hostsToSubscribeTo =
        testNodeStateManager.getStaleOrNotSubscribedNodes(TEST_NODE_1, TEN_S_IN_MILLIS,
            ImmutableSet.of(TEST_HOST_1, TEST_HOST_2));

    Assert.assertEquals(2, hostsToSubscribeTo.size());
    Assert.assertTrue(hostsToSubscribeTo.contains(TEST_HOST_1));
    Assert.assertTrue(hostsToSubscribeTo.contains(TEST_HOST_3));
  }
}