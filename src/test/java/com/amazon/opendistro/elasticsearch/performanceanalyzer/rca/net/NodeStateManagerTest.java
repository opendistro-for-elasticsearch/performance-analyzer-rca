package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class NodeStateManagerTest {

  private static final String TEST_HOST_1 = "host1";
  private static final String TEST_NODE_1 = "node1";
  private static final String TEST_HOST_2 = "host2";

  private NodeStateManager testNodeStateManager;

  @Before
  public void setUp() {
    this.testNodeStateManager = new NodeStateManager();
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
    Assert.assertTrue(
        testNodeStateManager.getLastReceivedTimestamp(TEST_NODE_1, TEST_HOST_2) > currentTime);

    testNodeStateManager.updateReceiveTime(TEST_HOST_2, TEST_NODE_1, currentTime);
    Assert.assertEquals(currentTime, testNodeStateManager.getLastReceivedTimestamp(TEST_NODE_1,
        TEST_HOST_2));
  }
}