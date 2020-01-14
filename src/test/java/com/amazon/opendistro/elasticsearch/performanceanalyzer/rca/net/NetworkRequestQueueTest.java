package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class NetworkRequestQueueTest {

  private static final int QUEUE_SIZE = 10;
  private NetworkRequestQueue<Integer> testQueue;

  @Before
  public void setUp() throws Exception {
    testQueue = new NetworkRequestQueue<>(QUEUE_SIZE);
  }

  @Test
  public void testOffer() {
    int i = 0;
    while (testQueue.offer(i)) {
      ++i;
    }
    Assert.assertEquals(QUEUE_SIZE, i);
  }

  @Test
  public void testDrain() {
    testQueue = new NetworkRequestQueue<>();
    int i = 0;
    while (testQueue.offer(i)) {
      ++i;
    }

    Assert.assertEquals(i, testQueue.drain().size());
  }
}