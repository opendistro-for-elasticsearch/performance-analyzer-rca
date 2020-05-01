package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(GradleTaskForRca.class)
public class ReceivedFlowUnitStoreTest {

  private static final String TEST_NODE = "testNode";
  private static final String TEST_NODE_2 = "testNode2";
  private static final long TIMESTAMP_1 = 1L;
  private static final long TIMESTAMP_2 = 2L;
  private static final long TIMESTAMP_3 = 3L;
  private static final int Q_SIZE = 1000;
  private static final int NUM_THREADS = 2;

  private ReceivedFlowUnitStore testFlowUnitStore;

  @Before
  public void setUp() throws Exception {
    this.testFlowUnitStore = new ReceivedFlowUnitStore();
  }

  @Test
  public void enqueue() throws InterruptedException {
    testFlowUnitStore = new ReceivedFlowUnitStore(Q_SIZE);
    ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
    final CountDownLatch latch = new CountDownLatch(Q_SIZE / NUM_THREADS);
    final CountDownLatch latch1 = new CountDownLatch(Q_SIZE / NUM_THREADS);
    final List<CountDownLatch> latches = Arrays.asList(latch, latch1);
    for (int i = 0; i < NUM_THREADS; ++i) {
      executorService.execute(new TestEnqueueTask(latches.get(i), testFlowUnitStore));
    }

    latch.await(100, TimeUnit.MILLISECONDS);
    latch1.await(100, TimeUnit.MILLISECONDS);
    Assert.assertEquals(Q_SIZE, testFlowUnitStore.drainNode(TEST_NODE).size());
  }

  @Test
  public void drainNode() {
    Assert.assertEquals(0, testFlowUnitStore.drainNode(TEST_NODE).size());

    testFlowUnitStore.enqueue(TEST_NODE, buildTestFlowUnitMessage());

    Assert.assertEquals(1, testFlowUnitStore.drainNode(TEST_NODE).size());
  }

  @Test
  public void testDrainAll() {
    FlowUnitMessage msg1 = FlowUnitMessage.newBuilder().setTimeStamp(TIMESTAMP_1).build();
    FlowUnitMessage msg2 = FlowUnitMessage.newBuilder().setTimeStamp(TIMESTAMP_2).build();
    FlowUnitMessage msg3 = FlowUnitMessage.newBuilder().setTimeStamp(TIMESTAMP_3).build();
    testFlowUnitStore.enqueue(TEST_NODE, msg1);
    testFlowUnitStore.enqueue(TEST_NODE, msg2);
    testFlowUnitStore.enqueue(TEST_NODE_2, msg3);
    List<FlowUnitMessage> drained = testFlowUnitStore.drainAll();
    Assert.assertEquals(3, drained.size());
    Assert.assertTrue(drained.contains(msg1));
    Assert.assertTrue(drained.contains(msg2));
    Assert.assertTrue(drained.contains(msg3));
  }

  private FlowUnitMessage buildTestFlowUnitMessage() {
    return FlowUnitMessage.newBuilder().build();
  }

  private static class TestEnqueueTask implements Runnable {

    private final CountDownLatch latch;
    private final ReceivedFlowUnitStore testFlowUnitStore;

    private TestEnqueueTask(CountDownLatch latch,
        ReceivedFlowUnitStore testFlowUnitStore) {
      this.latch = latch;
      this.testFlowUnitStore = testFlowUnitStore;
    }

    @Override
    public void run() {
      while (testFlowUnitStore.enqueue(TEST_NODE, FlowUnitMessage.newBuilder().build())) {
        latch.countDown();
      }
    }
  }
}