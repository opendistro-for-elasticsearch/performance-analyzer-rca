package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Impact;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ThreadPoolEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.Map;
import org.junit.Test;

public class QueueCapacityTest {

  @Test
  public void testIncreaseCapacity() {
    NodeKey node1 = new NodeKey("node-1", "1.2.3.4");
    QueueCapacity queueCapacity = new QueueCapacity(node1, ThreadPoolEnum.WRITE_QUEUE, 300, true);
    assertTrue(queueCapacity.getDesiredCapacity() > queueCapacity.getCurrentCapacity());
    assertTrue(queueCapacity.isActionable());
    assertEquals(300, queueCapacity.coolOffPeriodInSeconds());
    assertEquals(ThreadPoolEnum.WRITE_QUEUE, queueCapacity.getThreadPool());
    assertEquals(1, queueCapacity.impactedNodes().size());

    Map<Resource, Impact> impact = queueCapacity.impact().get(node1).getImpact();
    assertEquals(Impact.INCREASES_PRESSURE, impact.get(Resource.HEAP));
    assertEquals(Impact.INCREASES_PRESSURE, impact.get(Resource.CPU));
    assertEquals(Impact.INCREASES_PRESSURE, impact.get(Resource.NETWORK));
    assertEquals(Impact.NO_IMPACT, impact.get(Resource.RAM));
    assertEquals(Impact.NO_IMPACT, impact.get(Resource.DISK));
  }

  @Test
  public void testDecreaseCapacity() {
    NodeKey node1 = new NodeKey("node-1", "1.2.3.4");
    QueueCapacity queueCapacity = new QueueCapacity(node1, ThreadPoolEnum.SEARCH_QUEUE, 1500, false);
    assertTrue(queueCapacity.getDesiredCapacity() < queueCapacity.getCurrentCapacity());
    assertTrue(queueCapacity.isActionable());
    assertEquals(300, queueCapacity.coolOffPeriodInSeconds());
    assertEquals(ThreadPoolEnum.SEARCH_QUEUE, queueCapacity.getThreadPool());
    assertEquals(1, queueCapacity.impactedNodes().size());

    Map<Resource, Impact> impact = queueCapacity.impact().get(node1).getImpact();
    assertEquals(Impact.DECREASES_PRESSURE, impact.get(Resource.HEAP));
    assertEquals(Impact.DECREASES_PRESSURE, impact.get(Resource.CPU));
    assertEquals(Impact.DECREASES_PRESSURE, impact.get(Resource.NETWORK));
    assertEquals(Impact.NO_IMPACT, impact.get(Resource.RAM));
    assertEquals(Impact.NO_IMPACT, impact.get(Resource.DISK));
  }

  @Test
  public void testBounds() {
    // TODO: Move to work with test rcaConf when bounds moved to config
    NodeKey node1 = new NodeKey("node-1", "1.2.3.4");
    QueueCapacity searchQueueIncrease = new QueueCapacity(node1, ThreadPoolEnum.SEARCH_QUEUE, 3000, true);
    assertEquals(searchQueueIncrease.getDesiredCapacity(), searchQueueIncrease.getCurrentCapacity());
    assertFalse(searchQueueIncrease.isActionable());
    assertNoImpact(node1, searchQueueIncrease);

    QueueCapacity searchQueueDecrease = new QueueCapacity(node1, ThreadPoolEnum.SEARCH_QUEUE, 1000, false);
    assertEquals(searchQueueIncrease.getDesiredCapacity(), searchQueueIncrease.getCurrentCapacity());
    assertFalse(searchQueueIncrease.isActionable());
    assertNoImpact(node1, searchQueueDecrease);

    QueueCapacity writeQueueIncrease = new QueueCapacity(node1, ThreadPoolEnum.WRITE_QUEUE, 1000, true);
    assertEquals(writeQueueIncrease.getDesiredCapacity(), writeQueueIncrease.getCurrentCapacity());
    assertFalse(writeQueueIncrease.isActionable());
    assertNoImpact(node1, writeQueueIncrease);

    QueueCapacity writeQueueDecrease = new QueueCapacity(node1, ThreadPoolEnum.WRITE_QUEUE, 100, false);
    assertEquals(writeQueueDecrease.getDesiredCapacity(), writeQueueDecrease.getCurrentCapacity());
    assertFalse(writeQueueDecrease.isActionable());
    assertNoImpact(node1, writeQueueDecrease);
  }

  private void assertNoImpact(NodeKey node, QueueCapacity queueCapacity) {
    Map<Resource, Impact> impact = queueCapacity.impact().get(node).getImpact();
    assertEquals(Impact.NO_IMPACT, impact.get(Resource.HEAP));
    assertEquals(Impact.NO_IMPACT, impact.get(Resource.CPU));
    assertEquals(Impact.NO_IMPACT, impact.get(Resource.NETWORK));
    assertEquals(Impact.NO_IMPACT, impact.get(Resource.RAM));
    assertEquals(Impact.NO_IMPACT, impact.get(Resource.DISK));
  }
}
