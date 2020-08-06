package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.collator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Id;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Ip;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class SingleNodeImpactActionGrouperTest {
  private ActionGrouper testActionGrouper;

  private NodeKey nodeKey1;
  private NodeKey nodeKey2;

  @Mock
  private Action singleNodeImpactAction1;

  @Mock
  private Action singleNodeImpactAction2;

  @Mock
  private Action multiNodeImpactAction1;

  @Before
  public void setup() {
    initMocks(this);
    this.nodeKey1 = new NodeKey(new Id("node1"), new Ip("5.6.7.8"));
    this.nodeKey2 = new NodeKey(new Id("node2"), new Ip("5.6.7.9"));
    when(singleNodeImpactAction1.impactedNodes()).thenReturn(ImmutableList.of(nodeKey1));
    when(singleNodeImpactAction2.impactedNodes()).thenReturn(ImmutableList.of(nodeKey2));
    when(multiNodeImpactAction1.impactedNodes()).thenReturn(ImmutableList.of(nodeKey1, nodeKey2));
    this.testActionGrouper = new SingleNodeImpactActionGrouper();
  }

  @Test
  public void testAllSingleNodeImpactActions() {
    Map<NodeKey, List<Action>> groupedActions =
        testActionGrouper
            .groupByNodeId(ImmutableList.of(singleNodeImpactAction1, singleNodeImpactAction2));

    assertEquals(2, groupedActions.keySet().size());
    assertTrue(groupedActions.containsKey(nodeKey1));
    assertEquals(1, groupedActions.get(nodeKey1).size());
    assertEquals(singleNodeImpactAction1, groupedActions.get(nodeKey1).get(0));
    assertTrue(groupedActions.containsKey(nodeKey2));
    assertEquals(1, groupedActions.get(nodeKey2).size());
    assertEquals(singleNodeImpactAction2, groupedActions.get(nodeKey2).get(0));
  }

  @Test
  public void testSingleAndMultiNodeImpactActions() {
    Map<NodeKey, List<Action>> groupedActions =
        testActionGrouper
            .groupByNodeId(ImmutableList.of(singleNodeImpactAction1, multiNodeImpactAction1));

    assertEquals(1, groupedActions.keySet().size());
    assertTrue(groupedActions.containsKey(nodeKey1));
    assertFalse(groupedActions.containsKey(nodeKey2));
    assertEquals(1, groupedActions.get(nodeKey1).size());
    assertEquals(singleNodeImpactAction1, groupedActions.get(nodeKey1).get(0));
  }
}