package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.collator;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.Decider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.Decision;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Id;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Ip;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class CollatorTest {
  private static final long TEST_INTERVAL = 1;
  private static final String TEST_NODE_ID = "node1";
  private static final String TEST_HOST_ADDRESS = "5.6.7.8";
  private Collator testCollator;
  private NodeKey nodeKey;
  private ImpactVector increasingPressure;
  private ImpactVector decreasingPressure;

  @Mock
  private ActionGrouper mockActionGrouper;

  @Mock
  private Decider mockDecider1;

  @Mock
  private Decider mockDecider2;

  @Mock
  private Action mockPressureIncreasingAction;

  @Mock
  private Action mockPressureDecreasingAction;

  @Mock
  private Decision mockPressureIncreasingDecision;

  @Mock
  private Decision mockPressureDecreasingDecision;

  @Before
  public void setup() {
    initMocks(this);
    this.testCollator = new Collator(mockActionGrouper, mockDecider1, mockDecider2);
    this.nodeKey = new NodeKey(new Id(TEST_NODE_ID), new Ip(TEST_HOST_ADDRESS));
    this.increasingPressure = new ImpactVector();
    this.increasingPressure.increasesPressure(Dimension.CPU);
    this.decreasingPressure = new ImpactVector();
    this.decreasingPressure.decreasesPressure(Dimension.CPU);

    when(mockActionGrouper.groupByInstanceId(any())).thenReturn(ImmutableMap.of(nodeKey,
        ImmutableList.of(mockPressureDecreasingAction, mockPressureIncreasingAction)));
    when(mockPressureIncreasingAction.impactedNodes())
        .thenReturn(Collections.singletonList(nodeKey));
    when(mockPressureDecreasingAction.impactedNodes())
        .thenReturn(Collections.singletonList(nodeKey));
    when(mockPressureDecreasingAction.impact())
        .thenReturn(ImmutableMap.of(nodeKey, decreasingPressure));
    when(mockPressureIncreasingAction.impact())
        .thenReturn(ImmutableMap.of(nodeKey, increasingPressure));
    when(mockDecider1.getFlowUnits()).thenReturn(ImmutableList.of(mockPressureIncreasingDecision,
        mockPressureDecreasingDecision));
    when(mockDecider2.getFlowUnits())
        .thenReturn(Collections.singletonList(mockPressureIncreasingDecision));
  }

  @Test
  public void testCollatorDecreasingPressurePolarization() {
    final Decision collatorDecision = testCollator.operate();

    assertEquals(1, collatorDecision.getActions().size());
    assertEquals(mockPressureDecreasingAction, collatorDecision.getActions().get(0));
  }

  @Test
  public void testCollatorIncreasingPressurePolarization() {
    when(mockActionGrouper.groupByInstanceId(any())).thenReturn(ImmutableMap.of(this.nodeKey,
        Collections.singletonList(mockPressureIncreasingAction)));

    final Decision collatorDecision = testCollator.operate();

    assertEquals(1, collatorDecision.getActions().size());
    assertEquals(mockPressureIncreasingAction, collatorDecision.getActions().get(0));
  }
}