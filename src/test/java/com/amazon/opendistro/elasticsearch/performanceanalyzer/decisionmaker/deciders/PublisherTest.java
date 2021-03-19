/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ActionListener;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.collator.Collator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.Plugin;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.Persistable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.google.common.collect.Lists;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class PublisherTest {

  private static final int EVAL_INTERVAL_S = 5;
  private static Publisher publisher;

  // Mock objects
  @Mock
  private Collator collator;

  @Mock
  private Decision decision;

  @Mock
  private Action action;

  @Mock
  private ActionListener actionListener;

  @Mock
  private FlowUnitOperationArgWrapper flowUnitOperationArgWrapper;

  @Mock
  private Persistable persistable;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    publisher = new Publisher(EVAL_INTERVAL_S, collator);
    publisher.addActionListener(actionListener);
    List<Decision> decisionList = Lists.newArrayList(decision);
    Mockito.when(collator.getFlowUnits()).thenReturn(decisionList);
    Mockito.when(decision.getActions()).thenReturn(Lists.newArrayList(action));
    Mockito.when(flowUnitOperationArgWrapper.getPersistable()).thenReturn(persistable);
  }

  @Test
  public void testRejectsFlipFlops() throws Exception {
    // Setup testing objects
    NodeKey nodeKey = new NodeKey(new InstanceDetails.Id("A"), new InstanceDetails.Ip("127.0.0.1"));
    ImpactVector allDecrease = new ImpactVector();
    allDecrease.decreasesPressure(Dimension.values());
    Map<NodeKey, ImpactVector> impactVectorMap = new HashMap<>();
    impactVectorMap.put(nodeKey, allDecrease);
    Mockito.when(action.name()).thenReturn("testIsCooledOffAction");
    Mockito.when(action.coolOffPeriodInMillis()).thenReturn(500L);
    Mockito.when(action.impact()).thenReturn(impactVectorMap);
    // Record a flip flopping action
    publisher.getFlipFlopDetector().recordAction(action);
    ImpactVector allIncrease = new ImpactVector();
    allIncrease.increasesPressure(Dimension.values());
    Map<NodeKey, ImpactVector> increaseMap = new HashMap<>();
    increaseMap.put(nodeKey, allIncrease);
    Mockito.when(action.impact()).thenReturn(increaseMap);
    Thread.sleep(1000L);
    // Even though our action has cooled off, it will flip flop, so the publisher shouldn't
    // execute it
    publisher.compute(flowUnitOperationArgWrapper);
    Mockito.verify(actionListener, Mockito.times(0)).actionPublished(action);
  }

  @Test
  public void testListenerInvocations() {
    Mockito.when(collator.getFlowUnits()).thenReturn(Collections.singletonList(decision));
    Mockito.when(decision.getActions()).thenReturn(Lists.newArrayList(action));
    Mockito.when(action.name()).thenReturn("testAction");
    Mockito.when(action.coolOffPeriodInMillis()).thenReturn(0L);

    ActionListener actionListener2 = Mockito.mock(ActionListener.class);
    ActionListener testActionListener = Mockito.mock(TestActionListener.class);
    publisher.addActionListener(actionListener2);
    publisher.addActionListener(testActionListener);

    publisher.compute(flowUnitOperationArgWrapper);
    Mockito.verify(actionListener, Mockito.times(1)).actionPublished(action);
    Mockito.verify(actionListener2, Mockito.times(1)).actionPublished(action);
    Mockito.verify(testActionListener, Mockito.times(1)).actionPublished(action);
  }

  public static class TestActionListener extends Plugin implements ActionListener {

    @Override
    public void actionPublished(Action action) {
      assert true;
    }

    @Override
    public String name() {
      return "Test_Plugin";
    }
  }
}
