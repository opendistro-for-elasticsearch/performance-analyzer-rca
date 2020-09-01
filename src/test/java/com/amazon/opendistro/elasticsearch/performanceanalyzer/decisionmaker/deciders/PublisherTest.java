/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import static java.time.Instant.ofEpochMilli;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ActionListener;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.CoolOffDetector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.collator.Collator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.Plugin;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Id;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Ip;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.google.common.collect.Lists;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    publisher = new Publisher(EVAL_INTERVAL_S, collator);
    publisher.addActionListener(actionListener);
    List<Decision> decisionList = Lists.newArrayList(decision);
    Mockito.when(collator.getFlowUnits()).thenReturn(decisionList);
    Mockito.when(decision.getActions()).thenReturn(Lists.newArrayList(action));
  }

  @Test
  public void testIsCooledOff() throws Exception {
    NodeKey node1 = new NodeKey(new Id("node1"), new Ip("127.0.0.1"));
    int coolOffPeriod = 100;
    int evalStartTimeStamp = coolOffPeriod + 10;
    CoolOffDetector coolOffDetector = publisher.getCoolOffDetector();
    Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());
    coolOffDetector.setClock(constantClock);
    coolOffDetector.setInitTime(0);

    Mockito.when(action.name()).thenReturn("testIsCooledOffAction");
    Mockito.when(action.impactedNodes()).thenReturn(new ArrayList<>(
        Collections.singletonList(node1)));
    Mockito.when(action.coolOffPeriodInMillis()).thenReturn(TimeUnit.SECONDS.toMillis(coolOffPeriod));
    // Verify that a newly initialized publisher doesn't execute an action until the publisher object
    // has been alive for longer than the action's cool off period
    publisher.operate();
    Mockito.verify(actionListener, Mockito.times(0)).actionPublished(action);

    coolOffDetector.setClock(Clock.offset(constantClock, Duration.ofSeconds(evalStartTimeStamp)));
    publisher.operate();
    Mockito.verify(actionListener, Mockito.times(1)).actionPublished(action);
    // Verify that a publisher doesn't execute a previously executed action until the action's cool off period
    // has elapsed
    Mockito.reset(actionListener);
    coolOffDetector.setClock(Clock.offset(constantClock, Duration.ofSeconds(evalStartTimeStamp + 20)));
    publisher.operate();
    Mockito.verify(actionListener, Mockito.times(0)).actionPublished(action);
    // Verify that a published executes a previously executed action once the action's cool off period has elapsed
    coolOffDetector.setClock(Clock.offset(constantClock, Duration.ofSeconds(evalStartTimeStamp + 120)));
    publisher.operate();
    Mockito.verify(actionListener, Mockito.times(1)).actionPublished(action);
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
    publisher.operate();
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

    publisher.operate();
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
