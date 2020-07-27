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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.google.common.collect.Lists;

import java.time.Instant;
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

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        publisher = new Publisher(EVAL_INTERVAL_S, collator);
        List<Decision> decisionList = Lists.newArrayList(decision);
        Mockito.when(collator.getFlowUnits()).thenReturn(decisionList);
        Mockito.when(decision.getActions()).thenReturn(Lists.newArrayList(action));
    }

    @Test
    public void testIsCooledOff() throws Exception {
        Mockito.when(action.name()).thenReturn("testIsCooledOffAction");
        Mockito.when(action.coolOffPeriodInMillis()).thenReturn(100_000L);
        // Verify that a newly initialized publisher doesn't execute an action until the publisher object
        // has been alive for longer than the action's cool off period
        publisher.operate();
        Mockito.verify(action, Mockito.times(0)).execute();
        Mockito.when(action.coolOffPeriodInMillis()).thenReturn(Instant.now().toEpochMilli()
                - publisher.getInitTime() - 1000L);
        publisher.operate();
        Mockito.verify(action, Mockito.times(1)).execute();
        Mockito.reset(action);
        // Verify that a publisher doesn't execute a previously executed action until the action's cool off period
        // has elapsed
        Mockito.when(action.coolOffPeriodInMillis()).thenReturn(3000L);
        publisher.operate();
        Mockito.verify(action, Mockito.times(0)).execute();
        // Verify that a published executes a previously executed action once the action's cool off period has elapsed
        Thread.sleep(4000L);
        publisher.operate();
        Mockito.verify(action, Mockito.times(1)).execute();
    }

    @Test
    public void testRejectsFlipFlops() throws Exception {
        // Setup testing objects
        NodeKey nodeKey = new NodeKey("A", "localhost");
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
        Mockito.verify(action, Mockito.times(0)).execute();
    }
}
