/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions;

import static java.time.Instant.ofEpochMilli;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Id;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Ip;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class CoolOffDetectorTest {
  private static int EVAL_START_TS_IN_SECOND = 310;
  private NodeKey node1 = new NodeKey(new Id("node1"), new Ip("127.0.0.1"));
  private NodeKey node2 = new NodeKey(new Id("node2"), new Ip("127.0.0.2"));
  private NodeKey node3 = new NodeKey(new Id("node3"), new Ip("127.0.0.3"));

  @Test
  public void testFirstPublish() {
    DummyAction action = new DummyAction("action1", node1);
    Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());
    CoolOffDetector detector = new CoolOffDetector();
    detector.setClock(constantClock);
    detector.setInitTime(0);

    //ts = 0
    Assert.assertFalse(detector.isCooledOff(action));
    //ts = 150
    detector.setClock(Clock.offset(constantClock, Duration.ofSeconds(150)));
    Assert.assertFalse(detector.isCooledOff(action));
    //ts = 310
    detector.setClock(Clock.offset(constantClock, Duration.ofSeconds(310)));
    Assert.assertTrue(detector.isCooledOff(action));
  }

  @Test
  public void testCoolOffWithSingleNodeAction() {
    DummyAction action1Node1 = new DummyAction("action1", node1);
    DummyAction action2Node1 = new DummyAction("action2", node1);
    DummyAction action1Node2 = new DummyAction("action1", node2);

    CoolOffDetector detector = new CoolOffDetector();
    Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());
    detector.setClock(constantClock);
    detector.setInitTime(0);

    //ts = 0
    detector.setClock(Clock.offset(constantClock, Duration.ofSeconds(EVAL_START_TS_IN_SECOND)));
    Assert.assertTrue(detector.isCooledOff(action1Node1));
    detector.recordAction(action1Node1);
    //ts = 150s
    detector.setClock(Clock.offset(constantClock, Duration.ofSeconds(EVAL_START_TS_IN_SECOND + 150)));
    Assert.assertFalse(detector.isCooledOff(action1Node1));
    Assert.assertTrue(detector.isCooledOff(action2Node1));
    detector.recordAction(action2Node1);
    //ts = 310s
    detector.setClock(Clock.offset(constantClock, Duration.ofSeconds(EVAL_START_TS_IN_SECOND + 310)));
    Assert.assertTrue(detector.isCooledOff(action1Node1));
    Assert.assertFalse(detector.isCooledOff(action2Node1));
    detector.recordAction(action1Node1);
    //ts = 460s
    detector.setClock(Clock.offset(constantClock, Duration.ofSeconds(EVAL_START_TS_IN_SECOND + 460)));
    Assert.assertTrue(detector.isCooledOff(action1Node2));
    Assert.assertFalse(detector.isCooledOff(action1Node1));
    Assert.assertTrue(detector.isCooledOff(action2Node1));
  }

  @Test
  public void testCoolOffWithMultiNodesAction() {
    CoolOffDetector detector = new CoolOffDetector();
    Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());
    detector.setClock(constantClock);
    detector.setInitTime(0);

    DummyAction action1 = new DummyAction("action1", node1, node2);
    //ts = 0
    detector.setClock(Clock.offset(constantClock, Duration.ofSeconds(EVAL_START_TS_IN_SECOND)));
    Assert.assertTrue(detector.isCooledOff(action1));
    detector.recordAction(action1);

    //ts = 150s
    detector.setClock(Clock.offset(constantClock, Duration.ofSeconds(EVAL_START_TS_IN_SECOND + 150)));
    DummyAction action2 = new DummyAction("action1", node2, node3);
    Assert.assertFalse(detector.isCooledOff(action2));
    DummyAction action3 = new DummyAction("action1", node3);
    Assert.assertTrue(detector.isCooledOff(action3));
    detector.recordAction(action3);

    //ts = 310s
    detector.setClock(Clock.offset(constantClock, Duration.ofSeconds(EVAL_START_TS_IN_SECOND + 310)));
    DummyAction action4 = new DummyAction("action1", node1, node2, node3);
    Assert.assertFalse(detector.isCooledOff(action4));
    DummyAction action5 = new DummyAction("action2", node1, node2, node3);
    Assert.assertTrue(detector.isCooledOff(action5));
    DummyAction action6 = new DummyAction("action1", node1, node2);
    Assert.assertTrue(detector.isCooledOff(action6));
  }

  private static class DummyAction implements Action {
    private static final long COOL_OFF_PERIOD_IN_MILLIS = 300 * 1_000;
    private String name;
    private List<NodeKey> nodes;

    public DummyAction(String actionName, NodeKey esNode) {
      this.name = actionName;
      this.nodes = new ArrayList<>();
      nodes.add(esNode);
    }

    public DummyAction(String actionName, NodeKey... esNode) {
      this.name = actionName;
      this.nodes = new ArrayList<>(Arrays.asList(esNode));
    }

    @Override
    public boolean isActionable() {
      return true;
    }

    @Override
    public long coolOffPeriodInMillis() {
      return COOL_OFF_PERIOD_IN_MILLIS;
    }

    @Override
    public List<NodeKey> impactedNodes() {
      return nodes;
    }

    @Override
    public Map<NodeKey, ImpactVector> impact() {
      return null;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public String summary() {
      return "Test";
    }
  }
}
