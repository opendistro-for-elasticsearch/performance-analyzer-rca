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

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.CPU;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.DISK;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.HEAP;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.NETWORK;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension.RAM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Impact;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyQueueCapacityAction.Builder;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigCache;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class ModifyQueueCapacityActionTest {

  private AppContext testAppContext = new AppContext();
  private NodeConfigCache dummyCache = testAppContext.getNodeConfigCache();

  @Test
  public void testIncreaseCapacity() {
    NodeKey node1 = new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
    dummyCache.put(node1, ResourceUtil.WRITE_QUEUE_CAPACITY, 500);
    ModifyQueueCapacityAction.Builder builder =
        ModifyQueueCapacityAction.newBuilder(node1, ResourceEnum.WRITE_THREADPOOL, testAppContext);
    ModifyQueueCapacityAction modifyQueueCapacityAction = builder.increase(true).build();
    Assert.assertNotNull(modifyQueueCapacityAction);
    assertTrue(modifyQueueCapacityAction.getDesiredCapacity() > modifyQueueCapacityAction.getCurrentCapacity());
    assertTrue(modifyQueueCapacityAction.isActionable());
    assertEquals(Builder.DEFAULT_COOL_OFF_PERIOD_IN_MILLIS,
            modifyQueueCapacityAction.coolOffPeriodInMillis());
    assertEquals(ResourceEnum.WRITE_THREADPOOL, modifyQueueCapacityAction.getThreadPool());
    assertEquals(1, modifyQueueCapacityAction.impactedNodes().size());

    Map<Dimension, Impact> impact = modifyQueueCapacityAction.impact().get(node1).getImpact();
    assertEquals(Impact.INCREASES_PRESSURE, impact.get(HEAP));
    assertEquals(Impact.INCREASES_PRESSURE, impact.get(CPU));
    assertEquals(Impact.INCREASES_PRESSURE, impact.get(NETWORK));
    assertEquals(Impact.NO_IMPACT, impact.get(RAM));
    assertEquals(Impact.NO_IMPACT, impact.get(DISK));
  }

  @Test
  public void testDecreaseCapacity() {
    NodeKey node1 = new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
    dummyCache.put(node1, ResourceUtil.SEARCH_QUEUE_CAPACITY, 1500);
    ModifyQueueCapacityAction.Builder builder =
        ModifyQueueCapacityAction.newBuilder(node1, ResourceEnum.SEARCH_THREADPOOL, testAppContext);
    ModifyQueueCapacityAction modifyQueueCapacityAction = builder.increase(false).build();
    Assert.assertNotNull(modifyQueueCapacityAction);
    assertTrue(modifyQueueCapacityAction.getDesiredCapacity() < modifyQueueCapacityAction.getCurrentCapacity());
    assertTrue(modifyQueueCapacityAction.isActionable());
    assertEquals(Builder.DEFAULT_COOL_OFF_PERIOD_IN_MILLIS,
            modifyQueueCapacityAction.coolOffPeriodInMillis());
    assertEquals(ResourceEnum.SEARCH_THREADPOOL, modifyQueueCapacityAction.getThreadPool());
    assertEquals(1, modifyQueueCapacityAction.impactedNodes().size());

    Map<Dimension, Impact> impact = modifyQueueCapacityAction.impact().get(node1).getImpact();
    assertEquals(Impact.DECREASES_PRESSURE, impact.get(HEAP));
    assertEquals(Impact.DECREASES_PRESSURE, impact.get(CPU));
    assertEquals(Impact.DECREASES_PRESSURE, impact.get(NETWORK));
    assertEquals(Impact.NO_IMPACT, impact.get(RAM));
    assertEquals(Impact.NO_IMPACT, impact.get(DISK));
  }

  @Test
  public void testBounds() {
    // TODO: Move to work with test rcaConf when bounds moved to config
    NodeKey node1 = new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
    dummyCache.put(node1, ResourceUtil.SEARCH_QUEUE_CAPACITY, 3000);
    ModifyQueueCapacityAction.Builder builder =
        ModifyQueueCapacityAction.newBuilder(node1, ResourceEnum.SEARCH_THREADPOOL, testAppContext);
    ModifyQueueCapacityAction searchQueueIncrease = builder.increase(true).build();
    assertEquals(searchQueueIncrease.getDesiredCapacity(), searchQueueIncrease.getCurrentCapacity());
    assertFalse(searchQueueIncrease.isActionable());
    assertNoImpact(node1, searchQueueIncrease);

    dummyCache.put(node1, ResourceUtil.SEARCH_QUEUE_CAPACITY, 1000);
    builder = ModifyQueueCapacityAction.newBuilder(node1, ResourceEnum.SEARCH_THREADPOOL, testAppContext);
    ModifyQueueCapacityAction searchQueueDecrease = builder.increase(false).build();
    assertEquals(searchQueueDecrease.getDesiredCapacity(), searchQueueDecrease.getCurrentCapacity());
    assertFalse(searchQueueDecrease.isActionable());
    assertNoImpact(node1, searchQueueDecrease);

    dummyCache.put(node1, ResourceUtil.WRITE_QUEUE_CAPACITY, 1000);
    builder = ModifyQueueCapacityAction.newBuilder(node1, ResourceEnum.WRITE_THREADPOOL, testAppContext);
    ModifyQueueCapacityAction writeQueueIncrease = builder.increase(true).build();
    assertEquals(writeQueueIncrease.getDesiredCapacity(), writeQueueIncrease.getCurrentCapacity());
    assertFalse(writeQueueIncrease.isActionable());
    assertNoImpact(node1, writeQueueIncrease);

    dummyCache.put(node1, ResourceUtil.WRITE_QUEUE_CAPACITY, 100);
    builder = ModifyQueueCapacityAction.newBuilder(node1, ResourceEnum.WRITE_THREADPOOL, testAppContext);
    ModifyQueueCapacityAction writeQueueDecrease = builder.increase(false).build();
    assertEquals(writeQueueDecrease.getDesiredCapacity(), writeQueueDecrease.getCurrentCapacity());
    assertFalse(writeQueueDecrease.isActionable());
    assertNoImpact(node1, writeQueueDecrease);
  }

  @Test
  public void testMutedAction() {
    NodeKey node1 = new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
    dummyCache.put(node1, ResourceUtil.SEARCH_QUEUE_CAPACITY, 2000);
    ModifyQueueCapacityAction.Builder builder =
        ModifyQueueCapacityAction.newBuilder(node1, ResourceEnum.SEARCH_THREADPOOL, testAppContext);
    ModifyQueueCapacityAction modifyQueueCapacityAction = builder.increase(true).build();

    testAppContext.updateMutedActions(ImmutableSet.of(modifyQueueCapacityAction.name()));

    assertFalse(modifyQueueCapacityAction.isActionable());
  }

  private void assertNoImpact(NodeKey node, ModifyQueueCapacityAction modifyQueueCapacityAction) {
    Map<Dimension, Impact> impact = modifyQueueCapacityAction.impact().get(node).getImpact();
    assertEquals(Impact.NO_IMPACT, impact.get(HEAP));
    assertEquals(Impact.NO_IMPACT, impact.get(CPU));
    assertEquals(Impact.NO_IMPACT, impact.get(NETWORK));
    assertEquals(Impact.NO_IMPACT, impact.get(RAM));
    assertEquals(Impact.NO_IMPACT, impact.get(DISK));
  }
}
