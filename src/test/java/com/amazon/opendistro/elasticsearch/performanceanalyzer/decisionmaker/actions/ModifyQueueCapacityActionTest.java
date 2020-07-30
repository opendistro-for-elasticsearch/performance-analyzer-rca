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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Impact;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.Map;
import org.junit.Test;

public class ModifyQueueCapacityActionTest {

  @Test
  public void testIncreaseCapacity() {
    NodeKey node1 = new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
    ModifyQueueCapacityAction modifyQueueCapacityAction = new ModifyQueueCapacityAction(node1, ResourceEnum.WRITE_THREADPOOL, 300, true);
    assertTrue(modifyQueueCapacityAction.getDesiredCapacity() > modifyQueueCapacityAction.getCurrentCapacity());
    assertTrue(modifyQueueCapacityAction.isActionable());
    assertEquals(ModifyQueueCapacityAction.COOL_OFF_PERIOD_IN_MILLIS,
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
    ModifyQueueCapacityAction modifyQueueCapacityAction = new ModifyQueueCapacityAction(node1, ResourceEnum.SEARCH_THREADPOOL, 1500, false);
    assertTrue(modifyQueueCapacityAction.getDesiredCapacity() < modifyQueueCapacityAction.getCurrentCapacity());
    assertTrue(modifyQueueCapacityAction.isActionable());
    assertEquals(ModifyQueueCapacityAction.COOL_OFF_PERIOD_IN_MILLIS,
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
    ModifyQueueCapacityAction searchQueueIncrease = new ModifyQueueCapacityAction(node1, ResourceEnum.SEARCH_THREADPOOL, 3000, true);
    assertEquals(searchQueueIncrease.getDesiredCapacity(), searchQueueIncrease.getCurrentCapacity());
    assertFalse(searchQueueIncrease.isActionable());
    assertNoImpact(node1, searchQueueIncrease);

    ModifyQueueCapacityAction searchQueueDecrease = new ModifyQueueCapacityAction(node1, ResourceEnum.SEARCH_THREADPOOL, 1000, false);
    assertEquals(searchQueueIncrease.getDesiredCapacity(), searchQueueIncrease.getCurrentCapacity());
    assertFalse(searchQueueIncrease.isActionable());
    assertNoImpact(node1, searchQueueDecrease);

    ModifyQueueCapacityAction writeQueueIncrease = new ModifyQueueCapacityAction(node1, ResourceEnum.WRITE_THREADPOOL, 1000, true);
    assertEquals(writeQueueIncrease.getDesiredCapacity(), writeQueueIncrease.getCurrentCapacity());
    assertFalse(writeQueueIncrease.isActionable());
    assertNoImpact(node1, writeQueueIncrease);

    ModifyQueueCapacityAction writeQueueDecrease = new ModifyQueueCapacityAction(node1, ResourceEnum.WRITE_THREADPOOL, 100, false);
    assertEquals(writeQueueDecrease.getDesiredCapacity(), writeQueueDecrease.getCurrentCapacity());
    assertFalse(writeQueueDecrease.isActionable());
    assertNoImpact(node1, writeQueueDecrease);
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
