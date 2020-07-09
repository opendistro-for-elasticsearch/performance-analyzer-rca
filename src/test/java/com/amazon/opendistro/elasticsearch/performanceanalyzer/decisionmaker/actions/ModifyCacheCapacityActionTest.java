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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.Map;
import org.junit.Test;

public class ModifyCacheCapacityActionTest {

  @Test
  public void testIncreaseCapacity() {
    NodeKey node1 = new NodeKey("node-1", "1.2.3.4");
    ModifyCacheCapacityAction modifyCacheCapacityAction =
        new ModifyCacheCapacityAction(node1, ResourceEnum.FIELD_DATA_CACHE, 5000, true);
    assertTrue(
        modifyCacheCapacityAction.getDesiredCapacity() > modifyCacheCapacityAction.getCurrentCapacity());
    assertTrue(modifyCacheCapacityAction.isActionable());
    assertEquals(300, modifyCacheCapacityAction.coolOffPeriodInSeconds());
    assertEquals(ResourceEnum.FIELD_DATA_CACHE, modifyCacheCapacityAction.getCacheType());
    assertEquals(1, modifyCacheCapacityAction.impactedNodes().size());

    Map<Dimension, Impact> impact = modifyCacheCapacityAction.impact().get(node1).getImpact();
    assertEquals(Impact.INCREASES_PRESSURE, impact.get(HEAP));
    assertEquals(Impact.NO_IMPACT, impact.get(CPU));
    assertEquals(Impact.NO_IMPACT, impact.get(NETWORK));
    assertEquals(Impact.NO_IMPACT, impact.get(RAM));
    assertEquals(Impact.NO_IMPACT, impact.get(DISK));
  }

  @Test
  public void testDecreaseCapacity() {
    NodeKey node1 = new NodeKey("node-1", "1.2.3.4");
    ModifyCacheCapacityAction modifyCacheCapacityAction =
        new ModifyCacheCapacityAction(node1, ResourceEnum.FIELD_DATA_CACHE, 5000, false);
    assertTrue(
        modifyCacheCapacityAction.getDesiredCapacity()
            < modifyCacheCapacityAction.getCurrentCapacity());
    assertTrue(modifyCacheCapacityAction.isActionable());
    assertEquals(300, modifyCacheCapacityAction.coolOffPeriodInSeconds());
    assertEquals(ResourceEnum.FIELD_DATA_CACHE, modifyCacheCapacityAction.getCacheType());
    assertEquals(1, modifyCacheCapacityAction.impactedNodes().size());

    Map<Dimension, Impact> impact = modifyCacheCapacityAction.impact().get(node1).getImpact();
    assertEquals(Impact.DECREASES_PRESSURE, impact.get(HEAP));
    assertEquals(Impact.NO_IMPACT, impact.get(CPU));
    assertEquals(Impact.NO_IMPACT, impact.get(NETWORK));
    assertEquals(Impact.NO_IMPACT, impact.get(RAM));
    assertEquals(Impact.NO_IMPACT, impact.get(DISK));
  }

  @Test
  public void testBounds() {
    // TODO: Move to work with test rcaConf when bounds moved to nodeConfiguration rca
    NodeKey node1 = new NodeKey("node-1", "1.2.3.4");
    ModifyCacheCapacityAction fieldCacheIncrease =
        new ModifyCacheCapacityAction(node1, ResourceEnum.FIELD_DATA_CACHE, 12000, true);
    assertEquals(fieldCacheIncrease.getDesiredCapacity(), fieldCacheIncrease.getCurrentCapacity());
    assertFalse(fieldCacheIncrease.isActionable());
    assertNoImpact(node1, fieldCacheIncrease);

    ModifyCacheCapacityAction fieldCacheDecrease =
        new ModifyCacheCapacityAction(node1, ResourceEnum.FIELD_DATA_CACHE, 1000, false);
    assertEquals(fieldCacheDecrease.getDesiredCapacity(), fieldCacheDecrease.getCurrentCapacity());
    assertFalse(fieldCacheDecrease.isActionable());
    assertNoImpact(node1, fieldCacheDecrease);

    ModifyCacheCapacityAction shardRequestCacheIncrease =
        new ModifyCacheCapacityAction(node1, ResourceEnum.SHARD_REQUEST_CACHE, 12000, true);
    assertEquals(shardRequestCacheIncrease.getDesiredCapacity(), shardRequestCacheIncrease.getCurrentCapacity());
    assertFalse(shardRequestCacheIncrease.isActionable());
    assertNoImpact(node1, shardRequestCacheIncrease);

    ModifyCacheCapacityAction shardRequestCacheDecrease =
        new ModifyCacheCapacityAction(node1, ResourceEnum.SHARD_REQUEST_CACHE, 0, false);
    assertEquals(shardRequestCacheDecrease.getDesiredCapacity(), shardRequestCacheDecrease.getCurrentCapacity());
    assertFalse(shardRequestCacheDecrease.isActionable());
    assertNoImpact(node1, shardRequestCacheDecrease);
  }

  private void assertNoImpact(NodeKey node, ModifyCacheCapacityAction modifyCacheCapacityAction) {
    Map<Dimension, Impact> impact = modifyCacheCapacityAction.impact().get(node).getImpact();
    assertEquals(Impact.NO_IMPACT, impact.get(HEAP));
    assertEquals(Impact.NO_IMPACT, impact.get(CPU));
    assertEquals(Impact.NO_IMPACT, impact.get(NETWORK));
    assertEquals(Impact.NO_IMPACT, impact.get(RAM));
    assertEquals(Impact.NO_IMPACT, impact.get(DISK));
  }
}
