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
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.DeciderConfig.getDefaultFieldDataCacheUpperBound;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.DeciderConfig.getDefaultShardRequestCacheUpperBound;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Impact;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class ModifyCacheMaxSizeActionTest {
  private final long heapMaxSizeInBytes = 12000 * 1_000_000L;

  private AppContext appContext;

  @Before
  public void setUp() {
    final long fieldDataCacheMaxSizeInBytes = 12000;
    final long shardRequestCacheMaxSizeInBytes = 12000;

    appContext = new AppContext();

    appContext
        .getNodeConfigCache()
        .put(
            new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4")),
            ResourceUtil.HEAP_MAX_SIZE,
            heapMaxSizeInBytes);
    appContext
        .getNodeConfigCache()
        .put(
            new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4")),
            ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE,
            fieldDataCacheMaxSizeInBytes);
    appContext
        .getNodeConfigCache()
        .put(
            new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4")),
            ResourceUtil.SHARD_REQUEST_CACHE_MAX_SIZE,
            shardRequestCacheMaxSizeInBytes);
  }

  @Test
  public void testIncreaseCapacity() {
    NodeKey node1 =
        new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
    ModifyCacheMaxSizeAction.Builder builder =
        ModifyCacheMaxSizeAction.newBuilder(
            node1, ResourceEnum.FIELD_DATA_CACHE, appContext, getDefaultFieldDataCacheUpperBound());
    ModifyCacheMaxSizeAction fieldDataCacheIncrease = builder.increase(true).build();

    assertTrue(
            fieldDataCacheIncrease.getDesiredCacheMaxSizeInBytes()
            > fieldDataCacheIncrease.getCurrentCacheMaxSizeInBytes());
    assertTrue(fieldDataCacheIncrease.isActionable());
    assertEquals(300 * 1_000, fieldDataCacheIncrease.coolOffPeriodInMillis());
    assertEquals(ResourceEnum.FIELD_DATA_CACHE, fieldDataCacheIncrease.getCacheType());
    assertEquals(1, fieldDataCacheIncrease.impactedNodes().size());

    Map<Dimension, Impact> impact = fieldDataCacheIncrease.impact().get(node1).getImpact();
    assertEquals(Impact.INCREASES_PRESSURE, impact.get(HEAP));
    assertEquals(Impact.NO_IMPACT, impact.get(CPU));
    assertEquals(Impact.NO_IMPACT, impact.get(NETWORK));
    assertEquals(Impact.NO_IMPACT, impact.get(RAM));
    assertEquals(Impact.NO_IMPACT, impact.get(DISK));
  }

  @Test
  public void testNoIncreaseCapacity() {
    NodeKey node1 =
        new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
    ModifyCacheMaxSizeAction.Builder builder =
        ModifyCacheMaxSizeAction.newBuilder(
            node1,
            ResourceEnum.FIELD_DATA_CACHE,
            appContext,
            getDefaultFieldDataCacheUpperBound());
    ModifyCacheMaxSizeAction fieldDataCacheNoIncrease = builder.increase(false).build();
    assertEquals(
            fieldDataCacheNoIncrease.getDesiredCacheMaxSizeInBytes(),
            fieldDataCacheNoIncrease.getCurrentCacheMaxSizeInBytes());
    assertFalse(fieldDataCacheNoIncrease.isActionable());
    assertEquals(300 * 1_000, fieldDataCacheNoIncrease.coolOffPeriodInMillis());
    assertEquals(ResourceEnum.FIELD_DATA_CACHE, fieldDataCacheNoIncrease.getCacheType());
    assertEquals(1, fieldDataCacheNoIncrease.impactedNodes().size());

    Map<Dimension, Impact> impact = fieldDataCacheNoIncrease.impact().get(node1).getImpact();
    assertEquals(Impact.NO_IMPACT, impact.get(HEAP));
    assertEquals(Impact.NO_IMPACT, impact.get(CPU));
    assertEquals(Impact.NO_IMPACT, impact.get(NETWORK));
    assertEquals(Impact.NO_IMPACT, impact.get(RAM));
    assertEquals(Impact.NO_IMPACT, impact.get(DISK));
  }

  @Test
  public void testBounds() {
    // TODO: Move to work with test rcaConf when bounds moved to nodeConfiguration rca
    long maxSizeInBytes = (long) (heapMaxSizeInBytes * getDefaultFieldDataCacheUpperBound());
    setNodeConfigCache(ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE, maxSizeInBytes);

    NodeKey node1 =
        new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
    ModifyCacheMaxSizeAction.Builder builder =
        ModifyCacheMaxSizeAction.newBuilder(
            node1,
            ResourceEnum.FIELD_DATA_CACHE,
            appContext,
            getDefaultFieldDataCacheUpperBound());
    ModifyCacheMaxSizeAction fieldCacheIncrease = builder.increase(true).build();
    assertEquals(
        fieldCacheIncrease.getDesiredCacheMaxSizeInBytes(),
        fieldCacheIncrease.getCurrentCacheMaxSizeInBytes());
    assertFalse(fieldCacheIncrease.isActionable());
    assertNoImpact(node1, fieldCacheIncrease);

    maxSizeInBytes = (long) (heapMaxSizeInBytes * getDefaultShardRequestCacheUpperBound());
    setNodeConfigCache(ResourceUtil.SHARD_REQUEST_CACHE_MAX_SIZE, maxSizeInBytes);

    builder =
        ModifyCacheMaxSizeAction.newBuilder(
            node1,
            ResourceEnum.SHARD_REQUEST_CACHE,
            appContext,
            getDefaultShardRequestCacheUpperBound());
    ModifyCacheMaxSizeAction shardRequestCacheIncrease = builder.increase(true).build();
    assertEquals(
        shardRequestCacheIncrease.getDesiredCacheMaxSizeInBytes(),
        shardRequestCacheIncrease.getCurrentCacheMaxSizeInBytes());
    assertFalse(shardRequestCacheIncrease.isActionable());
    assertNoImpact(node1, shardRequestCacheIncrease);
  }

  @Test
  public void testMutedAction() {
    NodeKey node1 =
        new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
    ModifyCacheMaxSizeAction.Builder builder =
        ModifyCacheMaxSizeAction.newBuilder(
            node1,
            ResourceEnum.FIELD_DATA_CACHE,
            appContext,
            getDefaultFieldDataCacheUpperBound());
    ModifyCacheMaxSizeAction modifyCacheSizeAction = builder.increase(true).build();

    appContext.updateMutedActions(ImmutableSet.of(modifyCacheSizeAction.name()));

    assertFalse(modifyCacheSizeAction.isActionable());
  }

  private void assertNoImpact(NodeKey node, ModifyCacheMaxSizeAction modifyCacheSizeAction) {
    Map<Dimension, Impact> impact = modifyCacheSizeAction.impact().get(node).getImpact();
    assertEquals(Impact.NO_IMPACT, impact.get(HEAP));
    assertEquals(Impact.NO_IMPACT, impact.get(CPU));
    assertEquals(Impact.NO_IMPACT, impact.get(NETWORK));
    assertEquals(Impact.NO_IMPACT, impact.get(RAM));
    assertEquals(Impact.NO_IMPACT, impact.get(DISK));
  }

  private void setNodeConfigCache(final Resource resource, final long maxSizeInBytes) {
    appContext
        .getNodeConfigCache()
        .put(
            new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4")),
            resource,
            maxSizeInBytes);
  }
}
