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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.decider.CacheBoundConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.google.common.collect.ImmutableSet;
import java.nio.file.Paths;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class ModifyCacheMaxSizeActionTest {
  private final long heapMaxSizeInBytes = 12000 * 1_000_000L;
  private final long fieldDataCacheMaxSizeInBytes = 12000;
  private final long shardRequestCacheMaxSizeInBytes = 12000;

  private AppContext appContext;
  private CacheBoundConfig cacheBoundConfig;

  @Before
  public void setUp() {
    appContext = new AppContext();
    String rcaConfPath = Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString();
    RcaConf rcaConf = new RcaConf(rcaConfPath);
    cacheBoundConfig = rcaConf.getDeciderConfig().getCacheBoundConfig();
  }

  @Test
  public void testIncreaseCapacity() {
    populateNodeConfigCache();
    NodeKey node1 =
        new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
    ModifyCacheMaxSizeAction.Builder builder =
        ModifyCacheMaxSizeAction.newBuilder(
            node1, ResourceEnum.FIELD_DATA_CACHE, appContext);
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
    populateNodeConfigCache();
    NodeKey node1 =
        new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
    ModifyCacheMaxSizeAction.Builder builder =
        ModifyCacheMaxSizeAction.newBuilder(
            node1, ResourceEnum.FIELD_DATA_CACHE, appContext);
    ModifyCacheMaxSizeAction fieldDataCacheNoIncrease = builder.increase(false).build();
    assertEquals(
        fieldDataCacheNoIncrease.getDesiredCacheMaxSizeInBytes(),
        fieldDataCacheNoIncrease.getCurrentCacheMaxSizeInBytes());
    assertFalse(fieldDataCacheNoIncrease.isActionable());
    assertEquals(300 * 1_000, fieldDataCacheNoIncrease.coolOffPeriodInMillis());
    assertEquals(ResourceEnum.FIELD_DATA_CACHE, fieldDataCacheNoIncrease.getCacheType());
    assertEquals(1, fieldDataCacheNoIncrease.impactedNodes().size());
    assertNoImpact(node1, fieldDataCacheNoIncrease);
  }

  @Test
  public void testBounds() {
    // TODO: Move to work with test rcaConf when bounds moved to nodeConfiguration rca
    populateNodeConfigCache();

    long maxSizeInBytes = (long) (heapMaxSizeInBytes * cacheBoundConfig.fieldDataCacheUpperBound());
    setNodeConfigCache(ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE, maxSizeInBytes);

    NodeKey node1 =
        new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
    ModifyCacheMaxSizeAction.Builder builder =
        ModifyCacheMaxSizeAction.newBuilder(
            node1, ResourceEnum.FIELD_DATA_CACHE, appContext);
    ModifyCacheMaxSizeAction fieldDataCacheIncrease = builder.increase(false).build();
    assertEquals(
        fieldDataCacheIncrease.getDesiredCacheMaxSizeInBytes(),
        fieldDataCacheIncrease.getCurrentCacheMaxSizeInBytes());
    assertFalse(fieldDataCacheIncrease.isActionable());
    assertNoImpact(node1, fieldDataCacheIncrease);

    maxSizeInBytes = (long) (heapMaxSizeInBytes * cacheBoundConfig.shardRequestCacheUpperBound());
    setNodeConfigCache(ResourceUtil.SHARD_REQUEST_CACHE_MAX_SIZE, maxSizeInBytes);

    builder =
        ModifyCacheMaxSizeAction.newBuilder(
            node1,
            ResourceEnum.SHARD_REQUEST_CACHE,
            appContext);
    ModifyCacheMaxSizeAction shardRequestCacheIncrease = builder.increase(true).build();
    assertEquals(
        shardRequestCacheIncrease.getDesiredCacheMaxSizeInBytes(),
        shardRequestCacheIncrease.getCurrentCacheMaxSizeInBytes());
    assertFalse(shardRequestCacheIncrease.isActionable());
    assertNoImpact(node1, shardRequestCacheIncrease);
  }

  @Test
  public void testMutedAction() {
    populateNodeConfigCache();
    NodeKey node1 =
        new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
    ModifyCacheMaxSizeAction.Builder builder =
        ModifyCacheMaxSizeAction.newBuilder(
            node1, ResourceEnum.FIELD_DATA_CACHE, appContext);
    ModifyCacheMaxSizeAction modifyCacheSizeAction = builder.increase(true).build();

    appContext.updateMutedActions(ImmutableSet.of(modifyCacheSizeAction.name()));

    assertFalse(modifyCacheSizeAction.isActionable());
  }

  @Test
  public void testCacheMaxSizeNotPresent() {
    setNodeConfigCache(ResourceUtil.HEAP_MAX_SIZE, heapMaxSizeInBytes);
    NodeKey node1 =
        new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
    ModifyCacheMaxSizeAction.Builder builder =
            ModifyCacheMaxSizeAction.newBuilder(
                    node1, ResourceEnum.FIELD_DATA_CACHE, appContext);
    ModifyCacheMaxSizeAction fieldDataCacheNoAction = builder.increase(true).build();
    assertEquals(
            fieldDataCacheNoAction.getDesiredCacheMaxSizeInBytes(),
            fieldDataCacheNoAction.getCurrentCacheMaxSizeInBytes());
    assertFalse(fieldDataCacheNoAction.isActionable());
    assertEquals(300 * 1_000, fieldDataCacheNoAction.coolOffPeriodInMillis());
    assertEquals(ResourceEnum.FIELD_DATA_CACHE, fieldDataCacheNoAction.getCacheType());
    assertEquals(1, fieldDataCacheNoAction.impactedNodes().size());
    assertNoImpact(node1, fieldDataCacheNoAction);
  }

  @Test
  public void testHeapMaxSizeNotPresent() {
    setNodeConfigCache(ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE, fieldDataCacheMaxSizeInBytes);
    NodeKey node1 =
        new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
    ModifyCacheMaxSizeAction.Builder builder =
        ModifyCacheMaxSizeAction.newBuilder(
            node1, ResourceEnum.FIELD_DATA_CACHE, appContext);
    ModifyCacheMaxSizeAction fieldDataCacheNoAction = builder.increase(true).build();
    assertEquals(
        fieldDataCacheNoAction.getDesiredCacheMaxSizeInBytes(),
        fieldDataCacheNoAction.getCurrentCacheMaxSizeInBytes());
    assertFalse(fieldDataCacheNoAction.isActionable());
    assertEquals(300 * 1_000, fieldDataCacheNoAction.coolOffPeriodInMillis());
    assertEquals(ResourceEnum.FIELD_DATA_CACHE, fieldDataCacheNoAction.getCacheType());
    assertEquals(1, fieldDataCacheNoAction.impactedNodes().size());
    assertNoImpact(node1, fieldDataCacheNoAction);
  }

  private void assertNoImpact(NodeKey node, ModifyCacheMaxSizeAction modifyCacheSizeAction) {
    Map<Dimension, Impact> impact = modifyCacheSizeAction.impact().get(node).getImpact();
    assertEquals(Impact.NO_IMPACT, impact.get(HEAP));
    assertEquals(Impact.NO_IMPACT, impact.get(CPU));
    assertEquals(Impact.NO_IMPACT, impact.get(NETWORK));
    assertEquals(Impact.NO_IMPACT, impact.get(RAM));
    assertEquals(Impact.NO_IMPACT, impact.get(DISK));
  }

  private void populateNodeConfigCache() {
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

  private void setNodeConfigCache(final Resource resource, final long maxSizeInBytes) {
    appContext
        .getNodeConfigCache()
        .put(
            new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4")),
            resource,
            maxSizeInBytes);
  }
}
