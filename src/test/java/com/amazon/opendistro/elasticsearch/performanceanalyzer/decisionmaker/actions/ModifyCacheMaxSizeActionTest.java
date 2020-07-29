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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.CacheDeciderConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.Map;
import org.junit.Test;

public class ModifyCacheMaxSizeActionTest {

    @Test
    public void testIncreaseCapacity() {
        NodeKey node1 = new NodeKey(new InstanceDetails.Id("node-1"),
                new InstanceDetails.Ip("1.2.3.4"));
        ModifyCacheMaxSizeAction modifyCacheSizeAction =
                new ModifyCacheMaxSizeAction(
                        node1,
                        ResourceEnum.FIELD_DATA_CACHE,
                        5000,
                        12000 * 1_000_000L,
                        CacheDeciderConfig.DEFAULT_FIELD_DATA_CACHE_UPPER_BOUND,
                        CacheDeciderConfig.DEFAULT_SHARD_REQUEST_CACHE_UPPER_BOUND,
                        true);
        assertTrue(
                modifyCacheSizeAction.getDesiredCacheMaxSizeInBytes()
                        > modifyCacheSizeAction.getCurrentCacheMaxSizeInBytes());
        assertTrue(modifyCacheSizeAction.isActionable());
        assertEquals(300 * 1_000, modifyCacheSizeAction.coolOffPeriodInMillis());
        assertEquals(ResourceEnum.FIELD_DATA_CACHE, modifyCacheSizeAction.getCacheType());
        assertEquals(1, modifyCacheSizeAction.impactedNodes().size());

        Map<Dimension, Impact> impact = modifyCacheSizeAction.impact().get(node1).getImpact();
        assertEquals(Impact.INCREASES_PRESSURE, impact.get(HEAP));
        assertEquals(Impact.NO_IMPACT, impact.get(CPU));
        assertEquals(Impact.NO_IMPACT, impact.get(NETWORK));
        assertEquals(Impact.NO_IMPACT, impact.get(RAM));
        assertEquals(Impact.NO_IMPACT, impact.get(DISK));
    }

    @Test
    public void testNoIncreaseCapacity() {
        NodeKey node1 = new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
        ModifyCacheMaxSizeAction modifyCacheSizeAction =
                new ModifyCacheMaxSizeAction(
                        node1,
                        ResourceEnum.FIELD_DATA_CACHE,
                        5000,
                        12000 * 1_000_000L,
                        CacheDeciderConfig.DEFAULT_FIELD_DATA_CACHE_UPPER_BOUND,
                        CacheDeciderConfig.DEFAULT_SHARD_REQUEST_CACHE_UPPER_BOUND,
                        false);
        assertEquals(
                modifyCacheSizeAction.getDesiredCacheMaxSizeInBytes(),
                modifyCacheSizeAction.getCurrentCacheMaxSizeInBytes());
        assertFalse(modifyCacheSizeAction.isActionable());
        assertEquals(300 * 1_000, modifyCacheSizeAction.coolOffPeriodInMillis());
        assertEquals(ResourceEnum.FIELD_DATA_CACHE, modifyCacheSizeAction.getCacheType());
        assertEquals(1, modifyCacheSizeAction.impactedNodes().size());

        Map<Dimension, Impact> impact = modifyCacheSizeAction.impact().get(node1).getImpact();
        assertEquals(Impact.NO_IMPACT, impact.get(HEAP));
        assertEquals(Impact.NO_IMPACT, impact.get(CPU));
        assertEquals(Impact.NO_IMPACT, impact.get(NETWORK));
        assertEquals(Impact.NO_IMPACT, impact.get(RAM));
        assertEquals(Impact.NO_IMPACT, impact.get(DISK));
    }

    @Test
    public void testBounds() {
        // TODO: Move to work with test rcaConf when bounds moved to nodeConfiguration rca
        final long maxSizeInBytes = 12000 * 1_000_000L;
        NodeKey node1 = new NodeKey(new InstanceDetails.Id("node-1"), new InstanceDetails.Ip("1.2.3.4"));
        ModifyCacheMaxSizeAction fieldCacheIncrease =
                new ModifyCacheMaxSizeAction(
                        node1,
                        ResourceEnum.FIELD_DATA_CACHE,
                        (long) (maxSizeInBytes * CacheDeciderConfig.DEFAULT_FIELD_DATA_CACHE_UPPER_BOUND),
                        maxSizeInBytes,
                        CacheDeciderConfig.DEFAULT_FIELD_DATA_CACHE_UPPER_BOUND,
                        CacheDeciderConfig.DEFAULT_SHARD_REQUEST_CACHE_UPPER_BOUND,
                        true);
        assertEquals(
                fieldCacheIncrease.getDesiredCacheMaxSizeInBytes(),
                fieldCacheIncrease.getCurrentCacheMaxSizeInBytes());
        assertFalse(fieldCacheIncrease.isActionable());
        assertNoImpact(node1, fieldCacheIncrease);

        ModifyCacheMaxSizeAction shardRequestCacheIncrease =
                new ModifyCacheMaxSizeAction(
                        node1,
                        ResourceEnum.SHARD_REQUEST_CACHE,
                        (long) (maxSizeInBytes * CacheDeciderConfig.DEFAULT_SHARD_REQUEST_CACHE_UPPER_BOUND),
                        maxSizeInBytes,
                        CacheDeciderConfig.DEFAULT_FIELD_DATA_CACHE_UPPER_BOUND,
                        CacheDeciderConfig.DEFAULT_SHARD_REQUEST_CACHE_UPPER_BOUND,
                        true);
        assertEquals(
                shardRequestCacheIncrease.getDesiredCacheMaxSizeInBytes(),
                shardRequestCacheIncrease.getCurrentCacheMaxSizeInBytes());
        assertFalse(shardRequestCacheIncrease.isActionable());
        assertNoImpact(node1, shardRequestCacheIncrease);
    }

    private void assertNoImpact(NodeKey node, ModifyCacheMaxSizeAction modifyCacheSizeAction) {
        Map<Dimension, Impact> impact = modifyCacheSizeAction.impact().get(node).getImpact();
        assertEquals(Impact.NO_IMPACT, impact.get(HEAP));
        assertEquals(Impact.NO_IMPACT, impact.get(CPU));
        assertEquals(Impact.NO_IMPACT, impact.get(NETWORK));
        assertEquals(Impact.NO_IMPACT, impact.get(RAM));
        assertEquals(Impact.NO_IMPACT, impact.get(DISK));
    }
}