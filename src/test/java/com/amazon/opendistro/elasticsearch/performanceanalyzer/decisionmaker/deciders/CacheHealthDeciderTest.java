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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeRole;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.RcaTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.BaseClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.FieldDataCacheClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.QueueRejectionClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.ShardRequestCacheClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessorTestHelper;
import com.google.common.collect.ImmutableList;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class CacheHealthDeciderTest {

  @Before
  public void setupCluster() throws SQLException, ClassNotFoundException {
    ClusterDetailsEventProcessorTestHelper clusterDetailsEventProcessorTestHelper = new ClusterDetailsEventProcessorTestHelper();
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node1", "127.0.0.1", false);
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node2", "127.0.0.2", false);
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node3", "127.0.0.3", false);
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node4", "127.0.0.4", false);
    clusterDetailsEventProcessorTestHelper.addNodeDetails("master", "127.0.0.9", NodeRole.ELECTED_MASTER, true);
    clusterDetailsEventProcessorTestHelper.generateClusterDetailsEvent();
  }

  @Test
  public void testHighEvictionRemediation() {
    RcaTestHelper<HotNodeSummary> fieldDataCacheNodeRca = new RcaTestHelper<>("fieldDataCacheNodeRca");
    // node1: Field data and Shard request cache unhealthy
    // node2: Only field data unhealthy
    // node3: Only shard request unhealthy
    // node4: all caches healthy
    fieldDataCacheNodeRca.mockFlowUnit(
        RcaTestHelper.generateFlowUnit("node1", "127.0.0.1", Resources.State.UNHEALTHY, ResourceUtil.FIELD_DATA_CACHE_EVICTION),
        RcaTestHelper.generateFlowUnit("node2", "127.0.0.2", Resources.State.UNHEALTHY, ResourceUtil.FIELD_DATA_CACHE_EVICTION),
        RcaTestHelper.generateFlowUnit("node3", "127.0.0.3", Resources.State.HEALTHY),
        RcaTestHelper.generateFlowUnit("node4", "127.0.0.4", Resources.State.HEALTHY)
    );

    RcaTestHelper<HotNodeSummary> shardRequestCacheNodeRca = new RcaTestHelper<>("shardRequestCacheNodeRca");
    // node1: Field data and Shard request cache unhealthy
    // node2: Only shard request eviction unhealthy
    // node3: Only shard request hit unhealthy
    // node4: all caches healthy
    shardRequestCacheNodeRca.mockFlowUnit(
            RcaTestHelper.generateFlowUnit("node1", "127.0.0.1", Resources.State.UNHEALTHY, ResourceUtil.SHARD_REQUEST_CACHE_EVICTION),
            RcaTestHelper.generateFlowUnit("node2", "127.0.0.2", Resources.State.HEALTHY),
            RcaTestHelper.generateFlowUnit("node3", "127.0.0.3", Resources.State.UNHEALTHY, ResourceUtil.SHARD_REQUEST_CACHE_EVICTION),
            RcaTestHelper.generateFlowUnit("node4", "127.0.0.4", Resources.State.HEALTHY)
    );

    FieldDataCacheClusterRca fieldDataCacheClusterRca = new FieldDataCacheClusterRca(1, fieldDataCacheNodeRca);
    fieldDataCacheClusterRca.generateFlowUnitListFromLocal(null);

    ShardRequestCacheClusterRca shardRequestCacheClusterRca = new ShardRequestCacheClusterRca(1, shardRequestCacheNodeRca);
    shardRequestCacheClusterRca.generateFlowUnitListFromLocal(null);

    CacheHealthDecider decider =
        new CacheHealthDecider(
            5,
            12,
            ImmutableList.<BaseClusterRca>builder()
                .add(fieldDataCacheClusterRca)
                .add(shardRequestCacheClusterRca)
                .build());

    // Since deciderFrequency is 12, the first 11 invocations return empty decision
    for (int i = 0; i < 11; i++) {
      Decision decision = decider.operate();
      assertTrue(decision.isEmpty());
    }

    Decision decision = decider.operate();
    assertEquals(4, decision.getActions().size());

    Map<String, Map<ResourceEnum, Integer>> nodeActionCounter = new HashMap<>();
    for (Action action: decision.getActions()) {
      assertEquals(1, action.impactedNodes().size());
      String nodeId = action.impactedNodes().get(0).getNodeId();
      String summary = action.summary();
      if (summary.contains(ResourceEnum.FIELD_DATA_CACHE.toString())) {
        nodeActionCounter.computeIfAbsent(nodeId, k -> new HashMap<>()).merge(ResourceEnum.FIELD_DATA_CACHE, 1, Integer::sum);
      }
      if (summary.contains(ResourceEnum.SHARD_REQUEST_CACHE.toString())) {
        nodeActionCounter.computeIfAbsent(nodeId, k -> new HashMap<>()).merge(ResourceEnum.SHARD_REQUEST_CACHE, 1, Integer::sum);
      }
    }

    assertEquals(2, nodeActionCounter.get("node1").size());
    assertEquals(1, (int) nodeActionCounter.get("node1").get(ResourceEnum.FIELD_DATA_CACHE));
    assertEquals(1, (int) nodeActionCounter.get("node1").get(ResourceEnum.SHARD_REQUEST_CACHE));
    assertEquals(1, nodeActionCounter.get("node2").size());
    assertEquals(1, (int) nodeActionCounter.get("node2").get(ResourceEnum.FIELD_DATA_CACHE));
    assertEquals(1, nodeActionCounter.get("node3").size());
    assertEquals(1, (int) nodeActionCounter.get("node3").get(ResourceEnum.SHARD_REQUEST_CACHE));
    assertFalse(nodeActionCounter.containsKey("node4"));
  }
}
