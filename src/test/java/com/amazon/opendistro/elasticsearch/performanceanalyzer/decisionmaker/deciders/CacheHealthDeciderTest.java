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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeRole;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.RcaTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.FieldDataCacheClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.ShardRequestCacheClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class CacheHealthDeciderTest {
  private static final JsonParser JSON_PARSER = new JsonParser();
  private AppContext appContext;
  private RcaConf rcaConf;

  @Before
  public void setupCluster() throws SQLException, ClassNotFoundException {
    final long heapMaxSizeInBytes = 12000 * 1_000_000L;
    final long fieldDataCacheMaxSizeInBytes = 12000;
    final long shardRequestCacheMaxSizeInBytes = 12000;

    ClusterDetailsEventProcessor clusterDetailsEventProcessor = new ClusterDetailsEventProcessor();
    ClusterDetailsEventProcessor.NodeDetails node1 =
        new ClusterDetailsEventProcessor.NodeDetails(NodeRole.DATA, "node1", "127.0.0.1", false);
    ClusterDetailsEventProcessor.NodeDetails node2 =
        new ClusterDetailsEventProcessor.NodeDetails(NodeRole.DATA, "node2", "127.0.0.2", false);
    ClusterDetailsEventProcessor.NodeDetails node3 =
        new ClusterDetailsEventProcessor.NodeDetails(NodeRole.DATA, "node3", "127.0.0.3", false);
    ClusterDetailsEventProcessor.NodeDetails node4 =
        new ClusterDetailsEventProcessor.NodeDetails(NodeRole.DATA, "node3", "127.0.0.4", false);
    ClusterDetailsEventProcessor.NodeDetails master =
        new ClusterDetailsEventProcessor.NodeDetails(
            NodeRole.ELECTED_MASTER, "master", "127.0.0.9", true);

    final List<ClusterDetailsEventProcessor.NodeDetails> nodes = new ArrayList<>();
    nodes.add(node1);
    nodes.add(node2);
    nodes.add(node3);
    nodes.add(node4);
    nodes.add(master);
    clusterDetailsEventProcessor.setNodesDetails(nodes);

    appContext = new AppContext();
    appContext.setClusterDetailsEventProcessor(clusterDetailsEventProcessor);

    String rcaConfPath = Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString();
    rcaConf = new RcaConf(rcaConfPath);

    for (final ClusterDetailsEventProcessor.NodeDetails node : nodes) {
      appContext
          .getNodeConfigCache()
          .put(
              new NodeKey(
                  new InstanceDetails.Id(node.getId()),
                  new InstanceDetails.Ip(node.getHostAddress())),
              ResourceUtil.HEAP_MAX_SIZE,
              heapMaxSizeInBytes);
      appContext
          .getNodeConfigCache()
          .put(
              new NodeKey(
                  new InstanceDetails.Id(node.getId()),
                  new InstanceDetails.Ip(node.getHostAddress())),
              ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE,
              fieldDataCacheMaxSizeInBytes);
      appContext
          .getNodeConfigCache()
          .put(
              new NodeKey(
                  new InstanceDetails.Id(node.getId()),
                  new InstanceDetails.Ip(node.getHostAddress())),
              ResourceUtil.SHARD_REQUEST_CACHE_MAX_SIZE,
              shardRequestCacheMaxSizeInBytes);
    }
  }

  @Test
  public void testHighEvictionRemediation() {
    RcaTestHelper<HotNodeSummary> fieldDataCacheNodeRca =
        new RcaTestHelper<>("fieldDataCacheNodeRca");
    fieldDataCacheNodeRca.setAppContext(appContext);

    // node1: Field data and Shard request cache unhealthy
    // node2: Only field data unhealthy
    // node3: Only shard request unhealthy
    // node4: all caches healthy
    fieldDataCacheNodeRca.mockFlowUnit(
        RcaTestHelper.generateFlowUnit(
            "node1",
            "127.0.0.1",
            Resources.State.UNHEALTHY,
            ResourceUtil.FIELD_DATA_CACHE_EVICTION),
        RcaTestHelper.generateFlowUnit(
            "node2",
            "127.0.0.2",
            Resources.State.UNHEALTHY,
            ResourceUtil.FIELD_DATA_CACHE_EVICTION),
        RcaTestHelper.generateFlowUnit("node3", "127.0.0.3", Resources.State.HEALTHY),
        RcaTestHelper.generateFlowUnit("node4", "127.0.0.4", Resources.State.HEALTHY));

    RcaTestHelper<HotNodeSummary> shardRequestCacheNodeRca =
        new RcaTestHelper<>("shardRequestCacheNodeRca");
    shardRequestCacheNodeRca.setAppContext(appContext);

    // node1: Field data and Shard request cache unhealthy
    // node2: Only shard request eviction unhealthy
    // node3: Only shard request hit unhealthy
    // node4: all caches healthy
    shardRequestCacheNodeRca.mockFlowUnit(
        RcaTestHelper.generateFlowUnit(
            "node1",
            "127.0.0.1",
            Resources.State.UNHEALTHY,
            ResourceUtil.SHARD_REQUEST_CACHE_EVICTION),
        RcaTestHelper.generateFlowUnit("node2", "127.0.0.2", Resources.State.HEALTHY),
        RcaTestHelper.generateFlowUnit(
            "node3",
            "127.0.0.3",
            Resources.State.UNHEALTHY,
            ResourceUtil.SHARD_REQUEST_CACHE_EVICTION),
        RcaTestHelper.generateFlowUnit("node4", "127.0.0.4", Resources.State.HEALTHY));

    FieldDataCacheClusterRca fieldDataCacheClusterRca =
        new FieldDataCacheClusterRca(1, fieldDataCacheNodeRca);
    fieldDataCacheClusterRca.setAppContext(appContext);
    fieldDataCacheClusterRca.generateFlowUnitListFromLocal(null);

    ShardRequestCacheClusterRca shardRequestCacheClusterRca =
        new ShardRequestCacheClusterRca(1, shardRequestCacheNodeRca);
    shardRequestCacheClusterRca.setAppContext(appContext);
    shardRequestCacheClusterRca.generateFlowUnitListFromLocal(null);


    RcaTestHelper<HotNodeSummary> nodeRca = new RcaTestHelper<>("QueueRejectionNodeRca");
    nodeRca.setAppContext(appContext);
    HighHeapUsageClusterRca clusterRca = new HighHeapUsageClusterRca(1, nodeRca);
    CacheHealthDecider decider =
        new CacheHealthDecider(5, 12, fieldDataCacheClusterRca, shardRequestCacheClusterRca, clusterRca);
    decider.setAppContext(appContext);
    decider.readRcaConf(rcaConf);

    // Since deciderFrequency is 12, the first 11 invocations return empty decision
    for (int i = 0; i < 11; i++) {
      Decision decision = decider.operate();
      assertTrue(decision.isEmpty());
    }

    Decision decision = decider.operate();
    // Only one resource will be tuned at a time
    assertEquals(3, decision.getActions().size());

    Map<String, Map<ResourceEnum, Integer>> nodeActionCounter = new HashMap<>();
    for (Action action : decision.getActions()) {
      assertEquals(1, action.impactedNodes().size());
      String nodeId = action.impactedNodes().get(0).getNodeId().toString();
      String summary = action.summary();
      JsonObject jsonObject = JSON_PARSER.parse(summary).getAsJsonObject();

      if (jsonObject.get("resource").getAsInt() == ResourceEnum.FIELD_DATA_CACHE.getNumber()) {
        nodeActionCounter
            .computeIfAbsent(nodeId, k -> new HashMap<>())
            .merge(ResourceEnum.FIELD_DATA_CACHE, 1, Integer::sum);
      }
      if (jsonObject.get("resource").getAsInt() == ResourceEnum.SHARD_REQUEST_CACHE.getNumber()) {
        nodeActionCounter
            .computeIfAbsent(nodeId, k -> new HashMap<>())
            .merge(ResourceEnum.SHARD_REQUEST_CACHE, 1, Integer::sum);
      }
    }

    assertEquals(1, nodeActionCounter.get("node1").size());
    // Based on priority the shard request cache gets tuned before the field data cache
    assertEquals(1, (int) nodeActionCounter.get("node1").get(ResourceEnum.SHARD_REQUEST_CACHE));
    assertEquals(1, nodeActionCounter.get("node2").size());
    assertEquals(1, (int) nodeActionCounter.get("node2").get(ResourceEnum.FIELD_DATA_CACHE));
    assertEquals(1, nodeActionCounter.get("node3").size());
    assertEquals(1, (int) nodeActionCounter.get("node3").get(ResourceEnum.SHARD_REQUEST_CACHE));
    assertFalse(nodeActionCounter.containsKey("node4"));
  }
}
