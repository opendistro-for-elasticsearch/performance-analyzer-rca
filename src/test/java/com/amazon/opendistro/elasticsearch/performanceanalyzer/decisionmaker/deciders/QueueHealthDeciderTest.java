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

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil.SEARCH_QUEUE_CAPACITY;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil.WRITE_QUEUE_CAPACITY;
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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.QueueRejectionClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class QueueHealthDeciderTest {
  AppContext appContext;
  RcaConf rcaConf;

  @Before
  public void setupCluster() {
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
        new ClusterDetailsEventProcessor.NodeDetails(NodeRole.ELECTED_MASTER, "master", "127.0.0.9", true);

    List<ClusterDetailsEventProcessor.NodeDetails> nodes = new ArrayList<>();
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
  }

  @Test
  public void testHighRejectionRemediation() {
    RcaTestHelper<HotNodeSummary> nodeRca = new RcaTestHelper<>("QueueRejectionNodeRca");
    nodeRca.setAppContext(appContext);
    // node1: Both write and search queues unhealthy
    // node2: Only write unhealthy
    // node3: Only search unhealthy
    // node4: all queues healthy
    nodeRca.mockFlowUnit(
        RcaTestHelper.generateFlowUnit("node1", "127.0.0.1", Resources.State.UNHEALTHY,
            ResourceUtil.WRITE_QUEUE_REJECTION, ResourceUtil.SEARCH_QUEUE_REJECTION),
        RcaTestHelper.generateFlowUnit("node2", "127.0.0.2", Resources.State.UNHEALTHY, ResourceUtil.WRITE_QUEUE_REJECTION),
        RcaTestHelper.generateFlowUnit("node3", "127.0.0.3", Resources.State.UNHEALTHY, ResourceUtil.SEARCH_QUEUE_REJECTION),
        RcaTestHelper.generateFlowUnit("node4", "127.0.0.4", Resources.State.HEALTHY)
    );

    appContext.getNodeConfigCache().put(new NodeKey(new InstanceDetails.Id("node1"),
            new InstanceDetails.Ip("127.0.0.1")), SEARCH_QUEUE_CAPACITY,5000);
    appContext.getNodeConfigCache().put(new NodeKey(new InstanceDetails.Id("node1"),
            new InstanceDetails.Ip("127.0.0.1")), WRITE_QUEUE_CAPACITY,5000);
    appContext.getNodeConfigCache().put(new NodeKey(new InstanceDetails.Id("node2"),
            new InstanceDetails.Ip("127.0.0.2")), WRITE_QUEUE_CAPACITY,5000);
    appContext.getNodeConfigCache().put(new NodeKey(new InstanceDetails.Id("node3"),
            new InstanceDetails.Ip("127.0.0.3")), SEARCH_QUEUE_CAPACITY,5000);

    QueueRejectionClusterRca queueClusterRca = new QueueRejectionClusterRca(1, nodeRca);
    queueClusterRca.setAppContext(appContext);
    queueClusterRca.generateFlowUnitListFromLocal(null);
    QueueHealthDecider decider = new QueueHealthDecider(5, 12, queueClusterRca);
    decider.setAppContext(appContext);
    decider.readRcaConf(rcaConf);

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
      String nodeId = action.impactedNodes().get(0).getNodeId().toString();
      String summary = action.summary();
      JsonParser jsonParser = new JsonParser();
      JsonObject jsonObject = jsonParser.parse(summary).getAsJsonObject();

      if (jsonObject.get("resource").getAsInt() == ResourceEnum.WRITE_THREADPOOL.getNumber()) {
        nodeActionCounter.computeIfAbsent(nodeId, k -> new HashMap<>()).merge(ResourceEnum.WRITE_THREADPOOL, 1, Integer::sum);
      }
      if (jsonObject.get("resource").getAsInt() == ResourceEnum.SEARCH_THREADPOOL.getNumber()) {
        nodeActionCounter.computeIfAbsent(nodeId, k -> new HashMap<>()).merge(ResourceEnum.SEARCH_THREADPOOL, 1, Integer::sum);
      }
    }

    assertEquals(2, nodeActionCounter.get("node1").size());
    assertEquals(1, (int) nodeActionCounter.get("node1").get(ResourceEnum.WRITE_THREADPOOL));
    assertEquals(1, (int) nodeActionCounter.get("node1").get(ResourceEnum.SEARCH_THREADPOOL));
    assertEquals(1, nodeActionCounter.get("node2").size());
    assertEquals(1, (int) nodeActionCounter.get("node2").get(ResourceEnum.WRITE_THREADPOOL));
    assertEquals(1, nodeActionCounter.get("node3").size());
    assertEquals(1, (int) nodeActionCounter.get("node3").get(ResourceEnum.SEARCH_THREADPOOL));
    assertFalse(nodeActionCounter.containsKey("node4"));
  }
}
