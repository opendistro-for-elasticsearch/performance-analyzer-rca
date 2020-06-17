/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.store.rca.cluster;

import static java.time.Instant.ofEpochMilli;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.HardwareEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.JvmEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeRole;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.RcaTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.BaseClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessorTestHelper;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class BaseClusterRcaTest {
  private BaseClusterRca clusterRca;
  private RcaTestHelper<HotNodeSummary> nodeRca;
  private RcaTestHelper<HotNodeSummary> nodeRca2;
  private ResourceType type1;
  private ResourceType type2;
  private ResourceType invalidType;

  @Before
  public void setupCluster() throws SQLException, ClassNotFoundException {
    ClusterDetailsEventProcessorTestHelper clusterDetailsEventProcessorTestHelper = new ClusterDetailsEventProcessorTestHelper();
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node1", "127.0.0.0", false);
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node2", "127.0.0.1", false);
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node3", "127.0.0.2", false);
    clusterDetailsEventProcessorTestHelper.addNodeDetails("master", "127.0.0.9", NodeRole.ELECTED_MASTER, true);
    clusterDetailsEventProcessorTestHelper.generateClusterDetailsEvent();
  }

  @Before
  public void init() {
    nodeRca = new RcaTestHelper<>("RCA1");
    nodeRca2 = new RcaTestHelper<>("RCA2");
    invalidType = ResourceType.newBuilder().setJVM(JvmEnum.OLD_GEN).build();
    clusterRca = new BaseClusterRca(1, nodeRca, nodeRca2);
    type1 = ResourceType.newBuilder().setJVM(JvmEnum.OLD_GEN).build();
    type2 = ResourceType.newBuilder().setHardwareResourceType(HardwareEnum.CPU).build();
  }

  @Test
  public void testUnhealthyFlowunit() throws ClassCastException {
    ResourceFlowUnit<HotClusterSummary> flowUnit;
    nodeRca.mockFlowUnit(
        RcaTestHelper.generateFlowUnit(type1, "node1", Resources.State.UNHEALTHY),
        RcaTestHelper.generateFlowUnit(type2, "node2", Resources.State.HEALTHY)
        );

    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
    HotClusterSummary clusterSummary = flowUnit.getSummary();
    Assert.assertEquals(1, clusterSummary.getNumOfUnhealthyNodes());
    Assert.assertTrue(compareNodeSummary("node1", type1, clusterSummary.getHotNodeSummaryList().get(0)));

    nodeRca.mockFlowUnit(
        RcaTestHelper.generateFlowUnit(type1, "node1", Resources.State.HEALTHY),
        RcaTestHelper.generateFlowUnit(type2, "node2", Resources.State.HEALTHY)
    );

    flowUnit = clusterRca.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    nodeRca.mockFlowUnit(
        RcaTestHelper.generateFlowUnit(type1, "node1", Resources.State.UNHEALTHY),
        RcaTestHelper.generateFlowUnit(type2, "node2", Resources.State.UNHEALTHY)
    );

    flowUnit = clusterRca.operate();

    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
    clusterSummary = flowUnit.getSummary();
    Assert.assertEquals(2, clusterSummary.getNumOfUnhealthyNodes());
    if (compareNodeSummary("node1", type1, clusterSummary.getHotNodeSummaryList().get(0))) {
      Assert.assertTrue(compareNodeSummary("node2", type2, clusterSummary.getHotNodeSummaryList().get(1)));
    }
    else {
      Assert.assertTrue(compareNodeSummary("node1", type1, clusterSummary.getHotNodeSummaryList().get(1)));
      Assert.assertTrue(compareNodeSummary("node1", type1, clusterSummary.getHotNodeSummaryList().get(0)));
    }
  }

  @Test
  public void testMultipleRcas() throws ClassCastException {
    ResourceFlowUnit<HotClusterSummary> flowUnit;
    nodeRca.mockFlowUnit(
        RcaTestHelper.generateFlowUnit(type1, "node1", Resources.State.UNHEALTHY),
        RcaTestHelper.generateFlowUnit(type1, "node2", Resources.State.HEALTHY)
    );

    nodeRca2.mockFlowUnit(
        RcaTestHelper.generateFlowUnit(type2, "node1", Resources.State.HEALTHY),
        RcaTestHelper.generateFlowUnit(type2, "node2", Resources.State.HEALTHY)
    );

    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
    HotClusterSummary clusterSummary = flowUnit.getSummary();
    Assert.assertEquals(1, clusterSummary.getNumOfUnhealthyNodes());
    Assert.assertTrue(compareNodeSummary("node1", type1, clusterSummary.getHotNodeSummaryList().get(0)));

    nodeRca.mockFlowUnit(
        RcaTestHelper.generateFlowUnit(type1, "node1", Resources.State.UNHEALTHY),
        RcaTestHelper.generateFlowUnit(type1, "node2", Resources.State.HEALTHY)
    );

    nodeRca2.mockFlowUnit(
        RcaTestHelper.generateFlowUnit(type2, "node1", Resources.State.HEALTHY),
        RcaTestHelper.generateFlowUnit(type2, "node2", Resources.State.UNHEALTHY)
    );

    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
    clusterSummary = flowUnit.getSummary();
    Assert.assertEquals(2, clusterSummary.getNumOfUnhealthyNodes());
    Assert.assertTrue(compareNodeSummary("node1", type1, clusterSummary.getHotNodeSummaryList().get(0)));
    Assert.assertTrue(compareNodeSummary("node2", type2, clusterSummary.getHotNodeSummaryList().get(1)));

    nodeRca.mockFlowUnit(
        RcaTestHelper.generateFlowUnit(type1, "node1", Resources.State.HEALTHY),
        RcaTestHelper.generateFlowUnit(type1, "node2", Resources.State.HEALTHY)
    );

    nodeRca2.mockFlowUnit(
        RcaTestHelper.generateFlowUnit(type2, "node1", Resources.State.HEALTHY),
        RcaTestHelper.generateFlowUnit(type2, "node2", Resources.State.UNHEALTHY)
    );

    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
    clusterSummary = flowUnit.getSummary();
    Assert.assertEquals(1, clusterSummary.getNumOfUnhealthyNodes());
    Assert.assertTrue(compareNodeSummary("node2", type2, clusterSummary.getHotNodeSummaryList().get(0)));
  }

  @Test
  public void testTableEntryExpire() {
    Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());
    ResourceFlowUnit<HotClusterSummary> flowUnit;

    clusterRca.setClock(constantClock);
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(type1, "node1", Resources.State.UNHEALTHY, 0));
    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
    Assert.assertEquals(1, flowUnit.getSummary().getNumOfUnhealthyNodes());

    clusterRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(3)));
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(type2, "node2", Resources.State.UNHEALTHY,
        TimeUnit.MINUTES.toMillis(3)));
    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
    Assert.assertEquals(2, flowUnit.getSummary().getNumOfUnhealthyNodes());

    clusterRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(11)));
    nodeRca.mockFlowUnit();
    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
    Assert.assertEquals(1, flowUnit.getSummary().getNumOfUnhealthyNodes());

    clusterRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(14)));
    nodeRca.mockFlowUnit();
    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isHealthy());
  }

  @Test
  public void testCollectFromMasterNode() {
    ResourceFlowUnit<HotClusterSummary> flowUnit;
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(type1, "master", Resources.State.UNHEALTHY));
    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isHealthy());

    clusterRca.setCollectFromMasterNode(true);
    nodeRca.mockFlowUnit();
    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isHealthy());

    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(type1, "master", Resources.State.UNHEALTHY));
    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
    Assert.assertEquals(1, flowUnit.getSummary().getNumOfUnhealthyNodes());
    Assert.assertEquals(4, flowUnit.getSummary().getNumOfNodes());
    Assert.assertTrue(compareNodeSummary("master", type1, flowUnit.getSummary().getHotNodeSummaryList().get(0)));
  }

  @Test
  public void testRemoveNodeFromCluster() throws SQLException, ClassNotFoundException {
    ResourceFlowUnit<HotClusterSummary> flowUnit;
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(type1, "node1", Resources.State.UNHEALTHY));
    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());

    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(type2, "node2", Resources.State.UNHEALTHY));
    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
    Assert.assertEquals(2, flowUnit.getSummary().getNumOfUnhealthyNodes());

    removeNodeFromCluster();

    nodeRca.mockFlowUnit();
    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
    Assert.assertEquals(1, flowUnit.getSummary().getNumOfUnhealthyNodes());
    Assert.assertTrue(compareNodeSummary("node2", type2, flowUnit.getSummary().getHotNodeSummaryList().get(0)));
  }

  @Test
  public void testAddNewNodeIntoCluster() throws SQLException, ClassNotFoundException {
    ResourceFlowUnit<HotClusterSummary> flowUnit;
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(type1, "node1", Resources.State.UNHEALTHY));
    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());

    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(type2, "node4", Resources.State.UNHEALTHY));
    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
    Assert.assertEquals(1, flowUnit.getSummary().getNumOfUnhealthyNodes());
    Assert.assertTrue(compareNodeSummary("node1", type1, flowUnit.getSummary().getHotNodeSummaryList().get(0)));

    addNewNodeIntoCluster();

    nodeRca.mockFlowUnit();
    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
    Assert.assertEquals(1, flowUnit.getSummary().getNumOfUnhealthyNodes());
    Assert.assertTrue(compareNodeSummary("node1", type1, flowUnit.getSummary().getHotNodeSummaryList().get(0)));

    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(type2, "node4", Resources.State.UNHEALTHY));
    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
    HotClusterSummary clusterSummary = flowUnit.getSummary();
    Assert.assertEquals(2, clusterSummary.getNumOfUnhealthyNodes());
    Assert.assertTrue(compareNodeSummary("node1", type1, clusterSummary.getHotNodeSummaryList().get(0)));
    Assert.assertTrue(compareNodeSummary("node4", type2, clusterSummary.getHotNodeSummaryList().get(1)));
  }

   private void removeNodeFromCluster() throws SQLException, ClassNotFoundException {
    ClusterDetailsEventProcessorTestHelper clusterDetailsEventProcessorTestHelper = new ClusterDetailsEventProcessorTestHelper();
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node2", "127.0.0.1", false);
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node3", "127.0.0.2", false);
    clusterDetailsEventProcessorTestHelper.addNodeDetails("master", "127.0.0.9", NodeRole.ELECTED_MASTER, true);
    clusterDetailsEventProcessorTestHelper.generateClusterDetailsEvent();
  }

  private void addNewNodeIntoCluster() throws SQLException, ClassNotFoundException {
    ClusterDetailsEventProcessorTestHelper clusterDetailsEventProcessorTestHelper = new ClusterDetailsEventProcessorTestHelper();
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node1", "127.0.0.0", false);
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node2", "127.0.0.1", false);
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node3", "127.0.0.2", false);
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node4", "127.0.0.3", false);
    clusterDetailsEventProcessorTestHelper.addNodeDetails("master", "127.0.0.9", NodeRole.ELECTED_MASTER, true);
    clusterDetailsEventProcessorTestHelper.generateClusterDetailsEvent();
  }

  private boolean compareResourceSummary(ResourceType resourceType, HotResourceSummary resourceSummary) {
    return resourceSummary.getResourceType().equals(resourceType);
  }

  private boolean compareNodeSummary(String nodeId, ResourceType resourceType, HotNodeSummary nodeSummary) {
    if (!nodeId.equals(nodeSummary.getNodeID()) || nodeSummary.getHotResourceSummaryList().isEmpty()) {
      return false;
    }
    return compareResourceSummary(resourceType, nodeSummary.getHotResourceSummaryList().get(0));
  }
}
