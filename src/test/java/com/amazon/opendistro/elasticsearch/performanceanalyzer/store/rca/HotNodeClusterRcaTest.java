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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.store.rca;

import static java.time.Instant.ofEpochMilli;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.JvmEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.DummyTestHelperRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HotNodeClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessorTestHelper;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class HotNodeClusterRcaTest {


  @Before
  public void setupCluster() throws SQLException, ClassNotFoundException {
    ClusterDetailsEventProcessorTestHelper clusterDetailsEventProcessorTestHelper = new ClusterDetailsEventProcessorTestHelper();
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node1", "127.0.0.0", false);
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node2", "127.0.0.1", false);
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node3", "127.0.0.2", false);
    clusterDetailsEventProcessorTestHelper.generateClusterDetailsEvent();
  }

  @Test
  public void testNodeCntThresholdAndTimestampExpiration() {
    DummyTestHelperRca<HotNodeSummary> nodeRca = new DummyTestHelperRca<>();
    HotNodeClusterRca clusterRca = new HotNodeClusterRca(1, nodeRca);

    Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());
    clusterRca.setClock(constantClock);

    nodeRca.mockFlowUnit(generateFlowUnit(buildResourceType(JvmEnum.OLD_GEN), 2, "node1"));
    // did not collect enough nodes
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());

    clusterRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(6)));
    nodeRca.mockFlowUnit(generateFlowUnit(buildResourceType(JvmEnum.OLD_GEN), 8, "node2"));
    // first node expires
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());

    nodeRca.mockFlowUnit(generateFlowUnit(buildResourceType(JvmEnum.OLD_GEN), 2, "node1"));
    Assert.assertTrue(clusterRca.operate().getResourceContext().isUnhealthy());
  }

  @Test
  public void testCaptureHotNode() {
    ResourceFlowUnit<HotClusterSummary> fu;
    DummyTestHelperRca<HotNodeSummary> nodeRca = new DummyTestHelperRca();
    HotNodeClusterRca clusterRca = new HotNodeClusterRca(1, nodeRca);

    //medium = 5, below the 30% threshold
    nodeRca.mockFlowUnit(generateFlowUnit(buildResourceType(JvmEnum.OLD_GEN), 4, "node1"));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());
    nodeRca.mockFlowUnit(generateFlowUnit(buildResourceType(JvmEnum.OLD_GEN), 5, "node2"));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());
    nodeRca.mockFlowUnit(generateFlowUnit(buildResourceType(JvmEnum.OLD_GEN), 6, "node3"));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());

    // 10 is above 5*1.3
    nodeRca.mockFlowUnit(generateFlowUnit(buildResourceType(JvmEnum.OLD_GEN), 10, "node1"));
    fu = clusterRca.operate();
    Assert.assertTrue(fu.getResourceContext().isUnhealthy());
    Assert.assertTrue(fu.hasSummary());
    HotClusterSummary clusterSummary = fu.getSummary();
    Assert.assertTrue(clusterSummary.getNumOfUnhealthyNodes() == 1);
    Assert.assertTrue(clusterSummary.getNestedSummaryList().size() > 0);

    HotNodeSummary nodeSummary = clusterSummary.getNodeSummaryList().get(0);
    Assert.assertTrue(nodeSummary.getNodeID().equals("node1"));
    Assert.assertTrue(nodeSummary.getNestedSummaryList().size() > 0);

    HotResourceSummary resourceSummary =  nodeSummary.getHotResourceSummaryList().get(0);
    Assert.assertTrue(resourceSummary.getResourceType().equals(buildResourceType(JvmEnum.OLD_GEN)));
    Assert.assertEquals(resourceSummary.getValue(), 10, 0.1);
  }

  @Test
  //check whether can filter out noise data if the resource usage is very small
  public void testFilterNoiseData() {
    DummyTestHelperRca<HotNodeSummary> nodeRca = new DummyTestHelperRca<>();
    HotNodeClusterRca clusterRca = new HotNodeClusterRca(1, nodeRca);

    //medium = 0.2, 0.8 is above the 30% threshold. but since the data is too small, we will drop it
    nodeRca.mockFlowUnit(generateFlowUnit(buildResourceType(JvmEnum.OLD_GEN), 0.1, "node1"));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());
    nodeRca.mockFlowUnit(generateFlowUnit(buildResourceType(JvmEnum.OLD_GEN), 0.2, "node2"));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());
    nodeRca.mockFlowUnit(generateFlowUnit(buildResourceType(JvmEnum.OLD_GEN), 0.8, "node3"));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());
  }

  private ResourceFlowUnit<HotNodeSummary> generateFlowUnit(ResourceType type, double val, String nodeId) {
    HotResourceSummary resourceSummary = new HotResourceSummary(type,
        10, val, 60);
    HotNodeSummary nodeSummary = new HotNodeSummary(nodeId, "127.0.0.0");
    nodeSummary.appendNestedSummary(resourceSummary);
    return new ResourceFlowUnit<>(System.currentTimeMillis(), new ResourceContext(Resources.State.HEALTHY), nodeSummary);
  }

  private ResourceType buildResourceType(JvmEnum jvmEnum) {
    return ResourceType.newBuilder().setJVM(jvmEnum).build();
  }
}
