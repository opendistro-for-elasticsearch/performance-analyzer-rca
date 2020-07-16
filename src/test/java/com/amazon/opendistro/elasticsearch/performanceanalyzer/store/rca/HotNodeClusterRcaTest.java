
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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.RcaTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HotNodeClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessorTestHelper;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class HotNodeClusterRcaTest {
  private AppContext appContext;

  @Before
  public void setupCluster() {
    ClusterDetailsEventProcessor clusterDetailsEventProcessor = new ClusterDetailsEventProcessor();
    ClusterDetailsEventProcessor.NodeDetails node1 =
        new ClusterDetailsEventProcessor.NodeDetails(AllMetrics.NodeRole.DATA, "node1", "127.0.0.0", false);
    ClusterDetailsEventProcessor.NodeDetails node2 =
        new ClusterDetailsEventProcessor.NodeDetails(AllMetrics.NodeRole.DATA, "node2", "127.0.0.1", false);
    ClusterDetailsEventProcessor.NodeDetails node3 =
        new ClusterDetailsEventProcessor.NodeDetails(AllMetrics.NodeRole.DATA, "node3", "127.0.0.2", false);

    List<ClusterDetailsEventProcessor.NodeDetails> nodes = new ArrayList<>();
    nodes.add(node1);
    nodes.add(node2);
    nodes.add(node3);
    clusterDetailsEventProcessor.setNodesDetails(nodes);

    appContext = new AppContext();
    appContext.setClusterDetailsEventProcessor(clusterDetailsEventProcessor);
  }

  @Test
  public void testNodeCntThresholdAndTimestampExpiration() {
    RcaTestHelper nodeRca = new RcaTestHelper();
    nodeRca.setAppContext(appContext);
    HotNodeClusterRcaX clusterRca = new HotNodeClusterRcaX(1, nodeRca);
    clusterRca.setAppContext(appContext);

    Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());
    clusterRca.setClock(constantClock);

    nodeRca.mockFlowUnit(generateFlowUnit(ResourceUtil.OLD_GEN_HEAP_USAGE, 2, "node1"));
    // did not collect enough nodes
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());

    clusterRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(6)));
    nodeRca.mockFlowUnit(generateFlowUnit(ResourceUtil.OLD_GEN_HEAP_USAGE, 8, "node2"));
    // first node expires
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());

    nodeRca.mockFlowUnit(generateFlowUnit(ResourceUtil.OLD_GEN_HEAP_USAGE, 2, "node1"));
    Assert.assertTrue(clusterRca.operate().getResourceContext().isUnhealthy());
  }

  @Test
  public void testCaptureHotNode() {
    ResourceFlowUnit fu;
    RcaTestHelper nodeRca = new RcaTestHelper();
    nodeRca.setAppContext(appContext);
    HotNodeClusterRcaX clusterRca = new HotNodeClusterRcaX(1, nodeRca);
    clusterRca.setAppContext(appContext);

    //medium = 5, below the 30% threshold
    nodeRca.mockFlowUnit(generateFlowUnit(ResourceUtil.OLD_GEN_HEAP_USAGE, 4, "node1"));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());
    nodeRca.mockFlowUnit(generateFlowUnit(ResourceUtil.OLD_GEN_HEAP_USAGE, 5, "node2"));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());
    nodeRca.mockFlowUnit(generateFlowUnit(ResourceUtil.OLD_GEN_HEAP_USAGE, 6, "node3"));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());

    // 10 is above 5*1.3
    nodeRca.mockFlowUnit(generateFlowUnit(ResourceUtil.OLD_GEN_HEAP_USAGE, 10, "node1"));
    fu = clusterRca.operate();
    Assert.assertTrue(fu.getResourceContext().isUnhealthy());
    Assert.assertTrue(fu.hasResourceSummary());
    HotClusterSummary clusterSummary = (HotClusterSummary) fu.getSummary();
    Assert.assertTrue(clusterSummary.getNumOfUnhealthyNodes() == 1);
    Assert.assertTrue(clusterSummary.getNestedSummaryList().size() > 0);

    HotNodeSummary nodeSummary = (HotNodeSummary) clusterSummary.getNestedSummaryList().get(0);
    Assert.assertTrue(nodeSummary.getNodeID().equals("node1"));
    Assert.assertTrue(nodeSummary.getNestedSummaryList().size() > 0);

    HotResourceSummary resourceSummary = (HotResourceSummary) nodeSummary.getNestedSummaryList().get(0);
    Assert.assertTrue(resourceSummary.getResource().equals(ResourceUtil.OLD_GEN_HEAP_USAGE));
    Assert.assertEquals(resourceSummary.getValue(), 10, 0.1);
  }

  @Test
  //check whether can filter out noise data if the resource usage is very small
  public void testFilterNoiseData() {
    RcaTestHelper nodeRca = new RcaTestHelper();
    nodeRca.setAppContext(appContext);
    HotNodeClusterRcaX clusterRca = new HotNodeClusterRcaX(1, nodeRca);
    clusterRca.setAppContext(appContext);

    //medium = 0.2, 0.8 is above the 30% threshold. but since the data is too small, we will drop it
    nodeRca.mockFlowUnit(generateFlowUnit(ResourceUtil.OLD_GEN_HEAP_USAGE, 0.1, "node1"));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());
    nodeRca.mockFlowUnit(generateFlowUnit(ResourceUtil.OLD_GEN_HEAP_USAGE, 0.2, "node2"));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());
    nodeRca.mockFlowUnit(generateFlowUnit(ResourceUtil.OLD_GEN_HEAP_USAGE, 0.8, "node3"));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());
  }

  private static class HotNodeClusterRcaX extends HotNodeClusterRca {
    public <R extends Rca> HotNodeClusterRcaX(final int rcaPeriod,
        final R hotNodeRca) {
      super(rcaPeriod, hotNodeRca);
    }

    public void setClock(Clock testClock) {
      this.clock = testClock;
    }
  }

  private ResourceFlowUnit generateFlowUnit(Resource type, double val, String nodeId) {
    HotResourceSummary resourceSummary = new HotResourceSummary(type,
        10, val, 60);
    HotNodeSummary nodeSummary = new HotNodeSummary(nodeId, "127.0.0.0");
    nodeSummary.appendNestedSummary(resourceSummary);
    return new ResourceFlowUnit(System.currentTimeMillis(), new ResourceContext(Resources.State.HEALTHY), nodeSummary);
  }
}
