/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.store.rca;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.RcaTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources.State;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessorTestHelper;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class HighHeapUsageClusterRcaTest {

  @Test
  public void testOperate() {
    ClusterDetailsEventProcessor clusterDetailsEventProcessor = new ClusterDetailsEventProcessor();
    ClusterDetailsEventProcessor.NodeDetails node1 =
        new ClusterDetailsEventProcessor.NodeDetails(AllMetrics.NodeRole.DATA, "node1", "127.0.0.0", false);
    ClusterDetailsEventProcessor.NodeDetails node2 =
        new ClusterDetailsEventProcessor.NodeDetails(AllMetrics.NodeRole.DATA, "node2", "127.0.0.1", false);

    List<ClusterDetailsEventProcessor.NodeDetails> nodes = new ArrayList<>();
    nodes.add(node1);
    nodes.add(node2);
    clusterDetailsEventProcessor.setNodesDetails(nodes);

    AppContext appContext = new AppContext();
    appContext.setClusterDetailsEventProcessor(clusterDetailsEventProcessor);

    RcaTestHelper nodeRca = new RcaTestHelper("rcaTest");
    nodeRca.setAppContext(appContext);

    HighHeapUsageClusterRca clusterRca = new HighHeapUsageClusterRca(1, nodeRca);
    clusterRca.setAppContext(appContext);

    // send three young gen flowunits (healthy, unhealthy, unhealthy) to node1
    // the cluterRca will generate three healthy flowunits
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.YOUNG_GEN_PROMOTION_RATE, "node1", "127.0.0.0", State.HEALTHY));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.YOUNG_GEN_PROMOTION_RATE, "node1", "127.0.0.0", State.UNHEALTHY));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.YOUNG_GEN_PROMOTION_RATE, "node1", "127.0.0.0", State.UNHEALTHY));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());

    // send two young gen flowunits (unhealthy, unhealthy) to node2
    // the cluterRca will continue generating healthy flowunits
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.YOUNG_GEN_PROMOTION_RATE, "node2", "127.0.0.1", State.UNHEALTHY));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.YOUNG_GEN_PROMOTION_RATE, "node2", "127.0.0.1", State.UNHEALTHY));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());

    // send two old gen flowunits (unhealthy, unhealthy) to node1
    // the cluterRca will continue generating healthy flowunits
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.OLD_GEN_HEAP_USAGE, "node1", "127.0.0.0", State.UNHEALTHY));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.OLD_GEN_HEAP_USAGE, "node1", "127.0.0.0", State.UNHEALTHY));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());

    // send one old gen flowunits (unhealthy) to node1
    // the cluterRca will generate a unhealthy flowunit at the end
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.OLD_GEN_HEAP_USAGE, "node1", "127.0.0.0", State.UNHEALTHY));
    Assert.assertTrue(clusterRca.operate().getResourceContext().isUnhealthy());

    // send one young gen flowunits (unhealthy) to node1
    // flowunit becomes healthy
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.YOUNG_GEN_PROMOTION_RATE, "node1", "127.0.0.0", State.UNHEALTHY));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());

    // send one old gen flowunits (unhealthy) to node2
    // the cluterRca will generate a unhealthy flowunit at the end
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.YOUNG_GEN_PROMOTION_RATE, "node2", "127.0.0.1", State.UNHEALTHY));
    Assert.assertTrue(clusterRca.operate().getResourceContext().isUnhealthy());
  }
}
