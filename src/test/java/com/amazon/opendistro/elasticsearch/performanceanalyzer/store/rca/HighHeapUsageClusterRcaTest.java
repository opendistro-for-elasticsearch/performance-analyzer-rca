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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.HighHeapUsageClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
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

    // Healthy flow units should be processed and still considered healthy
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.YOUNG_GEN_PROMOTION_RATE, "node1", "127.0.0.0", State.HEALTHY));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.OLD_GEN_HEAP_USAGE, "node1", "127.0.0.0", State.HEALTHY));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());

    // A single unhealthy OLD_GEN flowunit should make the state unhealthy
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.OLD_GEN_HEAP_USAGE, "node1", "127.0.0.0", State.UNHEALTHY));
    Assert.assertTrue(clusterRca.operate().getResourceContext().isUnhealthy());

    // A single unhealthy YOUNG_GEN flowunit should make the state unhealthy
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.YOUNG_GEN_PROMOTION_RATE, "node1", "127.0.0.0", State.UNHEALTHY));
    Assert.assertTrue(clusterRca.operate().getResourceContext().isUnhealthy());

    // Healthy flow units should be processed and still considered healthy
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.YOUNG_GEN_PROMOTION_RATE, "node1", "127.0.0.0", State.HEALTHY));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.OLD_GEN_HEAP_USAGE, "node1", "127.0.0.0", State.HEALTHY));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());
  }
}
