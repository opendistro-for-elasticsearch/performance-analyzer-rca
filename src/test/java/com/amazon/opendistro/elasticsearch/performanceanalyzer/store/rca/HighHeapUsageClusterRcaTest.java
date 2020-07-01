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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.RcaTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources.State;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessorTestHelper;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class HighHeapUsageClusterRcaTest {

  @Test
  public void testOperate() {
    RcaTestHelper nodeRca = new RcaTestHelper();
    HighHeapUsageClusterRca clusterRca = new HighHeapUsageClusterRca(1, nodeRca);

    //setup cluster details
    try {
      ClusterDetailsEventProcessorTestHelper clusterDetailsEventProcessorTestHelper = new ClusterDetailsEventProcessorTestHelper();
      clusterDetailsEventProcessorTestHelper.addNodeDetails("node1", "127.0.0.0", false);
      clusterDetailsEventProcessorTestHelper.addNodeDetails("node2", "127.0.0.1", false);
      clusterDetailsEventProcessorTestHelper.generateClusterDetailsEvent();
    } catch (Exception e) {
      Assert.assertTrue("got exception when generating cluster details event", false);
      return;
    }

    // send three young gen flowunits (healthy, unhealthy, unhealthy) to node1
    // the cluterRca will generate three healthy flowunits
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.YOUNG_GEN_PROMOTION_RATE, "node1", State.HEALTHY));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.YOUNG_GEN_PROMOTION_RATE, "node1", State.UNHEALTHY));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.YOUNG_GEN_PROMOTION_RATE, "node1", State.UNHEALTHY));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());

    // send two young gen flowunits (unhealthy, unhealthy) to node2
    // the cluterRca will continue generating healthy flowunits
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.YOUNG_GEN_PROMOTION_RATE, "node2", State.UNHEALTHY));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.YOUNG_GEN_PROMOTION_RATE, "node2", State.UNHEALTHY));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());

    // send two old gen flowunits (unhealthy, unhealthy) to node1
    // the cluterRca will continue generating healthy flowunits
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.OLD_GEN_HEAP_USAGE, "node1", State.UNHEALTHY));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.OLD_GEN_HEAP_USAGE, "node1", State.UNHEALTHY));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());

    // send one old gen flowunits (unhealthy) to node1
    // the cluterRca will generate a unhealthy flowunit at the end
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.OLD_GEN_HEAP_USAGE, "node1", State.UNHEALTHY));
    Assert.assertTrue(clusterRca.operate().getResourceContext().isUnhealthy());

    // send one young gen flowunits (unhealthy) to node1
    // flowunit becomes healthy
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.YOUNG_GEN_PROMOTION_RATE, "node1", State.UNHEALTHY));
    Assert.assertFalse(clusterRca.operate().getResourceContext().isUnhealthy());

    // send one old gen flowunits (unhealthy) to node2
    // the cluterRca will generate a unhealthy flowunit at the end
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(ResourceUtil.YOUNG_GEN_PROMOTION_RATE, "node2", State.UNHEALTHY));
    Assert.assertTrue(clusterRca.operate().getResourceContext().isUnhealthy());
  }
}
