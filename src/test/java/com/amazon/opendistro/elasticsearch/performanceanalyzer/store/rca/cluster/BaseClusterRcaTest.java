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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ThreadPoolEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.RcaTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.BaseClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessorTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.RcaTestUtil;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class BaseClusterRcaTest {
  private BaseClusterRca clusterRca;
  private RcaTestHelper nodeRca;
  private ResourceType type1;
  private ResourceType type2;
  private ResourceType invalidType;

  @Before
  public void setupCluster() throws SQLException, ClassNotFoundException {
    ClusterDetailsEventProcessorTestHelper clusterDetailsEventProcessorTestHelper = new ClusterDetailsEventProcessorTestHelper();
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node1", "127.0.0.0", false);
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node2", "127.0.0.1", false);
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node3", "127.0.0.2", false);
    clusterDetailsEventProcessorTestHelper.generateClusterDetailsEvent();
  }

  @Before
  public void init() {
    nodeRca = new RcaTestHelper();
    type1 = ResourceType.newBuilder().setThreadpool(ThreadPoolEnum.THREADPOOL_REJECTED_REQS).build();
    type2 = ResourceType.newBuilder().setHardwareResourceType(HardwareEnum.CPU).build();
    invalidType = ResourceType.newBuilder().setJVM(JvmEnum.OLD_GEN).build();
    clusterRca = new BaseClusterRca(1, nodeRca, Arrays.asList(
        type1,
        type2
    ));
  }

  @Test
  public void testUnhealthyFlowunit() throws ClassCastException {
    ResourceFlowUnit flowUnit;
    nodeRca.mockFlowUnit(
        RcaTestHelper.generateFlowUnit(type1, "node1", Resources.State.UNHEALTHY),
        RcaTestHelper.generateFlowUnit(type2, "node2", Resources.State.HEALTHY)
        );

    flowUnit = clusterRca.operate();
    Assert.assertTrue(!flowUnit.getResourceContext().isUnhealthy());

    nodeRca.mockFlowUnit(
        RcaTestHelper.generateFlowUnit(type1, "node1", Resources.State.UNHEALTHY),
        RcaTestHelper.generateFlowUnit(type2, "node2", Resources.State.UNHEALTHY)
    );

    flowUnit = clusterRca.operate();
    Assert.assertTrue(!flowUnit.getResourceContext().isUnhealthy());

    nodeRca.mockFlowUnit(
        RcaTestHelper.generateFlowUnit(type1, "node1", Resources.State.UNHEALTHY),
        RcaTestHelper.generateFlowUnit(type2, "node2", Resources.State.UNHEALTHY)
    );

    flowUnit = clusterRca.operate();

    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
    HotClusterSummary clusterSummary = (HotClusterSummary) flowUnit.getResourceSummary();
    Assert.assertEquals(1, clusterSummary.getNumOfUnhealthyNodes());
    Assert.assertTrue(RcaTestUtil.checkNodeSummary("node1", type1, clusterSummary.getNestedSummaryList().get(0)));

    nodeRca.mockFlowUnit(
        RcaTestHelper.generateFlowUnit(type1, "node1", Resources.State.UNHEALTHY),
        RcaTestHelper.generateFlowUnit(type2, "node2", Resources.State.UNHEALTHY)
    );

    flowUnit = clusterRca.operate();

    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
    clusterSummary = (HotClusterSummary) flowUnit.getResourceSummary();
    Assert.assertEquals(2, clusterSummary.getNumOfUnhealthyNodes());
    boolean node1Check = RcaTestUtil.checkNodeSummary("node1", type1, clusterSummary.getNestedSummaryList().get(0));
    if (node1Check) {
      Assert.assertTrue(RcaTestUtil.checkNodeSummary("node2", type2, clusterSummary.getNestedSummaryList().get(1)));
    }
    else {
      Assert.assertTrue(RcaTestUtil.checkNodeSummary("node1", type1, clusterSummary.getNestedSummaryList().get(1)));
      Assert.assertTrue(RcaTestUtil.checkNodeSummary("node2", type2, clusterSummary.getNestedSummaryList().get(0)));
    }

    nodeRca.mockFlowUnit(
        RcaTestHelper.generateFlowUnit(type1, "node1", Resources.State.HEALTHY),
        RcaTestHelper.generateFlowUnit(type2, "node2", Resources.State.HEALTHY)
    );
    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isHealthy());
  }

  @Test
  public void testTableEntryExpire() {
    Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());
    ResourceFlowUnit flowUnit;

    clusterRca.setClock(constantClock);
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(type1, "node1", Resources.State.UNHEALTHY));
    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isHealthy());

    clusterRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(1)));
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(type1, "node1", Resources.State.UNHEALTHY));
    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isHealthy());

    clusterRca.setClock(Clock.offset(constantClock, Duration.ofMinutes(6)));
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(type1, "node1", Resources.State.UNHEALTHY));
    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isHealthy());
  }

  @Test
  public void testNewNodeJoinCluster() throws SQLException, ClassNotFoundException {
    ResourceFlowUnit flowUnit;
    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(type1, "node1", Resources.State.UNHEALTHY));
    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isHealthy());

    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(type1, "node1", Resources.State.UNHEALTHY));
    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isHealthy());

    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(type1, "node1", Resources.State.UNHEALTHY));
    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());

    setupNewCluster();

    nodeRca.mockFlowUnit(RcaTestHelper.generateFlowUnit(type1, "node1", Resources.State.UNHEALTHY));
    flowUnit = clusterRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isHealthy());
  }

   private void setupNewCluster() throws SQLException, ClassNotFoundException {
    ClusterDetailsEventProcessorTestHelper clusterDetailsEventProcessorTestHelper = new ClusterDetailsEventProcessorTestHelper();
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node4", "127.0.0.4", false);
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node2", "127.0.0.1", false);
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node3", "127.0.0.2", false);
    clusterDetailsEventProcessorTestHelper.generateClusterDetailsEvent();
  }
}
