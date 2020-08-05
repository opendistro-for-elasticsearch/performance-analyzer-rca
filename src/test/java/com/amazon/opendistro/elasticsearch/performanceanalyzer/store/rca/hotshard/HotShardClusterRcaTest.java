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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.store.rca.hotshard;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.RcaTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotshard.HotShardClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class HotShardClusterRcaTest {

    private RcaTestHelper hotShardRca;

    private HotShardClusterRca hotShardClusterRca;

    private enum index {
        index_1,
        index_2
    }

    private enum shard {
        shard_1,
        shard_2,
        shard_3
    }

    private enum node {
        node_1,
        node_2,
    }

    @Before
    public void setup() {
        //setup cluster details
        try {
            hotShardRca = new RcaTestHelper<HotNodeSummary>();
            hotShardClusterRca = new HotShardClusterRca(1, hotShardRca);

            InstanceDetails instanceDetails =
                new InstanceDetails(AllMetrics.NodeRole.DATA, new InstanceDetails.Id("node1"), new InstanceDetails.Ip("127.0.0.1"), false);
            ClusterDetailsEventProcessor clusterDetailsEventProcessor = new ClusterDetailsEventProcessor();
            clusterDetailsEventProcessor.setNodesDetails(Collections.singletonList(
                new ClusterDetailsEventProcessor.NodeDetails(
                    AllMetrics.NodeRole.DATA,
                    "node1",
                    "127.0.0.1",
                    false
                )
            ));
            AppContext appContext = new AppContext();
            appContext.setClusterDetailsEventProcessor(clusterDetailsEventProcessor);

            hotShardRca.setAppContext(appContext);
            hotShardClusterRca.setAppContext(appContext);
        } catch (Exception e) {
            Assert.assertTrue("Exception when generating cluster details event", false);
            return;
        }
    }

    // 1. No Flow Units received/generated on master
    @Test
    public void testOperateForMissingFlowUnits() {
        ResourceFlowUnit flowUnit = hotShardClusterRca.operate();
        Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());
    }

    // 2. Empty Flow Units received/generated on master
    @Test
    public void testOperateForEmptyFlowUnits() {
        hotShardRca.mockFlowUnits(Collections.emptyList());

        ResourceFlowUnit flowUnit = hotShardClusterRca.operate();
        Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    }

    // 3. Healthy FlowUnits received, i.e :
    //     CPU_UTILIZATION < CPU_UTILIZATION_threshold
    // and IO_THROUGHPUT < IO_THROUGHPUT_threshold
    // and IO_SYSCALLRATE < IO_SYSCALLRATE_threshold
    @Test
    public void testOperateForHealthyFlowUnits() {
        // 3.1  Flow Units received from single node
        hotShardRca.mockFlowUnits(Arrays.asList(
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_1.name(), shard.shard_1.name(), node.node_1.name(),0.40,
                        400000, 0.40, Resources.State.HEALTHY),
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_1.name(), shard.shard_2.name(), node.node_1.name(), 0.40,
                        400000, 0.30, Resources.State.HEALTHY)));

        ResourceFlowUnit flowUnit = hotShardClusterRca.operate();
        Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());
        Assert.assertTrue(flowUnit.getSummary().getNestedSummaryList().isEmpty());


        // 3.2 FlowUnits received from both nodes
        hotShardRca.mockFlowUnits(Arrays.asList(
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_1.name(), shard.shard_1.name(), node.node_1.name(), 0.40,
                        400000, 0.40, Resources.State.HEALTHY),
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_1.name(), shard.shard_2.name(), node.node_1.name(), 0.40,
                        400000, 0.30, Resources.State.HEALTHY),
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_1.name(), shard.shard_1.name(), node.node_2.name(), 0.45,
                        400000, 0.30, Resources.State.HEALTHY)));
    }


    // 4. UnHealthy FlowUnits received, hot shard identification on single Dimension
    @Test
    public void testOperateForHotShardonSingleDimension() {
        // 4.1  No hot shard across an index
        hotShardRca.mockFlowUnits(Arrays.asList(
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_1.name(), shard.shard_1.name(), node.node_1.name(), 0.65,
                        400000, 0.40, Resources.State.UNHEALTHY),
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_1.name(), shard.shard_1.name(), node.node_2.name(), 0.40,
                        400000, 0.30, Resources.State.UNHEALTHY)));

        ResourceFlowUnit flowUnit1 = hotShardClusterRca.operate();
        Assert.assertFalse(flowUnit1.getResourceContext().isUnhealthy());
        Assert.assertTrue(flowUnit1.getSummary().getNestedSummaryList().isEmpty());

        // 4.2  hot shards across an index as per CPU Utilization, ie. : CPU_UTILIZATION >= CPU_UTILIZATION_threshold
        hotShardRca.mockFlowUnits(Arrays.asList(
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_1.name(), shard.shard_1.name(), node.node_1.name(), 0.75,
                        400000, 0.40, Resources.State.UNHEALTHY),
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_1.name(), shard.shard_2.name(), node.node_1.name(), 0.40,
                        400000, 0.30, Resources.State.UNHEALTHY),
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_1.name(), shard.shard_1.name(), node.node_2.name(), 0.10,
                        400000, 0.35, Resources.State.UNHEALTHY),
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_2.name(), shard.shard_1.name(), node.node_1.name(), 0.10,
                        400000, 0.37, Resources.State.UNHEALTHY),
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_2.name(), shard.shard_2.name(), node.node_2.name(), 0.80,
                        400000, 0.30, Resources.State.UNHEALTHY)));

        ResourceFlowUnit flowUnit2 = hotShardClusterRca.operate();
        Assert.assertTrue(flowUnit2.getResourceContext().isUnhealthy());

        Assert.assertEquals(1, flowUnit2.getSummary().getNestedSummaryList().size());
        GenericSummary nodeSummary = flowUnit2.getSummary().getNestedSummaryList().get(0);
        Assert.assertEquals(2, nodeSummary.getNestedSummaryList().size());
        List<Object> hotShard1 = nodeSummary.getNestedSummaryList().get(0).getSqlValue();
        List<Object> hotShard2 = nodeSummary.getNestedSummaryList().get(1).getSqlValue();

        // verify the resource type, cpu utilization value, node ID, Index Name, shard ID
        Assert.assertEquals(ResourceUtil.CPU_USAGE.getResourceEnumValue(), hotShard1.get(0));
        Assert.assertEquals(ResourceUtil.CPU_USAGE.getResourceEnumValue(), hotShard2.get(0));

        Assert.assertEquals(0.75, hotShard1.get(3));
        String [] nodeIndexShardInfo1 = hotShard1.get(8).toString().split(" ");
        Assert.assertEquals(node.node_1.name(), nodeIndexShardInfo1[0]);
        Assert.assertEquals(index.index_1.name(), nodeIndexShardInfo1[1]);
        Assert.assertEquals(shard.shard_1.name(), nodeIndexShardInfo1[2]);

        Assert.assertEquals(0.80, hotShard2.get(3));
        String [] nodeIndexShardInfo2 = hotShard2.get(8).toString().split(" ");
        Assert.assertEquals(node.node_2.name(), nodeIndexShardInfo2[0]);
        Assert.assertEquals(index.index_2.name(), nodeIndexShardInfo2[1]);
        Assert.assertEquals(shard.shard_2.name(), nodeIndexShardInfo2[2]);

        // 4.3  hot shards across multiple indices as per IO Total Throughput,
        // ie. : IO_TOTAL_THROUGHPUT >= IO_TOTAL_THROUGHPUT_threshold
        hotShardRca.mockFlowUnits(Arrays.asList(
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_1.name(), shard.shard_1.name(), node.node_1.name(), 0.35,
                        200000, 0.40, Resources.State.UNHEALTHY),
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_1.name(), shard.shard_2.name(), node.node_1.name(), 0.40,
                        550000, 0.30, Resources.State.UNHEALTHY),
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_1.name(), shard.shard_1.name(), node.node_2.name(), 0.30,
                        430000, 0.35, Resources.State.UNHEALTHY),
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_1.name(), shard.shard_2.name(), node.node_2.name(), 0.30,
                        400000, 0.35, Resources.State.UNHEALTHY),
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_2.name(), shard.shard_1.name(), node.node_1.name(), 0.20,
                        650000, 0.30, Resources.State.UNHEALTHY),
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_2.name(), shard.shard_2.name(), node.node_2.name(), 0.25,
                        100000, 0.30, Resources.State.UNHEALTHY)));

        ResourceFlowUnit flowUnit3 = hotShardClusterRca.operate();
        Assert.assertTrue(flowUnit3.getResourceContext().isUnhealthy());

        Assert.assertEquals(1, flowUnit3.getSummary().getNestedSummaryList().size());
        nodeSummary = flowUnit3.getSummary().getNestedSummaryList().get(0);
        Assert.assertEquals(2, nodeSummary.getNestedSummaryList().size());
        List<Object> hotShard3 = nodeSummary.getNestedSummaryList().get(0).getSqlValue();
        List<Object> hotShard4 = nodeSummary.getNestedSummaryList().get(1).getSqlValue();

        // verify the resource type, IO total throughput, node ID, Index Name, shard ID
        Assert.assertEquals(ResourceUtil.IO_TOTAL_THROUGHPUT.getResourceEnumValue(), hotShard3.get(0));
        Assert.assertEquals(ResourceUtil.IO_TOTAL_THROUGHPUT.getResourceEnumValue(), hotShard4.get(0));

        Assert.assertEquals(550000.0, hotShard3.get(3));
        String [] nodeIndexShardInfo3 = hotShard3.get(8).toString().split(" ");
        Assert.assertEquals(node.node_1.name(), nodeIndexShardInfo3[0]);
        Assert.assertEquals(index.index_1.name(), nodeIndexShardInfo3[1]);
        Assert.assertEquals(shard.shard_2.name(), nodeIndexShardInfo3[2]);

        Assert.assertEquals(650000.0, hotShard4.get(3));
        String [] nodeIndexShardInfo4 = hotShard4.get(8).toString().split(" ");
        Assert.assertEquals(node.node_1.name(), nodeIndexShardInfo4[0]);
        Assert.assertEquals(index.index_2.name(), nodeIndexShardInfo4[1]);
        Assert.assertEquals(shard.shard_1.name(), nodeIndexShardInfo4[2]);

        // 4.4  hot shards across multiple indices as per IO Total Throughput,
        // ie. : IO_TOTAL_SYS_CALLRATE >= IO_TOTAL_SYS_CALLRATE_threshold
        hotShardRca.mockFlowUnits(Arrays.asList(
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_1.name(), shard.shard_1.name(), node.node_1.name(), 0.45,
                        490000, 0.75, Resources.State.UNHEALTHY),
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_1.name(), shard.shard_2.name(), node.node_1.name(), 0.50,
                        400000, 0.25, Resources.State.UNHEALTHY),
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_1.name(), shard.shard_1.name(), node.node_2.name(), 0.47,
                        420000, 0.30, Resources.State.UNHEALTHY),
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_2.name(), shard.shard_1.name(), node.node_1.name(), 0.20,
                        350000, 0.10, Resources.State.UNHEALTHY),
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_2.name(), shard.shard_2.name(), node.node_2.name(), 0.25,
                        370000, 0.50, Resources.State.UNHEALTHY)));

        ResourceFlowUnit flowUnit4 = hotShardClusterRca.operate();
        Assert.assertTrue(flowUnit4.getResourceContext().isUnhealthy());

        Assert.assertEquals(1, flowUnit4.getSummary().getNestedSummaryList().size());
        nodeSummary = flowUnit4.getSummary().getNestedSummaryList().get(0);
        Assert.assertEquals(2, nodeSummary.getNestedSummaryList().size());
        List<Object> hotShard5 = nodeSummary.getNestedSummaryList().get(0).getSqlValue();
        List<Object> hotShard6 = nodeSummary.getNestedSummaryList().get(1).getSqlValue();

        // verify the resource type, IO total sys callrate, node ID, Index Name, shard ID
        Assert.assertEquals(ResourceUtil.IO_TOTAL_SYS_CALLRATE.getResourceEnumValue(), hotShard5.get(0));
        Assert.assertEquals(ResourceUtil.IO_TOTAL_SYS_CALLRATE.getResourceEnumValue(), hotShard6.get(0));

        Assert.assertEquals(0.75, hotShard5.get(3));
        String [] nodeIndexShardInfo5 = hotShard5.get(8).toString().split(" ");
        Assert.assertEquals(node.node_1.name(), nodeIndexShardInfo5[0]);
        Assert.assertEquals(index.index_1.name(), nodeIndexShardInfo5[1]);
        Assert.assertEquals(shard.shard_1.name(), nodeIndexShardInfo5[2]);

        Assert.assertEquals(0.50, hotShard6.get(3));
        String [] nodeIndexShardInfo6 = hotShard6.get(8).toString().split(" ");
        Assert.assertEquals(node.node_2.name(), nodeIndexShardInfo6[0]);
        Assert.assertEquals(index.index_2.name(), nodeIndexShardInfo6[1]);
        Assert.assertEquals(shard.shard_2.name(), nodeIndexShardInfo6[2]);
    }


    // 5. UnHealthy FlowUnits received, hot shard identification on multiple Dimension
    @Test
    public void testOperateForHotShardonMultipleDimension() {
        // CPU_UTILIZATION >= CPU_UTILIZATION_threshold, IO_TOTAL_SYS_CALLRATE >= IO_TOTAL_SYS_CALLRATE_threshold
        hotShardRca.mockFlowUnits(Arrays.asList(
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_1.name(), shard.shard_1.name(), node.node_1.name(), 0.75,
                        300000, 0.25, Resources.State.UNHEALTHY),
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_1.name(), shard.shard_2.name(), node.node_1.name(), 0.40,
                        560000, 0.35, Resources.State.UNHEALTHY),
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_1.name(), shard.shard_1.name(), node.node_2.name(), 0.10,
                        350000, 0.30, Resources.State.UNHEALTHY),
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_2.name(), shard.shard_1.name(), node.node_1.name(), 0.10,
                        400000, 0.10, Resources.State.UNHEALTHY),
                RcaTestHelper.generateFlowUnitForHotShard(
                        index.index_2.name(), shard.shard_2.name(), node.node_2.name(), 0.15,
                        400000, 0.50, Resources.State.UNHEALTHY)));

        ResourceFlowUnit flowUnit2 = hotShardClusterRca.operate();
        Assert.assertTrue(flowUnit2.getResourceContext().isUnhealthy());

        Assert.assertEquals(1, flowUnit2.getSummary().getNestedSummaryList().size());
        GenericSummary nodeSummary = flowUnit2.getSummary().getNestedSummaryList().get(0);
        Assert.assertEquals(3, nodeSummary.getNestedSummaryList().size());
        List<Object> hotShard1 = nodeSummary.getNestedSummaryList().get(0).getSqlValue();
        List<Object> hotShard2 = nodeSummary.getNestedSummaryList().get(1).getSqlValue();
        List<Object> hotShard3 = nodeSummary.getNestedSummaryList().get(2).getSqlValue();

        Assert.assertEquals(ResourceUtil.CPU_USAGE.getResourceEnumValue(), hotShard1.get(0));
        Assert.assertEquals(ResourceUtil.IO_TOTAL_THROUGHPUT.getResourceEnumValue(), hotShard2.get(0));
        Assert.assertEquals(ResourceUtil.IO_TOTAL_SYS_CALLRATE.getResourceEnumValue(), hotShard3.get(0));

        // verify the resource type, cpu utilization value, node ID, Index Name, shard ID
        Assert.assertEquals(0.75, hotShard1.get(3));
        String [] nodeIndexShardInfo1 = hotShard1.get(8).toString().split(" ");
        Assert.assertEquals(node.node_1.name(), nodeIndexShardInfo1[0]);
        Assert.assertEquals(index.index_1.name(), nodeIndexShardInfo1[1]);
        Assert.assertEquals(shard.shard_1.name(), nodeIndexShardInfo1[2]);

        // verify the resource type, IO total throughput, node ID, Index Name, shard ID
        Assert.assertEquals(560000.0, hotShard2.get(3));
        String [] nodeIndexShardInfo2 = hotShard2.get(8).toString().split(" ");
        Assert.assertEquals(node.node_1.name(), nodeIndexShardInfo2[0]);
        Assert.assertEquals(index.index_1.name(), nodeIndexShardInfo2[1]);
        Assert.assertEquals(shard.shard_2.name(), nodeIndexShardInfo2[2]);

        // verify the resource type, IO total sys callrate, node ID, Index Name, shard ID
        Assert.assertEquals(0.50, hotShard3.get(3));
        String [] nodeIndexShardInfo3 = hotShard3.get(8).toString().split(" ");
        Assert.assertEquals(node.node_2.name(), nodeIndexShardInfo3[0]);
        Assert.assertEquals(index.index_2.name(), nodeIndexShardInfo3[1]);
        Assert.assertEquals(shard.shard_2.name(), nodeIndexShardInfo3[2]);
    }

}