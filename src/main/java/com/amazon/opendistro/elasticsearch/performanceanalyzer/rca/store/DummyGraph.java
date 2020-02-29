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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.LOCUS_DATA_MASTER_NODE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.LOCUS_DATA_NODE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.LOCUS_MASTER_NODE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.TAG_AGGREGATE_UPSTREAM;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.TAG_LOCUS;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.GC_Collection_Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.GC_Collection_Time;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Max;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Used;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.ShardStore;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.AvgCpuUtilByShards;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.CpuUtilByShard;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.capacity.NodeLevelUsageForCpu;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.shardIndependent.CpuUtilShardIndependent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HotNodeRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotheap.HighHeapUsageOldGenRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotheap.HighHeapUsageYoungGenRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.ClusterHeatRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.CpuUtilHeatRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.NodeHeatRca;
import java.util.Arrays;
import java.util.Collections;

public class DummyGraph extends AnalysisGraph {

    @Override
    public void construct() {
        Metric heapUsed = new Heap_Used(5);
        Metric gcEvent = new GC_Collection_Event(5);
        Metric heapMax = new Heap_Max(5);
        Metric gc_Collection_Time = new GC_Collection_Time(5);

        heapUsed.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
        gcEvent.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
        heapMax.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
        gc_Collection_Time.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
        addLeaf(heapUsed);
        addLeaf(gcEvent);
        addLeaf(heapMax);
        addLeaf(gc_Collection_Time);

        Rca<ResourceFlowUnit> highHeapUsageOldGenRca = new HighHeapUsageOldGenRca(12, heapUsed, gcEvent,
                heapMax);
        highHeapUsageOldGenRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
        highHeapUsageOldGenRca.addAllUpstreams(Arrays.asList(heapUsed, gcEvent, heapMax));

        Rca<ResourceFlowUnit> highHeapUsageYoungGenRca = new HighHeapUsageYoungGenRca(12, heapUsed,
                gc_Collection_Time);
        highHeapUsageYoungGenRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
        highHeapUsageYoungGenRca.addAllUpstreams(Arrays.asList(heapUsed, gc_Collection_Time));

        Rca<ResourceFlowUnit> hotJVMNodeRca = new HotNodeRca(12, highHeapUsageOldGenRca,
                highHeapUsageYoungGenRca);
        hotJVMNodeRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
        hotJVMNodeRca.addAllUpstreams(Arrays.asList(highHeapUsageOldGenRca, highHeapUsageYoungGenRca));

        Rca<ResourceFlowUnit> highHeapUsageClusterRca =
                new HighHeapUsageClusterRca(12, hotJVMNodeRca);
        highHeapUsageClusterRca.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
        highHeapUsageClusterRca.addAllUpstreams(Collections.singletonList(hotJVMNodeRca));
        highHeapUsageClusterRca.addTag(TAG_AGGREGATE_UPSTREAM, LOCUS_DATA_NODE);

        constructResourceHeatMapGraph();
    }

    protected void constructResourceHeatMapGraph() {
        ShardStore shardStore = new ShardStore();

        CpuUtilByShard cpuUtilByShard = new CpuUtilByShard();
        AvgCpuUtilByShards avgCpuUtilByShards = new AvgCpuUtilByShards();
        CpuUtilShardIndependent cpuUtilShardIndependent = new CpuUtilShardIndependent();
        NodeLevelUsageForCpu cpuUtilPeakUsage = new NodeLevelUsageForCpu();

        // heap map is developed only for data nodes.
        cpuUtilByShard.addTag(TAG_LOCUS, LOCUS_DATA_NODE);
        avgCpuUtilByShards.addTag(TAG_LOCUS, LOCUS_DATA_NODE);
        cpuUtilShardIndependent.addTag(TAG_LOCUS, LOCUS_DATA_NODE);
        cpuUtilPeakUsage.addTag(TAG_LOCUS, LOCUS_DATA_NODE);

        addLeaf(cpuUtilByShard);
        addLeaf(avgCpuUtilByShards);
        addLeaf(cpuUtilShardIndependent);
        addLeaf(cpuUtilPeakUsage);

        CpuUtilHeatRca cpuUtilHeat = new CpuUtilHeatRca(shardStore, cpuUtilByShard,
                avgCpuUtilByShards,
                cpuUtilShardIndependent, cpuUtilPeakUsage);
        cpuUtilHeat.addTag(TAG_LOCUS, LOCUS_DATA_NODE);
        cpuUtilHeat.addAllUpstreams(Arrays.asList(cpuUtilByShard, avgCpuUtilByShards,
                cpuUtilShardIndependent, cpuUtilPeakUsage));

        NodeHeatRca nodeHeatRca = new NodeHeatRca(cpuUtilHeat);
        nodeHeatRca.addTag(TAG_LOCUS, LOCUS_DATA_NODE);
        nodeHeatRca.addAllUpstreams(Arrays.asList(cpuUtilHeat));

        ClusterHeatRca clusterHeatRca = new ClusterHeatRca(nodeHeatRca);
        clusterHeatRca.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
        clusterHeatRca.addAllUpstreams(Arrays.asList(nodeHeatRca));
    }
}
