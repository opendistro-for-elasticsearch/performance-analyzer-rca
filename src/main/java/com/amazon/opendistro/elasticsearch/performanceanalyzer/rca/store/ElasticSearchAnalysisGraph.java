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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.CPU_Utilization;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.GC_Collection_Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.GC_Collection_Time;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Max;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Used;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.IO_TotThroughput;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.IO_TotalSyscallRate;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.ShardStore;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.AggregateMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.AggregateMetric.AggregateFunction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.AvgCpuUtilByShardsMetricBasedTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.CpuUtilByShardsMetricBasedTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.capacity.TotalCpuUtilForTotalNodeMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.shardIndependent.ShardIndependentTemperatureCalculatorCpuUtilMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HotNodeClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HotNodeRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hot_node.HighCpuRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotheap.HighHeapUsageOldGenRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotheap.HighHeapUsageYoungGenRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotshard.HighCPUShardRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotshard.HotShardClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.ClusterTemperatureRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.NodeTemperatureRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.dimension.CpuUtilDimensionTemperatureRca;

import java.util.Arrays;
import java.util.Collections;

public class ElasticSearchAnalysisGraph extends AnalysisGraph {

  @Override
  public void construct() {
    Metric heapUsed = new Heap_Used(5);
    Metric gcEvent = new GC_Collection_Event(5);
    Metric heapMax = new Heap_Max(5);
    Metric gc_Collection_Time = new GC_Collection_Time(5);
    Metric cpuUtilizationGroupByOperation = new AggregateMetric(1, CPU_Utilization.NAME, AggregateFunction.SUM,
              MetricsDB.AVG, CommonDimension.OPERATION.toString());

    heapUsed.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    gcEvent.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    heapMax.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    gc_Collection_Time.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    cpuUtilizationGroupByOperation.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);

    addLeaf(heapUsed);
    addLeaf(gcEvent);
    addLeaf(heapMax);
    addLeaf(gc_Collection_Time);
    addLeaf(cpuUtilizationGroupByOperation);

    Rca<ResourceFlowUnit> highHeapUsageOldGenRca = new HighHeapUsageOldGenRca(12, heapUsed, gcEvent,
        heapMax);
    highHeapUsageOldGenRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    highHeapUsageOldGenRca.addAllUpstreams(Arrays.asList(heapUsed, gcEvent, heapMax));

    Rca<ResourceFlowUnit> highHeapUsageYoungGenRca = new HighHeapUsageYoungGenRca(12, heapUsed,
        gc_Collection_Time);
    highHeapUsageYoungGenRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    highHeapUsageYoungGenRca.addAllUpstreams(Arrays.asList(heapUsed, gc_Collection_Time));

    Rca<ResourceFlowUnit> highCpuRca = new HighCpuRca(12, cpuUtilizationGroupByOperation);
    highCpuRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    highCpuRca.addAllUpstreams(Collections.singletonList(cpuUtilizationGroupByOperation));

    Rca<ResourceFlowUnit> hotJVMNodeRca = new HotNodeRca(12, highHeapUsageOldGenRca,
        highHeapUsageYoungGenRca, highCpuRca);
    hotJVMNodeRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    hotJVMNodeRca.addAllUpstreams(Arrays.asList(highHeapUsageOldGenRca, highHeapUsageYoungGenRca, highCpuRca));

    Rca<ResourceFlowUnit> highHeapUsageClusterRca =
        new HighHeapUsageClusterRca(12, hotJVMNodeRca);
    highHeapUsageClusterRca.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    highHeapUsageClusterRca.addAllUpstreams(Collections.singletonList(hotJVMNodeRca));
    highHeapUsageClusterRca.addTag(TAG_AGGREGATE_UPSTREAM, LOCUS_DATA_NODE);

    Rca<ResourceFlowUnit> hotNodeClusterRca =
        new HotNodeClusterRca(12, hotJVMNodeRca);
    hotNodeClusterRca.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    hotNodeClusterRca.addAllUpstreams(Collections.singletonList(hotJVMNodeRca));

    constructShardResourceUsageGraph();

    // constructResourceHeatMapGraph();
  }

  private void constructShardResourceUsageGraph() {
    Metric cpuUsage = new CPU_Utilization(5);
    Metric ioTotThroughput = new IO_TotThroughput(5);
    Metric ioTotSyscallRate = new IO_TotalSyscallRate(5);

    cpuUsage.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    ioTotThroughput.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    ioTotSyscallRate.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    addLeaf(cpuUsage);
    addLeaf(ioTotThroughput);
    addLeaf(ioTotSyscallRate);

    // High CPU Usage RCA
    HighCPUShardRca highCPUShardRca = new HighCPUShardRca(5, 12, cpuUsage, ioTotThroughput, ioTotSyscallRate);
    highCPUShardRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    highCPUShardRca.addAllUpstreams(Arrays.asList(cpuUsage, ioTotThroughput, ioTotSyscallRate));

    // Hot Shard Cluster RCA which consumes the above
    HotShardClusterRca hotShardClusterRca = new HotShardClusterRca(12, highCPUShardRca);
    hotShardClusterRca.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    hotShardClusterRca.addAllUpstreams(Collections.singletonList(highCPUShardRca));
    hotShardClusterRca.addTag(TAG_AGGREGATE_UPSTREAM, LOCUS_DATA_NODE);
  }

    protected void constructResourceHeatMapGraph() {
        ShardStore shardStore = new ShardStore();

        CpuUtilByShardsMetricBasedTemperatureCalculator cpuUtilByShard =
                new CpuUtilByShardsMetricBasedTemperatureCalculator();
        AvgCpuUtilByShardsMetricBasedTemperatureCalculator avgCpuUtilByShards =
                new AvgCpuUtilByShardsMetricBasedTemperatureCalculator();
        ShardIndependentTemperatureCalculatorCpuUtilMetric shardIndependentCpuUtilMetric =
                new ShardIndependentTemperatureCalculatorCpuUtilMetric();
        TotalCpuUtilForTotalNodeMetric cpuUtilPeakUsage = new TotalCpuUtilForTotalNodeMetric();

        // heap map is developed only for data nodes.
        cpuUtilByShard.addTag(TAG_LOCUS, LOCUS_DATA_NODE);
        avgCpuUtilByShards.addTag(TAG_LOCUS, LOCUS_DATA_NODE);
        shardIndependentCpuUtilMetric.addTag(TAG_LOCUS, LOCUS_DATA_NODE);
        cpuUtilPeakUsage.addTag(TAG_LOCUS, LOCUS_DATA_NODE);

        addLeaf(cpuUtilByShard);
        addLeaf(avgCpuUtilByShards);
        addLeaf(shardIndependentCpuUtilMetric);
        addLeaf(cpuUtilPeakUsage);

        CpuUtilDimensionTemperatureRca cpuUtilHeat = new CpuUtilDimensionTemperatureRca(shardStore, cpuUtilByShard,
                avgCpuUtilByShards,
                shardIndependentCpuUtilMetric, cpuUtilPeakUsage);
        cpuUtilHeat.addTag(TAG_LOCUS, LOCUS_DATA_NODE);
        cpuUtilHeat.addAllUpstreams(Arrays.asList(cpuUtilByShard, avgCpuUtilByShards,
                shardIndependentCpuUtilMetric, cpuUtilPeakUsage));

        NodeTemperatureRca nodeTemperatureRca = new NodeTemperatureRca(cpuUtilHeat);
        nodeTemperatureRca.addTag(TAG_LOCUS, LOCUS_DATA_NODE);
        nodeTemperatureRca.addAllUpstreams(Arrays.asList(cpuUtilHeat));

        ClusterTemperatureRca clusterTemperatureRca = new ClusterTemperatureRca(nodeTemperatureRca);
        clusterTemperatureRca.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
        clusterTemperatureRca.addAllUpstreams(Arrays.asList(nodeTemperatureRca));
    }
}
