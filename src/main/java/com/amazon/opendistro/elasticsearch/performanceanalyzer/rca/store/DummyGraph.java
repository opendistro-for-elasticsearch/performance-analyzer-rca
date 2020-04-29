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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Bitset_Memory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.CPU_Utilization;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_FieldData_Size;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_Query_Size;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_Request_Size;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.DocValues_Memory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.GC_Collection_Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.GC_Collection_Time;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Max;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Used;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.IndexWriter_Memory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Norms_Memory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Points_Memory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Segments_Memory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.StoredFields_Memory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.TermVectors_Memory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Terms_Memory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.VersionMap_Memory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.ShardStore;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.AggregateMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.AggregateMetric.AggregateFunction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.AvgCpuUtilByShardsMetricBasedTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.CpuUtilByShardsMetricBasedTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.HeapAllocRateByShardAvgTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.HeapAllocRateByShardTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.capacity.HeapAllocRateTotalTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.capacity.TotalCpuUtilForTotalNodeMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.shardIndependent.HeapAllocRateShardIndependentTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.shardIndependent.ShardIndependentTemperatureCalculatorCpuUtilMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HotNodeClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HotNodeRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hot_node.HighCpuRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotheap.HighHeapUsageOldGenRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotheap.HighHeapUsageYoungGenRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.ClusterTemperatureRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.NodeTemperatureRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.dimension.CpuUtilDimensionTemperatureRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.dimension.HeapAllocRateTemperatureRca;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DummyGraph extends AnalysisGraph {

  private static final Logger LOG = LogManager.getLogger(DummyGraph.class);

  @Override
  public void construct() {
    Metric heapUsed = new Heap_Used(5);
    Metric gcEvent = new GC_Collection_Event(5);
    Metric heapMax = new Heap_Max(5);
    Metric gc_Collection_Time = new GC_Collection_Time(5);
    Metric cpuUtilizationGroupByOperation = new AggregateMetric(1, CPU_Utilization.NAME,
        AggregateFunction.SUM,
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

    //add node stats metrics
    List<Metric> nodeStatsMetrics = constructNodeStatsMetrics();

    Rca<ResourceFlowUnit> highHeapUsageOldGenRca = new HighHeapUsageOldGenRca(12, heapUsed, gcEvent,
        heapMax, nodeStatsMetrics);
    highHeapUsageOldGenRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    List<Node<?>> upstream = new ArrayList<>(Arrays.asList(heapUsed, gcEvent, heapMax));
    upstream.addAll(nodeStatsMetrics);
    highHeapUsageOldGenRca.addAllUpstreams(upstream);

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
    hotJVMNodeRca.addAllUpstreams(
        Arrays.asList(highHeapUsageOldGenRca, highHeapUsageYoungGenRca, highCpuRca));

    Rca<ResourceFlowUnit> highHeapUsageClusterRca =
        new HighHeapUsageClusterRca(12, hotJVMNodeRca);
    highHeapUsageClusterRca.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    highHeapUsageClusterRca.addAllUpstreams(Collections.singletonList(hotJVMNodeRca));
    highHeapUsageClusterRca.addTag(TAG_AGGREGATE_UPSTREAM, LOCUS_DATA_NODE);

    Rca<ResourceFlowUnit> hotNodeClusterRca =
        new HotNodeClusterRca(12, hotJVMNodeRca);
    hotNodeClusterRca.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    hotNodeClusterRca.addAllUpstreams(Collections.singletonList(hotJVMNodeRca));

    // constructResourceHeatMapGraph();
  }

  private List<Metric> constructNodeStatsMetrics() {
    List<Metric> nodeStatsMetrics = new ArrayList<Metric>() {{
      add(new Cache_FieldData_Size(5));
      add(new Cache_Request_Size(5));
      add(new Cache_Query_Size(5));
      add(new Segments_Memory(5));
      add(new Terms_Memory(5));
      add(new StoredFields_Memory(5));
      add(new TermVectors_Memory(5));
      add(new Norms_Memory(5));
      add(new Points_Memory(5));
      add(new DocValues_Memory(5));
      add(new IndexWriter_Memory(5));
      add(new Bitset_Memory(5));
      add(new VersionMap_Memory(5));
    }};
    for (Metric metric : nodeStatsMetrics) {
      metric.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
      addLeaf(metric);
    }
    return nodeStatsMetrics;
  }

  protected void constructResourceHeatMapGraph() {
    LOG.info("Constructing temperature profile RCA components");
    ShardStore shardStore = new ShardStore();

    HeapAllocRateByShardTemperatureCalculator heapAllocByShard =
        new HeapAllocRateByShardTemperatureCalculator();
    HeapAllocRateByShardAvgTemperatureCalculator heapAllocRateByShardAvg =
        new HeapAllocRateByShardAvgTemperatureCalculator();
    HeapAllocRateShardIndependentTemperatureCalculator shardIndependentHeapAllocRate =
        new HeapAllocRateShardIndependentTemperatureCalculator();
    HeapAllocRateTotalTemperatureCalculator heapAllocRateTotal =
        new HeapAllocRateTotalTemperatureCalculator();

    CpuUtilByShardsMetricBasedTemperatureCalculator cpuUtilByShard =
        new CpuUtilByShardsMetricBasedTemperatureCalculator();
    AvgCpuUtilByShardsMetricBasedTemperatureCalculator avgCpuUtilByShards =
        new AvgCpuUtilByShardsMetricBasedTemperatureCalculator();
    ShardIndependentTemperatureCalculatorCpuUtilMetric shardIndependentCpuUtilMetric =
        new ShardIndependentTemperatureCalculatorCpuUtilMetric();
    TotalCpuUtilForTotalNodeMetric cpuUtilPeakUsage = new TotalCpuUtilForTotalNodeMetric();

    // heat map is developed only for data nodes.
    cpuUtilByShard.addTag(TAG_LOCUS, LOCUS_DATA_NODE);
    avgCpuUtilByShards.addTag(TAG_LOCUS, LOCUS_DATA_NODE);
    shardIndependentCpuUtilMetric.addTag(TAG_LOCUS, LOCUS_DATA_NODE);
    cpuUtilPeakUsage.addTag(TAG_LOCUS, LOCUS_DATA_NODE);

    heapAllocByShard.addTag(TAG_LOCUS, LOCUS_DATA_NODE);
    heapAllocRateByShardAvg.addTag(TAG_LOCUS, LOCUS_DATA_NODE);
    shardIndependentHeapAllocRate.addTag(TAG_LOCUS, LOCUS_DATA_NODE);
    heapAllocRateTotal.addTag(TAG_LOCUS, LOCUS_DATA_NODE);

    addLeaf(cpuUtilByShard);
    addLeaf(avgCpuUtilByShards);
    addLeaf(shardIndependentCpuUtilMetric);
    addLeaf(cpuUtilPeakUsage);

    addLeaf(heapAllocByShard);
    addLeaf(heapAllocRateByShardAvg);
    addLeaf(shardIndependentHeapAllocRate);
    addLeaf(heapAllocRateTotal);

    CpuUtilDimensionTemperatureRca cpuUtilHeat = new CpuUtilDimensionTemperatureRca(shardStore,
        cpuUtilByShard,
        avgCpuUtilByShards,
        shardIndependentCpuUtilMetric, cpuUtilPeakUsage);
    cpuUtilHeat.addTag(TAG_LOCUS, LOCUS_DATA_NODE);
    cpuUtilHeat.addAllUpstreams(Arrays.asList(cpuUtilByShard, avgCpuUtilByShards,
        shardIndependentCpuUtilMetric, cpuUtilPeakUsage));

    HeapAllocRateTemperatureRca heapAllocRateHeat = new HeapAllocRateTemperatureRca(shardStore,
        heapAllocByShard, heapAllocRateByShardAvg, shardIndependentHeapAllocRate,
        heapAllocRateTotal);

    heapAllocRateHeat.addTag(TAG_LOCUS, LOCUS_DATA_NODE);
    heapAllocRateHeat.addAllUpstreams(Arrays.asList(heapAllocByShard, heapAllocRateByShardAvg,
        shardIndependentHeapAllocRate, heapAllocRateTotal));

    NodeTemperatureRca nodeTemperatureRca = new NodeTemperatureRca(cpuUtilHeat, heapAllocRateHeat);
    nodeTemperatureRca.addTag(TAG_LOCUS, LOCUS_DATA_NODE);
    nodeTemperatureRca.addAllUpstreams(Arrays.asList(cpuUtilHeat, heapAllocRateHeat));

    ClusterTemperatureRca clusterTemperatureRca = new ClusterTemperatureRca(nodeTemperatureRca);
    clusterTemperatureRca.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    clusterTemperatureRca.addAllUpstreams(Collections.singletonList(nodeTemperatureRca));
  }
}
