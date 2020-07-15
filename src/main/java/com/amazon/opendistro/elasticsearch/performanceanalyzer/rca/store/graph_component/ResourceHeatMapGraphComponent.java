/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.graph_component;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.LOCUS_DATA_MASTER_NODE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.LOCUS_DATA_NODE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.LOCUS_MASTER_NODE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.TAG_AGGREGATE_UPSTREAM;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.TAG_LOCUS;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.graph_component.Constants.EVALUATION_INTERVAL_SECONDS;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.ShardStore;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.ElasticSearchAnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.AvgCpuUtilByShardsMetricBasedTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.CpuUtilByShardsMetricBasedTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.HeapAllocRateByShardAvgTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.HeapAllocRateByShardTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.ShardSizeAvgTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.ShardSizeMetricBasedTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.capacity.HeapAllocRateTotalTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.capacity.ShardTotalDiskUsageTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.capacity.TotalCpuUtilForTotalNodeMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.shardIndependent.HeapAllocRateShardIndependentTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.shardIndependent.ShardIndependentTemperatureCalculatorCpuUtilMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.ClusterTemperatureRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.NodeTemperatureRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.dimension.CpuUtilDimensionTemperatureRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.dimension.HeapAllocRateTemperatureRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.dimension.ShardSizeDimensionTemperatureRca;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ResourceHeatMapGraphComponent implements AnalysisGraphComponent {
  private static final Logger LOG = LogManager.getLogger(ResourceHeatMapGraphComponent.class);

  private ElasticSearchAnalysisGraph analysisGraph;

  public ResourceHeatMapGraphComponent(final ElasticSearchAnalysisGraph analysisGraph) {
    this.analysisGraph = analysisGraph;
  }

  @Override
  public List<Metric> getMetrics() {
    return new ArrayList<Metric>() {
      {
        add(new HeapAllocRateByShardTemperatureCalculator());
        add(new HeapAllocRateByShardAvgTemperatureCalculator());
        add(new HeapAllocRateShardIndependentTemperatureCalculator());
        add(new HeapAllocRateTotalTemperatureCalculator());
        add(new CpuUtilByShardsMetricBasedTemperatureCalculator());
        add(new AvgCpuUtilByShardsMetricBasedTemperatureCalculator());
        add(new ShardIndependentTemperatureCalculatorCpuUtilMetric());
        add(new TotalCpuUtilForTotalNodeMetric());
        add(new ShardSizeMetricBasedTemperatureCalculator());
        add(new ShardSizeAvgTemperatureCalculator());
        add(new ShardTotalDiskUsageTemperatureCalculator());
      }
    };
  }

  @Override
  public void registerRca() {
    LOG.info("Constructing temperature profile RCA components");
    final ShardStore shardStore = new ShardStore();
    final CpuUtilByShardsMetricBasedTemperatureCalculator cpuUtilByShard =
        (CpuUtilByShardsMetricBasedTemperatureCalculator)
            analysisGraph
                .getMetric(CpuUtilByShardsMetricBasedTemperatureCalculator.class.getSimpleName())
                .get();
    final AvgCpuUtilByShardsMetricBasedTemperatureCalculator avgCpuUtilByShards =
        (AvgCpuUtilByShardsMetricBasedTemperatureCalculator)
            analysisGraph
                .getMetric(AvgCpuUtilByShardsMetricBasedTemperatureCalculator.class.getSimpleName())
                .get();
    final ShardIndependentTemperatureCalculatorCpuUtilMetric shardIndependentCpuUtilMetric =
        (ShardIndependentTemperatureCalculatorCpuUtilMetric)
            analysisGraph
                .getMetric(ShardIndependentTemperatureCalculatorCpuUtilMetric.class.getSimpleName())
                .get();
    final TotalCpuUtilForTotalNodeMetric cpuUtilPeakUsage =
        (TotalCpuUtilForTotalNodeMetric)
            analysisGraph.getMetric(TotalCpuUtilForTotalNodeMetric.class.getSimpleName()).get();
    final HeapAllocRateByShardTemperatureCalculator heapAllocByShard =
        (HeapAllocRateByShardTemperatureCalculator)
            analysisGraph
                .getMetric(HeapAllocRateByShardTemperatureCalculator.class.getSimpleName())
                .get();
    final HeapAllocRateByShardAvgTemperatureCalculator heapAllocRateByShardAvg =
        (HeapAllocRateByShardAvgTemperatureCalculator)
            analysisGraph
                .getMetric(HeapAllocRateByShardAvgTemperatureCalculator.class.getSimpleName())
                .get();
    final HeapAllocRateShardIndependentTemperatureCalculator shardIndependentHeapAllocRate =
        (HeapAllocRateShardIndependentTemperatureCalculator)
            analysisGraph
                .getMetric(HeapAllocRateShardIndependentTemperatureCalculator.class.getSimpleName())
                .get();
    final HeapAllocRateTotalTemperatureCalculator heapAllocRateTotal =
        (HeapAllocRateTotalTemperatureCalculator)
            analysisGraph
                .getMetric(HeapAllocRateTotalTemperatureCalculator.class.getSimpleName())
                .get();
    final ShardSizeMetricBasedTemperatureCalculator shardSizeByShard =
        (ShardSizeMetricBasedTemperatureCalculator)
            analysisGraph
                .getMetric(ShardSizeMetricBasedTemperatureCalculator.class.getSimpleName())
                .get();
    final ShardSizeAvgTemperatureCalculator shardSizeAvg =
        (ShardSizeAvgTemperatureCalculator)
            analysisGraph.getMetric(ShardSizeAvgTemperatureCalculator.class.getSimpleName()).get();
    final ShardTotalDiskUsageTemperatureCalculator shardTotalDiskUsage =
        (ShardTotalDiskUsageTemperatureCalculator)
            analysisGraph
                .getMetric(ShardTotalDiskUsageTemperatureCalculator.class.getSimpleName())
                .get();

    final CpuUtilDimensionTemperatureRca cpuUtilHeat =
        new CpuUtilDimensionTemperatureRca(
            EVALUATION_INTERVAL_SECONDS,
            shardStore,
            cpuUtilByShard,
            avgCpuUtilByShards,
            shardIndependentCpuUtilMetric,
            cpuUtilPeakUsage);
    cpuUtilHeat.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    cpuUtilHeat.addAllUpstreams(
        Arrays.asList(
            cpuUtilByShard, avgCpuUtilByShards, shardIndependentCpuUtilMetric, cpuUtilPeakUsage));
    analysisGraph.getOrRegisterRca(cpuUtilHeat);

    final HeapAllocRateTemperatureRca heapAllocRateHeat =
        new HeapAllocRateTemperatureRca(
            EVALUATION_INTERVAL_SECONDS,
            shardStore,
            heapAllocByShard,
            heapAllocRateByShardAvg,
            shardIndependentHeapAllocRate,
            heapAllocRateTotal);
    heapAllocRateHeat.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    heapAllocRateHeat.addAllUpstreams(
        Arrays.asList(
            heapAllocByShard,
            heapAllocRateByShardAvg,
            shardIndependentHeapAllocRate,
            heapAllocRateTotal));
    analysisGraph.getOrRegisterRca(heapAllocRateHeat);

    final ShardSizeDimensionTemperatureRca shardSizeHeat =
        new ShardSizeDimensionTemperatureRca(
            EVALUATION_INTERVAL_SECONDS,
            shardStore,
            shardSizeByShard,
            shardSizeAvg,
            shardTotalDiskUsage);
    shardSizeHeat.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    shardSizeHeat.addAllUpstreams(Arrays.asList(shardSizeByShard, shardSizeAvg, shardTotalDiskUsage));
    analysisGraph.getOrRegisterRca(shardSizeHeat);

    final NodeTemperatureRca nodeTemperatureRca = new NodeTemperatureRca(cpuUtilHeat, heapAllocRateHeat, shardSizeHeat);
    nodeTemperatureRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    nodeTemperatureRca.addAllUpstreams(Arrays.asList(cpuUtilHeat, heapAllocRateHeat, shardSizeHeat));
    analysisGraph.getOrRegisterRca(nodeTemperatureRca);

    final ClusterTemperatureRca clusterTemperatureRca = new ClusterTemperatureRca(nodeTemperatureRca);
    clusterTemperatureRca.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    clusterTemperatureRca.addTag(TAG_AGGREGATE_UPSTREAM, LOCUS_DATA_NODE);
    clusterTemperatureRca.addAllUpstreams(Collections.singletonList(nodeTemperatureRca));
    analysisGraph.getOrRegisterRca(clusterTemperatureRca);
  }
}
