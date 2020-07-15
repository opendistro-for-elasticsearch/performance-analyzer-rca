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
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.graph_component.Constants.RCA_PERIOD;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.ElasticSearchAnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.AggregateMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HotNodeClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HotNodeRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hot_node.HighCpuRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotheap.HighHeapUsageOldGenRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotheap.HighHeapUsageYoungGenRca;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ResourcesGraphComponent implements AnalysisGraphComponent {
  private ElasticSearchAnalysisGraph analysisGraph;
  private List<Metric> nodeStatsMetrics;

  public ResourcesGraphComponent(final ElasticSearchAnalysisGraph analysisGraph) {
    this.analysisGraph = analysisGraph;
    this.nodeStatsMetrics = constructNodeStatsMetrics();
  }

  @Override
  public List<Metric> getMetrics() {
    return new ArrayList<Metric>() {
      {
        add(new Heap_Used(EVALUATION_INTERVAL_SECONDS));
        add(new GC_Collection_Event(EVALUATION_INTERVAL_SECONDS));
        add(new Heap_Max(EVALUATION_INTERVAL_SECONDS));
        add(new GC_Collection_Time(EVALUATION_INTERVAL_SECONDS));
        add(
            new AggregateMetric(
                1,
                CPU_Utilization.NAME,
                AggregateMetric.AggregateFunction.SUM,
                MetricsDB.AVG,
                AllMetrics.CommonDimension.OPERATION.toString()));
        addAll(nodeStatsMetrics);
      }
    };
  }

  @Override
  public void registerRca() {
    final Metric heapUsed = analysisGraph.getMetric(AllMetrics.HeapValue.Constants.USED_VALUE).get();
    final Metric gcEvent = analysisGraph.getMetric(AllMetrics.HeapValue.Constants.COLLECTION_COUNT_VALUE).get();
    final Metric heapMax = analysisGraph.getMetric(AllMetrics.HeapValue.HEAP_MAX.name()).get();
    final Metric gcCollectionTime = analysisGraph.getMetric(AllMetrics.HeapValue.GC_COLLECTION_TIME.name()).get();
    final Metric cpuUtilizationGroupByOperation = analysisGraph.getMetric(AggregateMetric.class.getSimpleName()).get();

    final Rca<ResourceFlowUnit<HotResourceSummary>> highHeapUsageOldGenRca =
            new HighHeapUsageOldGenRca(RCA_PERIOD, heapUsed, gcEvent, heapMax, nodeStatsMetrics);
    highHeapUsageOldGenRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    List<Node<?>> upstream = new ArrayList<>(Arrays.asList(heapUsed, gcEvent, heapMax));
    upstream.addAll(nodeStatsMetrics);
    highHeapUsageOldGenRca.addAllUpstreams(upstream);
    analysisGraph.getOrRegisterRca(highHeapUsageOldGenRca);

    final Rca<ResourceFlowUnit<HotResourceSummary>> highHeapUsageYoungGenRca =
            new HighHeapUsageYoungGenRca(RCA_PERIOD, heapUsed, gcCollectionTime);
    highHeapUsageYoungGenRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    highHeapUsageYoungGenRca.addAllUpstreams(Arrays.asList(heapUsed, gcCollectionTime));
    analysisGraph.getOrRegisterRca(highHeapUsageYoungGenRca);

    final Rca<ResourceFlowUnit<HotResourceSummary>> highCpuRca =
            new HighCpuRca(RCA_PERIOD, cpuUtilizationGroupByOperation);
    highCpuRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    highCpuRca.addAllUpstreams(Collections.singletonList(cpuUtilizationGroupByOperation));
    analysisGraph.getOrRegisterRca(highCpuRca);

    final Rca<ResourceFlowUnit<HotNodeSummary>> hotJVMNodeRca =
        new HotNodeRca(RCA_PERIOD, highHeapUsageOldGenRca, highHeapUsageYoungGenRca, highCpuRca);
    hotJVMNodeRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    hotJVMNodeRca.addAllUpstreams(
        Arrays.asList(highHeapUsageOldGenRca, highHeapUsageYoungGenRca, highCpuRca));
    analysisGraph.getOrRegisterRca(hotJVMNodeRca);

    final Rca<ResourceFlowUnit<HotClusterSummary>> highHeapUsageClusterRca =
        new HighHeapUsageClusterRca(RCA_PERIOD, hotJVMNodeRca);
    highHeapUsageClusterRca.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    highHeapUsageClusterRca.addAllUpstreams(Collections.singletonList(hotJVMNodeRca));
    highHeapUsageClusterRca.addTag(TAG_AGGREGATE_UPSTREAM, LOCUS_DATA_NODE);
    analysisGraph.getOrRegisterRca(highHeapUsageClusterRca);

    final Rca<ResourceFlowUnit<HotClusterSummary>> hotNodeClusterRca =
            new HotNodeClusterRca(RCA_PERIOD, hotJVMNodeRca);
    hotNodeClusterRca.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    hotNodeClusterRca.addAllUpstreams(Collections.singletonList(hotJVMNodeRca));
    analysisGraph.getOrRegisterRca(hotNodeClusterRca);
  }

  private List<Metric> constructNodeStatsMetrics() {
    return new ArrayList<Metric>() {
      {
        add(new Cache_FieldData_Size(EVALUATION_INTERVAL_SECONDS));
        add(new Cache_Request_Size(EVALUATION_INTERVAL_SECONDS));
        add(new Cache_Query_Size(EVALUATION_INTERVAL_SECONDS));
        add(new Segments_Memory(EVALUATION_INTERVAL_SECONDS));
        add(new Terms_Memory(EVALUATION_INTERVAL_SECONDS));
        add(new StoredFields_Memory(EVALUATION_INTERVAL_SECONDS));
        add(new TermVectors_Memory(EVALUATION_INTERVAL_SECONDS));
        add(new Norms_Memory(EVALUATION_INTERVAL_SECONDS));
        add(new Points_Memory(EVALUATION_INTERVAL_SECONDS));
        add(new DocValues_Memory(EVALUATION_INTERVAL_SECONDS));
        add(new IndexWriter_Memory(EVALUATION_INTERVAL_SECONDS));
        add(new Bitset_Memory(EVALUATION_INTERVAL_SECONDS));
        add(new VersionMap_Memory(EVALUATION_INTERVAL_SECONDS));
      }
    };
  }
}
