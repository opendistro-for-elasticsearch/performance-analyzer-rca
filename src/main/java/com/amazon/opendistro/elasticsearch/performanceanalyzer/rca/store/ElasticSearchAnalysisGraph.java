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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.CacheHealthDecider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.Publisher;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.QueueHealthDecider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.collator.Collator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.jvm.HeapHealthDecider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ShardStatsDerivedDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.PluginController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.PluginControllerConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Bitset_Memory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.CPU_Utilization;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_FieldData_Eviction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_FieldData_Size;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_Max_Size;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_Query_Size;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_Request_Eviction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_Request_Hit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_Request_Size;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.DocValues_Memory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.GC_Collection_Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.GC_Collection_Time;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Max;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Used;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.IO_TotThroughput;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.IO_TotalSyscallRate;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.IndexWriter_Memory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Norms_Memory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Points_Memory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Segments_Memory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.StoredFields_Memory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.TermVectors_Memory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Terms_Memory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.ThreadPool_QueueCapacity;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.ThreadPool_RejectedReqs;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.VersionMap_Memory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.ShardStore;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigClusterCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.collector.NodeConfigCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.AggregateMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.AggregateMetric.AggregateFunction;
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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HotNodeClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HotNodeRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cache.FieldDataCacheRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cache.ShardRequestCacheRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.FieldDataCacheClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.QueueRejectionClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.ShardRequestCacheClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hot_node.HighCpuRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotheap.HighHeapUsageOldGenRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotheap.HighHeapUsageYoungGenRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotshard.HotShardClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotshard.HotShardRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.ClusterTemperatureRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.NodeTemperatureRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.dimension.CpuUtilDimensionTemperatureRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.dimension.HeapAllocRateTemperatureRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.dimension.ShardSizeDimensionTemperatureRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.threadpool.QueueRejectionRca;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ElasticSearchAnalysisGraph extends AnalysisGraph {

  private static final Logger LOG = LogManager.getLogger(ElasticSearchAnalysisGraph.class);
  private static final int RCA_PERIOD = 12;  // 1 minute. RCA_PERIOD is measured as number of EVALUATION_INTERVAL_SECONDS
  private static final int EVALUATION_INTERVAL_SECONDS = 5;


  @Override
  public void construct() {
    Metric heapUsed = new Heap_Used(EVALUATION_INTERVAL_SECONDS);
    Metric gcEvent = new GC_Collection_Event(EVALUATION_INTERVAL_SECONDS);
    Heap_Max heapMax = new Heap_Max(EVALUATION_INTERVAL_SECONDS);
    Metric gc_Collection_Time = new GC_Collection_Time(EVALUATION_INTERVAL_SECONDS);
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

    Rca<ResourceFlowUnit<HotResourceSummary>> highHeapUsageOldGenRca = new HighHeapUsageOldGenRca(RCA_PERIOD, heapUsed, gcEvent,
            heapMax, nodeStatsMetrics);
    highHeapUsageOldGenRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    List<Node<?>> upstream = new ArrayList<>(Arrays.asList(heapUsed, gcEvent, heapMax));
    upstream.addAll(nodeStatsMetrics);
    highHeapUsageOldGenRca.addAllUpstreams(upstream);

    Rca<ResourceFlowUnit<HotResourceSummary>> highHeapUsageYoungGenRca = new HighHeapUsageYoungGenRca(RCA_PERIOD, heapUsed,
            gc_Collection_Time);
    highHeapUsageYoungGenRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    highHeapUsageYoungGenRca.addAllUpstreams(Arrays.asList(heapUsed, gc_Collection_Time));

    Rca<ResourceFlowUnit<HotResourceSummary>> highCpuRca = new HighCpuRca(RCA_PERIOD, cpuUtilizationGroupByOperation);
    highCpuRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    highCpuRca.addAllUpstreams(Collections.singletonList(cpuUtilizationGroupByOperation));

    Rca<ResourceFlowUnit<HotNodeSummary>> hotJVMNodeRca = new HotNodeRca(RCA_PERIOD, highHeapUsageOldGenRca,
            highHeapUsageYoungGenRca, highCpuRca);
    hotJVMNodeRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    hotJVMNodeRca.addAllUpstreams(
            Arrays.asList(highHeapUsageOldGenRca, highHeapUsageYoungGenRca, highCpuRca));

    HighHeapUsageClusterRca highHeapUsageClusterRca =
            new HighHeapUsageClusterRca(RCA_PERIOD, hotJVMNodeRca);
    highHeapUsageClusterRca.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    highHeapUsageClusterRca.addAllUpstreams(Collections.singletonList(hotJVMNodeRca));
    highHeapUsageClusterRca.addTag(TAG_AGGREGATE_UPSTREAM, LOCUS_DATA_NODE);

    Rca<ResourceFlowUnit<HotClusterSummary>> hotNodeClusterRca =
            new HotNodeClusterRca(RCA_PERIOD, hotJVMNodeRca);
    hotNodeClusterRca.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    hotNodeClusterRca.addAllUpstreams(Collections.singletonList(hotJVMNodeRca));

    // Heap Health Decider
    HeapHealthDecider heapHealthDecider = new HeapHealthDecider(12, highHeapUsageClusterRca);
    heapHealthDecider.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    heapHealthDecider.addAllUpstreams(Collections.singletonList(highHeapUsageClusterRca));

    /* Queue Rejection RCAs
     */
    // TODO: Refactor this monolithic function
    Metric threadpool_RejectedReqs = new ThreadPool_RejectedReqs(EVALUATION_INTERVAL_SECONDS);
    threadpool_RejectedReqs.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    addLeaf(threadpool_RejectedReqs);

    // Node level queue rejection RCA
    QueueRejectionRca queueRejectionNodeRca = new QueueRejectionRca(RCA_PERIOD, threadpool_RejectedReqs);
    queueRejectionNodeRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    queueRejectionNodeRca.addAllUpstreams(Collections.singletonList(threadpool_RejectedReqs));

    // Cluster level queue rejection RCA
    QueueRejectionClusterRca queueRejectionClusterRca = new QueueRejectionClusterRca(RCA_PERIOD, queueRejectionNodeRca);
    queueRejectionClusterRca.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    queueRejectionClusterRca.addAllUpstreams(Collections.singletonList(queueRejectionNodeRca));
    queueRejectionClusterRca.addTag(TAG_AGGREGATE_UPSTREAM, LOCUS_DATA_NODE);

    // Queue Health Decider
    QueueHealthDecider queueHealthDecider = new QueueHealthDecider(EVALUATION_INTERVAL_SECONDS, 12, queueRejectionClusterRca);
    queueHealthDecider.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    queueHealthDecider.addAllUpstreams(Collections.singletonList(queueRejectionClusterRca));

    // Node Config Collector
    ThreadPool_QueueCapacity queueCapacity = new ThreadPool_QueueCapacity();
    queueCapacity.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    addLeaf(queueCapacity);

    Cache_Max_Size cacheMaxSize =  new Cache_Max_Size(EVALUATION_INTERVAL_SECONDS);
    cacheMaxSize.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    addLeaf(cacheMaxSize);

    NodeConfigCollector nodeConfigCollector = new NodeConfigCollector(RCA_PERIOD, queueCapacity, cacheMaxSize, heapMax);
    nodeConfigCollector.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    nodeConfigCollector.addAllUpstreams(Arrays.asList(queueCapacity, cacheMaxSize, heapMax));
    NodeConfigClusterCollector nodeConfigClusterCollector = new NodeConfigClusterCollector(nodeConfigCollector);
    nodeConfigClusterCollector.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    nodeConfigClusterCollector.addAllUpstreams(Collections.singletonList(nodeConfigCollector));
    nodeConfigClusterCollector.addTag(TAG_AGGREGATE_UPSTREAM, LOCUS_DATA_NODE);

    // Field Data Cache RCA
    Metric fieldDataCacheEvictions = new Cache_FieldData_Eviction(EVALUATION_INTERVAL_SECONDS);
    fieldDataCacheEvictions.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    addLeaf(fieldDataCacheEvictions);

    Metric fieldDataCacheSizeGroupByOperation = new AggregateMetric(EVALUATION_INTERVAL_SECONDS,
            Cache_FieldData_Size.NAME,
            AggregateFunction.SUM,
            MetricsDB.MAX, ShardStatsDerivedDimension.INDEX_NAME.toString());
    fieldDataCacheSizeGroupByOperation.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    addLeaf(fieldDataCacheSizeGroupByOperation);

    FieldDataCacheRca fieldDataCacheNodeRca = new FieldDataCacheRca(RCA_PERIOD,
            fieldDataCacheEvictions,
            fieldDataCacheSizeGroupByOperation);
    fieldDataCacheNodeRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    fieldDataCacheNodeRca.addAllUpstreams(Arrays.asList(fieldDataCacheEvictions, fieldDataCacheSizeGroupByOperation));

    FieldDataCacheClusterRca fieldDataCacheClusterRca = new FieldDataCacheClusterRca(RCA_PERIOD, fieldDataCacheNodeRca);
    fieldDataCacheClusterRca.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    fieldDataCacheClusterRca.addAllUpstreams(Collections.singletonList(fieldDataCacheNodeRca));
    fieldDataCacheClusterRca.addTag(TAG_AGGREGATE_UPSTREAM, LOCUS_DATA_NODE);

    // Shard Request Cache RCA
    Metric shardRequestCacheEvictions = new Cache_Request_Eviction(EVALUATION_INTERVAL_SECONDS);
    shardRequestCacheEvictions.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    addLeaf(shardRequestCacheEvictions);
    Metric shardRequestHits = new Cache_Request_Hit(EVALUATION_INTERVAL_SECONDS);
    shardRequestHits.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    addLeaf(shardRequestHits);

    Metric shardRequestCacheSizeGroupByOperation = new AggregateMetric(EVALUATION_INTERVAL_SECONDS,
            Cache_Request_Size.NAME,
            AggregateFunction.SUM,
            MetricsDB.MAX, ShardStatsDerivedDimension.INDEX_NAME.toString());
    shardRequestCacheSizeGroupByOperation.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    addLeaf(shardRequestCacheSizeGroupByOperation);

    ShardRequestCacheRca shardRequestCacheNodeRca = new ShardRequestCacheRca(RCA_PERIOD,
            shardRequestCacheEvictions,
            shardRequestHits,
            shardRequestCacheSizeGroupByOperation);
    shardRequestCacheNodeRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    shardRequestCacheNodeRca.addAllUpstreams(Arrays.asList(
            shardRequestCacheEvictions, shardRequestHits, shardRequestCacheSizeGroupByOperation));

    ShardRequestCacheClusterRca shardRequestCacheClusterRca = new ShardRequestCacheClusterRca(RCA_PERIOD, shardRequestCacheNodeRca);
    shardRequestCacheClusterRca.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    shardRequestCacheClusterRca.addAllUpstreams(Collections.singletonList(shardRequestCacheNodeRca));
    shardRequestCacheClusterRca.addTag(TAG_AGGREGATE_UPSTREAM, LOCUS_DATA_NODE);

    // Cache Health Decider
    CacheHealthDecider cacheHealthDecider = new CacheHealthDecider(
            EVALUATION_INTERVAL_SECONDS, 12, fieldDataCacheClusterRca, shardRequestCacheClusterRca);
    cacheHealthDecider.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    cacheHealthDecider.addAllUpstreams(Arrays.asList(fieldDataCacheClusterRca, shardRequestCacheClusterRca));

    constructShardResourceUsageGraph();

    //constructResourceHeatMapGraph();

    // Collator - Collects actions from all deciders and aligns impact vectors
    Collator collator = new Collator(queueHealthDecider, cacheHealthDecider, heapHealthDecider);
    collator.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    collator.addAllUpstreams(Arrays.asList(queueHealthDecider, cacheHealthDecider, heapHealthDecider));

    // Publisher - Executes decisions output from collator
    Publisher publisher = new Publisher(EVALUATION_INTERVAL_SECONDS, collator);
    publisher.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    publisher.addAllUpstreams(Collections.singletonList(collator));

    // TODO: Refactor using DI to move out of construct method
    PluginControllerConfig pluginControllerConfig = new PluginControllerConfig();
    PluginController pluginController = new PluginController(pluginControllerConfig, publisher);
    pluginController.initPlugins();
  }

  private void constructShardResourceUsageGraph() {
    Metric cpuUtilization = new CPU_Utilization(EVALUATION_INTERVAL_SECONDS);
    Metric ioTotThroughput = new IO_TotThroughput(EVALUATION_INTERVAL_SECONDS);
    Metric ioTotSyscallRate = new IO_TotalSyscallRate(EVALUATION_INTERVAL_SECONDS);

    cpuUtilization.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    ioTotThroughput.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    ioTotSyscallRate.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    addLeaf(cpuUtilization);
    addLeaf(ioTotThroughput);
    addLeaf(ioTotSyscallRate);

    // High CPU Utilization RCA
    HotShardRca hotShardRca = new HotShardRca(EVALUATION_INTERVAL_SECONDS, RCA_PERIOD, cpuUtilization, ioTotThroughput, ioTotSyscallRate);
    hotShardRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    hotShardRca.addAllUpstreams(Arrays.asList(cpuUtilization, ioTotThroughput, ioTotSyscallRate));

    // Hot Shard Cluster RCA which consumes the above
    HotShardClusterRca hotShardClusterRca = new HotShardClusterRca(RCA_PERIOD, hotShardRca);
    hotShardClusterRca.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    hotShardClusterRca.addAllUpstreams(Collections.singletonList(hotShardRca));
    hotShardClusterRca.addTag(TAG_AGGREGATE_UPSTREAM, LOCUS_DATA_NODE);
  }

  private List<Metric> constructNodeStatsMetrics() {
    List<Metric> nodeStatsMetrics = new ArrayList<Metric>() {{
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

    ShardSizeMetricBasedTemperatureCalculator shardSizeByShard =
            new ShardSizeMetricBasedTemperatureCalculator();
    ShardSizeAvgTemperatureCalculator shardSizeAvg =
            new ShardSizeAvgTemperatureCalculator();
    ShardTotalDiskUsageTemperatureCalculator shardTotalDiskUsage =
            new ShardTotalDiskUsageTemperatureCalculator();

    // heat map is developed only for data nodes.
    cpuUtilByShard.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    avgCpuUtilByShards.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    shardIndependentCpuUtilMetric.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    cpuUtilPeakUsage.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);

    heapAllocByShard.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    heapAllocRateByShardAvg.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    shardIndependentHeapAllocRate.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    heapAllocRateTotal.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);

    shardSizeByShard.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    shardSizeAvg.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    shardTotalDiskUsage.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);

    addLeaf(cpuUtilByShard);
    addLeaf(avgCpuUtilByShards);
    addLeaf(shardIndependentCpuUtilMetric);
    addLeaf(cpuUtilPeakUsage);

    addLeaf(heapAllocByShard);
    addLeaf(heapAllocRateByShardAvg);
    addLeaf(shardIndependentHeapAllocRate);
    addLeaf(heapAllocRateTotal);

    addLeaf(shardSizeByShard);
    addLeaf(shardSizeAvg);
    addLeaf(shardTotalDiskUsage);

    CpuUtilDimensionTemperatureRca cpuUtilHeat = new
            CpuUtilDimensionTemperatureRca(EVALUATION_INTERVAL_SECONDS,
            shardStore,
            cpuUtilByShard, avgCpuUtilByShards, shardIndependentCpuUtilMetric, cpuUtilPeakUsage);
    cpuUtilHeat.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    cpuUtilHeat.addAllUpstreams(Arrays.asList(cpuUtilByShard, avgCpuUtilByShards,
            shardIndependentCpuUtilMetric, cpuUtilPeakUsage));

    HeapAllocRateTemperatureRca heapAllocRateHeat = new HeapAllocRateTemperatureRca(EVALUATION_INTERVAL_SECONDS,
            shardStore,
            heapAllocByShard, heapAllocRateByShardAvg, shardIndependentHeapAllocRate, heapAllocRateTotal);

    heapAllocRateHeat.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    heapAllocRateHeat.addAllUpstreams(Arrays.asList(heapAllocByShard, heapAllocRateByShardAvg,
            shardIndependentHeapAllocRate, heapAllocRateTotal));

    ShardSizeDimensionTemperatureRca shardSizeHeat = new ShardSizeDimensionTemperatureRca(EVALUATION_INTERVAL_SECONDS,
            shardStore,
            shardSizeByShard, shardSizeAvg, shardTotalDiskUsage);
    shardSizeHeat.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    shardSizeHeat.addAllUpstreams(Arrays.asList(shardSizeByShard, shardSizeAvg, shardTotalDiskUsage));

    NodeTemperatureRca nodeTemperatureRca = new NodeTemperatureRca(cpuUtilHeat, heapAllocRateHeat, shardSizeHeat);
    nodeTemperatureRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
    nodeTemperatureRca.addAllUpstreams(Arrays.asList(cpuUtilHeat, heapAllocRateHeat, shardSizeHeat));

    ClusterTemperatureRca clusterTemperatureRca = new ClusterTemperatureRca(nodeTemperatureRca);
    clusterTemperatureRca.addTag(TAG_LOCUS, LOCUS_MASTER_NODE);
    clusterTemperatureRca.addTag(TAG_AGGREGATE_UPSTREAM, LOCUS_DATA_NODE);
    clusterTemperatureRca.addAllUpstreams(Collections.singletonList(nodeTemperatureRca));
  }
}
