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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotshard;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension.INDEX_NAME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension.SHARD_ID;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.HOT_SHARD_RCA_ERROR_METRIC;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.HardwareEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HotShardRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindow;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.aggregators.SlidingWindowData;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotShardSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;

/**
 * This RCA is to identify a hot shard within an index. A Hot shard is an outlier within its counterparts.
 * The RCA subscribes to following metrics :
 * 1. CPU_Utilization
 * 2. IO_TotThroughput
 * 3. IO_TotalSyscallRate
 *
 * <p>The RCA looks at the above 3 metric data, compares the values against the threshold for each resource
 * and if the usage for any of 3 resources is greater than their individual threshold, we mark the context
 * as 'UnHealthy' and create a HotShardResourceSummary for the shard.
 *
 * <p>Optional metrics which can be added in future :
 * 1. Heap_AllocRate
 * 2. Paging_RSS
 *
 */
public class HotShardRca extends Rca<ResourceFlowUnit<HotNodeSummary>> {

    private static final Logger LOG = LogManager.getLogger(HotShardRca.class);
    private static final int SLIDING_WINDOW_IN_SECONDS =  60;

    private double cpuUtilizationThreshold;
    private double ioTotThroughputThreshold;
    private double ioTotSysCallRateThreshold;

    private final Metric cpuUtilization;
    private final Metric ioTotThroughput;
    private final Metric ioTotSyscallRate;
    private final ResourceType resourceType;
    private final int rcaPeriod;
    private int counter;
    protected Clock clock;

    // HashMap with IndexShardKey object as key and SlidingWindowData object of metric data as value
    private HashMap<IndexShardKey, SlidingWindow<SlidingWindowData>> cpuUtilizationMap;
    private HashMap<IndexShardKey, SlidingWindow<SlidingWindowData>> ioTotThroughputMap;
    private HashMap<IndexShardKey, SlidingWindow<SlidingWindowData>> ioTotSyscallRateMap;

    public <M extends Metric> HotShardRca(final long evaluationIntervalSeconds, final int rcaPeriod,
                                          final M cpuUtilization, final M ioTotThroughput, final M ioTotSyscallRate) {
        super(evaluationIntervalSeconds);
        this.cpuUtilization = cpuUtilization;
        this.ioTotThroughput = ioTotThroughput;
        this.ioTotSyscallRate = ioTotSyscallRate;
        this.rcaPeriod = rcaPeriod;
        this.counter = 0;
        this.resourceType = ResourceType.newBuilder().setHardwareResourceTypeValue(HardwareEnum.CPU_VALUE).build();
        this.clock = Clock.systemUTC();
        this.cpuUtilizationMap = new HashMap<>();
        this.ioTotThroughputMap = new HashMap<>();
        this.ioTotSyscallRateMap = new HashMap<>();
        this.cpuUtilizationThreshold = HotShardRcaConfig.DEFAULT_CPU_UTILIZATION_THRESHOLD;
        this.ioTotThroughputThreshold = HotShardRcaConfig.DEFAULT_IO_TOTAL_THROUGHPUT_THRESHOLD_IN_BYTE_PER_SEC;
        this.ioTotSysCallRateThreshold = HotShardRcaConfig.DEFAULT_IO_TOTAL_SYSCALL_RATE_THRESHOLD_PER_SEC;
    }

    private void consumeFlowUnit(final MetricFlowUnit metricFlowUnit, final String metricType,
                                 final HashMap<IndexShardKey, SlidingWindow<SlidingWindowData>> metricMap) {
        for (Record record : metricFlowUnit.getData()) {
            try {
                String indexName = record.getValue(INDEX_NAME.toString(), String.class);
                Integer shardId = record.getValue(SHARD_ID.toString(), Integer.class);
                if (indexName != null &&  shardId != null) {
                    IndexShardKey indexShardKey = IndexShardKey.buildIndexShardKey(record);
                    double usage = record.getValue(MetricsDB.SUM, Double.class);
                    SlidingWindow<SlidingWindowData> usageDeque = metricMap.get(indexShardKey);
                    if (null == usageDeque) {
                        usageDeque = new SlidingWindow<>(SLIDING_WINDOW_IN_SECONDS, TimeUnit.SECONDS);
                        metricMap.put(indexShardKey, usageDeque);
                    }
                    usageDeque.next(new SlidingWindowData(this.clock.millis(), usage));
                }
            } catch (Exception e) {
                StatsCollector.instance().logMetric(HOT_SHARD_RCA_ERROR_METRIC);
                LOG.error("Failed to parse metric in FlowUnit: {} from {}", record, metricType);
            }
        }
    }

    private void consumeMetrics(final Metric metric,
                                final HashMap<IndexShardKey, SlidingWindow<SlidingWindowData>> metricMap) {
        for (MetricFlowUnit metricFlowUnit : metric.getFlowUnits()) {
            if (metricFlowUnit.getData() != null) {
                consumeFlowUnit(metricFlowUnit, metric.getClass().getName(), metricMap);
            }
        }
    }

    private double fetchUsageValueFromMap(HashMap<IndexShardKey, SlidingWindow<SlidingWindowData>> usageMap, IndexShardKey indexShardKey) {
        double value = 0;
        if (usageMap.get(indexShardKey) != null) {
            value = usageMap.get(indexShardKey).readAvg(TimeUnit.SECONDS);
        }
        return value;
    }

    /**
     * Locally identifies hot shards on the node.
     * The function uses CPU_Utilization, IO_TotThroughput and IO_TotalSyscallRate FlowUnits
     * to identify a Hot Shard.
     *
     * <p>We specify the threshold for CPU_Utilization, IO_TotThroughput and IO_TotalSyscallRate and
     * any shard using either of 3 resources more than the specified threshold is declared Hot.
     *
     */
    @Override
    public ResourceFlowUnit<HotNodeSummary> operate() {
        counter += 1;

        // Populate the Resource HashMaps
        consumeMetrics(cpuUtilization, cpuUtilizationMap);
        consumeMetrics(ioTotThroughput, ioTotThroughputMap);
        consumeMetrics(ioTotSyscallRate, ioTotSyscallRateMap);;

        if (counter == rcaPeriod) {
            ResourceContext context = new ResourceContext(Resources.State.HEALTHY);
            List<GenericSummary> HotShardSummaryList = new ArrayList<>();
            ClusterDetailsEventProcessor.NodeDetails currentNode = ClusterDetailsEventProcessor.getCurrentNodeDetails();

            Set<IndexShardKey> indexShardKeySet = new HashSet<>(cpuUtilizationMap.keySet());
            indexShardKeySet.addAll(ioTotThroughputMap.keySet());
            indexShardKeySet.addAll(ioTotSyscallRateMap.keySet());

            for (IndexShardKey indexShardKey : indexShardKeySet) {
                double avgCpuUtilization = fetchUsageValueFromMap(cpuUtilizationMap, indexShardKey);
                double avgIoTotThroughput = fetchUsageValueFromMap(ioTotThroughputMap, indexShardKey);
                double avgIoTotSyscallRate = fetchUsageValueFromMap(ioTotSyscallRateMap, indexShardKey);

                if (avgCpuUtilization > cpuUtilizationThreshold
                        || avgIoTotThroughput > ioTotThroughputThreshold
                        || avgIoTotSyscallRate > ioTotSysCallRateThreshold) {
                    HotShardSummary summary = new HotShardSummary(indexShardKey.getIndexName(),
                            String.valueOf(indexShardKey.getShardId()), currentNode.getId(), SLIDING_WINDOW_IN_SECONDS);
                    summary.setcpuUtilization(avgCpuUtilization);
                    summary.setCpuUtilizationThreshold(cpuUtilizationThreshold);
                    summary.setIoThroughput(avgIoTotThroughput);
                    summary.setIoThroughputThreshold(ioTotThroughputThreshold);
                    summary.setIoSysCallrate(avgIoTotSyscallRate);
                    summary.setIoSysCallrateThreshold(ioTotSysCallRateThreshold);
                    HotShardSummaryList.add(summary);
                    context = new ResourceContext(Resources.State.UNHEALTHY);
                    LOG.debug("Hot Shard Identified, Shard : {} , avgCpuUtilization = {} , avgIoTotThroughput = {}, "
                            + "avgIoTotSyscallRate = {}", indexShardKey, avgCpuUtilization, avgIoTotThroughput, avgIoTotSyscallRate);
                }
            }

            // reset the variables
            counter = 0;

            HotNodeSummary summary = new HotNodeSummary(currentNode.getId(), currentNode.getHostAddress());
            summary.addNestedSummaryList(HotShardSummaryList);

            //check if the current node is data node. If it is the data node
            //then HotNodeRca is the top level RCA on this node and we want to persist summaries in flowunit.
            boolean isDataNode = !currentNode.getIsMasterNode();
            return new ResourceFlowUnit<>(this.clock.millis(), context, summary, isDataNode);
        } else {
            LOG.debug("Empty FlowUnit returned for Hot Shard RCA");
            return new ResourceFlowUnit<>(this.clock.millis());
        }
    }

    /**
     * read threshold values from rca.conf
     * @param conf RcaConf object
     */
    @Override
    public void readRcaConf(RcaConf conf) {
        HotShardRcaConfig configObj = conf.getHotShardRcaConfig();
        cpuUtilizationThreshold = configObj.getCpuUtilizationThreshold();
        ioTotThroughputThreshold = configObj.getIoTotThroughputThreshold();
        ioTotSysCallRateThreshold = configObj.getIoTotSysCallRateThreshold();
    }

    @Override
    public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
        final List<FlowUnitMessage> flowUnitMessages =
                args.getWireHopper().readFromWire(args.getNode());
        List<ResourceFlowUnit<HotNodeSummary>> flowUnitList = new ArrayList<>();
        LOG.debug("rca: Executing fromWire: {}", this.getClass().getSimpleName());
        for (FlowUnitMessage flowUnitMessage : flowUnitMessages) {
            flowUnitList.add(ResourceFlowUnit.buildFlowUnitFromWrapper(flowUnitMessage));
        }
        setFlowUnits(flowUnitList);
    }
}
