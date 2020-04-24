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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.HardwareEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
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
public class HighCPUShardRca extends Rca<ResourceFlowUnit> {

    private static final Logger LOG = LogManager.getLogger(HighCPUShardRca.class);
    private static final int SLIDING_WINDOW_IN_SECONDS =  60;

    //TODO : {@khushbr} refine the threshold values and read same from config file
    private static final double CPU_USAGE_THRESHOLD = 0.01;
    private static final double IO_TOT_THROUGHPUT_THRESHOLD_IN_BYTES = 250000;
    private static final double IO_TOT_SYSCALL_RATE_THRESHOLD_PER_SECOND = 0.01;

    private final Metric cpuUsage;
    private final Metric ioTotThroughput;
    private final Metric ioTotSyscallRate;
    private final ResourceType resourceType;
    private final int rcaPeriod;
    private int counter;
    protected Clock clock;

    // HashMap with IndexShardKey object as key and SlidingWindowData object of metric data as value
    private HashMap<IndexShardKey, SlidingWindow<SlidingWindowData>> cpuUsageHashMap;
    private HashMap<IndexShardKey, SlidingWindow<SlidingWindowData>> ioTotThroughputHashMap;
    private HashMap<IndexShardKey, SlidingWindow<SlidingWindowData>>  ioTotSyscallRateHashMap;

    public <M extends Metric> HighCPUShardRca(final long evaluationIntervalSeconds, final int rcaPeriod,
            final M cpuUsage, final M ioTotThroughput, final M ioTotSyscallRate) {
        super(evaluationIntervalSeconds);
        this.cpuUsage = cpuUsage;
        this.ioTotThroughput = ioTotThroughput;
        this.ioTotSyscallRate = ioTotSyscallRate;
        this.rcaPeriod = rcaPeriod;
        this.counter = 0;
        this.resourceType = ResourceType.newBuilder().setHardwareResourceTypeValue(HardwareEnum.CPU_VALUE).build();
        this.clock = Clock.systemUTC();
        this.cpuUsageHashMap = new HashMap<>();
        this.ioTotThroughputHashMap = new HashMap<>();
        this.ioTotSyscallRateHashMap = new HashMap<>();
    }

    private void consumeFlowUnit(MetricFlowUnit metricFlowUnit, String metricType,
                                 HashMap<IndexShardKey, SlidingWindow<SlidingWindowData>> metricMap) {
        for (Record record : metricFlowUnit.getData()) {
            try {
                String indexName = record.getValue(INDEX_NAME.toString(), String.class);
                String shardId = record.getValue(SHARD_ID.toString(), String.class);
                if (indexName == null || shardId == null) {
                    continue;
                }
                double usage = record.getValue(MetricsDB.SUM, Double.class);
                IndexShardKey indexShardKey = new IndexShardKey(indexName, shardId);
                SlidingWindow<SlidingWindowData> usageDeque = metricMap.get(indexShardKey);
                if (null == usageDeque) {
                    usageDeque = new SlidingWindow<>(SLIDING_WINDOW_IN_SECONDS, TimeUnit.SECONDS);
                    metricMap.put(indexShardKey, usageDeque);
                }
                usageDeque.next(new SlidingWindowData(this.clock.millis(), usage));
            } catch (Exception e) {
                // TODO: Add a metric here.
                LOG.error("Failed to parse metric in FlowUnit from {}", metricType);
            }
        }
    }

    private void consumeMetrics(List<MetricFlowUnit> metrics,
                                HashMap<IndexShardKey, SlidingWindow<SlidingWindowData>> metricMap) {
        for (MetricFlowUnit metric: metrics) {
            if (metric.getData() != null) {
                consumeFlowUnit(metric, metrics.getClass().getName(), metricMap);
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
    public ResourceFlowUnit operate() {
        List<MetricFlowUnit> cpuUsageMetrics = cpuUsage.getFlowUnits();
        List<MetricFlowUnit> ioTotThroughputMetrics = ioTotThroughput.getFlowUnits();
        List<MetricFlowUnit> ioTotSyscallRateMetrics = ioTotSyscallRate.getFlowUnits();
        counter += 1;

        // Populate the Resource HashMaps
        consumeMetrics(cpuUsageMetrics, cpuUsageHashMap);
        consumeMetrics(ioTotThroughputMetrics, ioTotThroughputHashMap);
        consumeMetrics(ioTotSyscallRateMetrics, ioTotSyscallRateHashMap);

        if (counter == rcaPeriod) {
            ResourceContext context = new ResourceContext(Resources.State.HEALTHY);
            List<HotShardSummary> HotShardSummaryList = new ArrayList<>();
            ClusterDetailsEventProcessor.NodeDetails currentNode = ClusterDetailsEventProcessor.getCurrentNodeDetails();

            Set<IndexShardKey> indexShardKeySet = new HashSet<>(cpuUsageHashMap.keySet());
            indexShardKeySet.addAll(ioTotThroughputHashMap.keySet());
            indexShardKeySet.addAll(ioTotSyscallRateHashMap.keySet());

            for (IndexShardKey indexShardKey : indexShardKeySet) {
                double avgCpuUsage = fetchUsageValueFromMap(cpuUsageHashMap, indexShardKey);
                double avgIoTotThroughput = fetchUsageValueFromMap(ioTotThroughputHashMap, indexShardKey);
                double avgIoTotSyscallRate = fetchUsageValueFromMap(ioTotSyscallRateHashMap, indexShardKey);

                if (avgCpuUsage > CPU_USAGE_THRESHOLD
                        || avgIoTotThroughput > IO_TOT_THROUGHPUT_THRESHOLD_IN_BYTES
                        || avgIoTotSyscallRate > IO_TOT_SYSCALL_RATE_THRESHOLD_PER_SECOND) {
                    HotShardSummaryList.add(new HotShardSummary(
                            indexShardKey.getIndexName(), indexShardKey.getShardId(), currentNode.getId(),
                            avgCpuUsage, CPU_USAGE_THRESHOLD, avgIoTotThroughput, IO_TOT_THROUGHPUT_THRESHOLD_IN_BYTES,
                            avgIoTotSyscallRate, IO_TOT_SYSCALL_RATE_THRESHOLD_PER_SECOND, SLIDING_WINDOW_IN_SECONDS));
                    context = new ResourceContext(Resources.State.UNHEALTHY);
                    LOG.debug("Hot Shard Identified, Shard : {} , avgCpuUsage = {} , avgIoTotThroughput = {}, "
                            + "avgIoTotSyscallRate = {}", indexShardKey, avgCpuUsage, avgIoTotThroughput, avgIoTotSyscallRate);
                }
            }

            // reset the variables
            counter = 0;

            HotNodeSummary summary = new HotNodeSummary(
                    currentNode.getId(), currentNode.getHostAddress(), HotShardSummaryList);

            LOG.debug("High CPU Usage Shard RCA Context :  " + context.toString());
            return new ResourceFlowUnit(this.clock.millis(), context, summary);
        } else {
            LOG.debug("Empty FlowUnit returned for High CPU Usage Shard RCA");
            return new ResourceFlowUnit(this.clock.millis());
        }
    }

  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    LOG.error("rca: {} should be executed from Local", this.getClass().getSimpleName());
  }

}
