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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.HardwareEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.HotShardClusterRcaConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotShardSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This RCA is used to find hot shards per index in a cluster using the HotShardSummary
 * sent from each node via 'HotShardRca'. If the resource utilization is (threshold)%
 * higher than the mean resource utilization for the index, we declare the shard hot.
 *
 */
public class HotShardClusterRca extends Rca<ResourceFlowUnit> {

    private static final Logger LOG = LogManager.getLogger(HotShardClusterRca.class);
    private static final int SLIDING_WINDOW_IN_SECONDS = 60;

    private double cpuUtilizationClusterThreshold;
    private double ioTotThroughputClusterThreshold;
    private double ioTotSysCallRateClusterThreshold;

    private final Rca<ResourceFlowUnit> hotShardRca;
    private int rcaPeriod;
    private int counter;

    // Guava Table with Row: 'Index_Name', Column: 'NodeShardKey', Cell Value: 'Value'
    private Table<String, NodeShardKey, Double> cpuUtilizationInfoTable;
    private Table<String, NodeShardKey, Double> IOThroughputInfoTable;
    private Table<String, NodeShardKey, Double> IOSysCallRateInfoTable;

    public <R extends Rca> HotShardClusterRca(final int rcaPeriod, final R hotShardRca) {
        super(5);
        this.hotShardRca = hotShardRca;
        this.rcaPeriod = rcaPeriod;
        this.counter = 0;
        this.cpuUtilizationInfoTable = HashBasedTable.create();
        this.IOThroughputInfoTable = HashBasedTable.create();
        this.IOSysCallRateInfoTable = HashBasedTable.create();
        this.cpuUtilizationClusterThreshold = HotShardClusterRcaConfig.DEFAULT_CPU_UTILIZATION_CLUSTER_THRESHOLD;
        this.ioTotThroughputClusterThreshold = HotShardClusterRcaConfig.DEFAULT_IO_TOTAL_THROUGHPUT_CLUSTER_THRESHOLD;
        this.ioTotSysCallRateClusterThreshold = HotShardClusterRcaConfig.DEFAULT_IO_TOTAL_SYSCALL_RATE_CLUSTER_THRESHOLD;
    }

    private void populateResourceInfoTable(String indexName, NodeShardKey nodeShardKey,
                                           double metricValue, Table<String, NodeShardKey, Double> metricMap) {
        if (null == metricMap.get(indexName, nodeShardKey)) {
            metricMap.put(indexName, nodeShardKey, metricValue);
        } else {
            double existingOccurence = metricMap.get(indexName, nodeShardKey);
            metricMap.put(indexName, nodeShardKey, existingOccurence + metricValue);
        }
    }

    private void consumeFlowUnit(ResourceFlowUnit resourceFlowUnit) {
        String nodeId = ((HotNodeSummary) resourceFlowUnit.getResourceSummary()).getNodeID();
        HotNodeSummary hotNodeSummary = ((HotNodeSummary) resourceFlowUnit.getResourceSummary());
        for (HotShardSummary hotShardSummary : hotNodeSummary.getHotShardSummaryList()) {
            String indexName = hotShardSummary.getIndexName();
            NodeShardKey nodeShardKey = new NodeShardKey(nodeId, hotShardSummary.getShardId());
            // 1. Populate CPU Table
            populateResourceInfoTable(indexName, nodeShardKey, hotShardSummary.getCpuUtilization(), cpuUtilizationInfoTable);

            // 2. Populate ioTotThroughput Table
            populateResourceInfoTable(indexName, nodeShardKey, hotShardSummary.getIOThroughput(), IOThroughputInfoTable);

            // 3. Populate ioTotSysCallrate Table
            populateResourceInfoTable(indexName,nodeShardKey, hotShardSummary.getIOSysCallrate(), IOSysCallRateInfoTable);
        }
    }

    /**
     * Evaluates the threshold value for resource usage across shards for given index.
     * @param perIndexShardInfo Resource usage across shards for given index
     * @param thresholdInPercentage Threshold for the resource in percentage
     *
     */
    private double getThresholdValue(Map<NodeShardKey, Double> perIndexShardInfo, double thresholdInPercentage) {
        // To handle the outlier(s) in the data, using median instead of mean
        double[] perIndexShardUsage = perIndexShardInfo.values().stream().mapToDouble(usage -> usage).toArray();
        Arrays.sort(perIndexShardUsage);

        double median;
        int length = perIndexShardUsage.length;
        if (length % 2 != 0) {
            median = perIndexShardUsage[length / 2];
        } else {
            median = (perIndexShardUsage[(length - 1) / 2] + perIndexShardUsage[length / 2]) / 2.0;
        }
        return (median * (1 + thresholdInPercentage));
    }

    /**
     * Finds hot shard(s) across an index and creates HotResourceSummary for them.
     * @param resourceInfoTable Guava Table with 'Index_Name', 'NodeShardKey' and 'UsageValue'
     * @param thresholdInPercentage Threshold for the resource in percentage
     * @param hotResourceSummaryList Summary List for hot shards
     * @param resourceType Resource Type
     *
     */
    private void findHotShardAndCreateSummary(Table<String, NodeShardKey, Double> resourceInfoTable, double thresholdInPercentage,
                                              List<GenericSummary> hotResourceSummaryList, ResourceType resourceType) {
        for (String indexName : resourceInfoTable.rowKeySet()) {
            Map<NodeShardKey, Double> perIndexShardInfo = resourceInfoTable.row(indexName);
            double thresholdValue = getThresholdValue(perIndexShardInfo, thresholdInPercentage);
            for (Map.Entry<NodeShardKey, Double> shardInfo : perIndexShardInfo.entrySet()) {
                if (shardInfo.getValue() > thresholdValue) {
                    // Shard Identifier is represented by "Node_ID Index_Name Shard_ID" string
                    String shardIdentifier =  String.join(" ", new String[]
                            { shardInfo.getKey().getNodeId(), indexName, shardInfo.getKey().getShardId() });

                    // Add to hotResourceSummaryList
                    hotResourceSummaryList.add(new HotResourceSummary(resourceType, thresholdValue,
                            shardInfo.getValue(), SLIDING_WINDOW_IN_SECONDS, shardIdentifier));
                }
            }
        }
    }

    /**
     * Compare between the shard counterparts. Within an index, the shard which
     * is (threshold)% higher than the mean resource utilization is hot.
     *
     * <p>We are evaluating hot shards on 3 dimensions and if shard is hot in any of
     * the 3 dimension, we declare it hot.
     */
    @Override
    public ResourceFlowUnit operate() {
        counter++;

        // Populate the Table, compiling the information per index
        final List<ResourceFlowUnit> resourceFlowUnits = hotShardRca.getFlowUnits();
        for (final ResourceFlowUnit resourceFlowUnit : resourceFlowUnits) {
            if (resourceFlowUnit.isEmpty()) {
                continue;
            }

            if (resourceFlowUnit.getResourceContext().isUnhealthy()) {
                consumeFlowUnit(resourceFlowUnit);
            }
        }

        if (counter >= rcaPeriod) {
            List<GenericSummary> hotShardSummaryList = new ArrayList<>();
            ResourceContext context;
            HotClusterSummary summary = new HotClusterSummary(
                    ClusterDetailsEventProcessor.getNodesDetails().size(), 0);

            // We evaluate hot shards individually on all the 3 dimensions
            findHotShardAndCreateSummary(
                    cpuUtilizationInfoTable, cpuUtilizationClusterThreshold, hotShardSummaryList,
                    ResourceType.newBuilder().setHardwareResourceTypeValue(HardwareEnum.CPU_VALUE).build());

            findHotShardAndCreateSummary(
                    IOThroughputInfoTable, ioTotThroughputClusterThreshold, hotShardSummaryList,
                    ResourceType.newBuilder().setHardwareResourceTypeValue(HardwareEnum.IO_TOTAL_THROUGHPUT_VALUE).build());

            findHotShardAndCreateSummary(
                    IOSysCallRateInfoTable, ioTotSysCallRateClusterThreshold, hotShardSummaryList,
                    ResourceType.newBuilder().setHardwareResourceTypeValue(HardwareEnum.IO_TOTAL_SYS_CALLRATE_VALUE).build());

            if (hotShardSummaryList.isEmpty()) {
                context = new ResourceContext(Resources.State.HEALTHY);
            } else {
                context = new ResourceContext(Resources.State.UNHEALTHY);
                summary.addNestedSummaryList(hotShardSummaryList);
                LOG.debug("rca: Hot Shards Identified: {}", hotShardSummaryList);
            }

            // reset the variables
            counter = 0;
            this.cpuUtilizationInfoTable.clear();
            this.IOThroughputInfoTable.clear();
            this.IOSysCallRateInfoTable.clear();
            LOG.debug("Hot Shard Cluster RCA Context :  " + context.toString());
            return new ResourceFlowUnit(System.currentTimeMillis(), context, summary);
        } else {
            LOG.debug("Empty FlowUnit returned for Hot Shard CLuster RCA");
            return new ResourceFlowUnit(System.currentTimeMillis());
        }
    }

    /**
     * read threshold values from rca.conf
     * @param conf RcaConf object
     */
    @Override
    public void readRcaConf(RcaConf conf) {
        HotShardClusterRcaConfig configObj = conf.getHotShardClusterRcaConfig();
        cpuUtilizationClusterThreshold = configObj.getCpuUtilizationClusterThreshold();
        ioTotThroughputClusterThreshold = configObj.getIoTotThroughputClusterThreshold();
        ioTotSysCallRateClusterThreshold = configObj.getIoTotSysCallRateClusterThreshold();
    }

    /**
     * This is a local node RCA which by definition can not be serialize/de-serialized
     * over gRPC.
     */
    @Override
    public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
        final List<FlowUnitMessage> flowUnitMessages =
                args.getWireHopper().readFromWire(args.getNode());
        List<ResourceFlowUnit> flowUnitList = new ArrayList<>();
        LOG.debug("rca: Executing fromWire: {}", this.getClass().getSimpleName());
        for (FlowUnitMessage flowUnitMessage : flowUnitMessages) {
            flowUnitList.add(ResourceFlowUnit.buildFlowUnitFromWrapper(flowUnitMessage));
        }
        setFlowUnits(flowUnitList);
    }
}
