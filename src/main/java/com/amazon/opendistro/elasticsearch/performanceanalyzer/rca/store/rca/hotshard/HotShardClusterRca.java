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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotShardSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This RCA is used to find hot shards per index in a cluster using the HotShardSummary
 * sent from each node via 'HighCPUShardRca'. If the resource utilization is (threshold)%
 * higher than the mean resource utilization for the index, we declare the shard hot.
 *
 */
public class HotShardClusterRca extends Rca<ResourceFlowUnit> {

    private static final Logger LOG = LogManager.getLogger(HotShardClusterRca.class);
    private static final double CPU_USAGE_DEFAULT_THRESHOLD_IN_PERCENTAGE = 0.3;
    private static final double IO_THROUGHPUT_DEFAULT_THRESHOLD_IN_PERCENTAGE = 0.3;
    private static final double IO_SYSCALLRATE_DEFAULT_THRESHOLD_IN_PERCENTAGE = 0.3;
    private static final int SLIDING_WINDOW_IN_SECONDS = 60;
    private final Rca<ResourceFlowUnit> hotNodeRca;
    private int rcaPeriod;
    private int counter;

    // Guava Table with Row: 'Index_Name', Column: 'NodeShardKey', Cell Value: 'Value'
    private Table<String, NodeShardKey, Double> cpuUsageInfoTable;
    private Table<String, NodeShardKey, Double> IOThroughputInfoTable;
    private Table<String, NodeShardKey, Double> IOSysCallRateInfoTable;

    public <R extends Rca> HotShardClusterRca(final int rcaPeriod, final R hotNodeRca) {
        super(5);
        this.hotNodeRca = hotNodeRca;
        this.rcaPeriod = rcaPeriod;
        this.counter = 0;
        this.cpuUsageInfoTable = HashBasedTable.create();
        this.IOThroughputInfoTable = HashBasedTable.create();
        this.IOSysCallRateInfoTable = HashBasedTable.create();
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
            populateResourceInfoTable(indexName, nodeShardKey, hotShardSummary.getCpuUsage(), cpuUsageInfoTable);

            // 2. Populate ioTotThroughput Table
            populateResourceInfoTable(indexName, nodeShardKey, hotShardSummary.getIOThroughput(), IOThroughputInfoTable);

            // 3. Populate ioTotSysCallrate Table
            populateResourceInfoTable(indexName,nodeShardKey, hotShardSummary.getCpuUsage(), IOSysCallRateInfoTable);
        }
    }

    private double getThresholdValue(Map<NodeShardKey, Double> perIndexShardInfo, double thresholdInPercentage) {
        OptionalDouble average = perIndexShardInfo.values().stream().mapToDouble(usage -> usage).average();
        if (average.isPresent()) {
            return (average.getAsDouble() * (1 + thresholdInPercentage));
        }
        return Double.NaN;
    }

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
        final List<ResourceFlowUnit> resourceFlowUnits = hotNodeRca.getFlowUnits();
        for (final ResourceFlowUnit resourceFlowUnit : resourceFlowUnits) {
            if (resourceFlowUnit.isEmpty()) {
                continue;
            }

            if (resourceFlowUnit.getResourceContext().isUnhealthy()) {
                consumeFlowUnit(resourceFlowUnit);
            }
        }

        if (counter == rcaPeriod) {
            List<GenericSummary> hotShardSummaryList = new ArrayList<>();
            ResourceContext context;
            HotClusterSummary summary = new HotClusterSummary(
                    ClusterDetailsEventProcessor.getNodesDetails().size(), 0);

            findHotShardAndCreateSummary(
                    cpuUsageInfoTable, CPU_USAGE_DEFAULT_THRESHOLD_IN_PERCENTAGE, hotShardSummaryList,
                    ResourceType.newBuilder().setHardwareResourceTypeValue(HardwareEnum.CPU_VALUE).build());

            findHotShardAndCreateSummary(
                    IOThroughputInfoTable, IO_THROUGHPUT_DEFAULT_THRESHOLD_IN_PERCENTAGE, hotShardSummaryList,
                    ResourceType.newBuilder().setHardwareResourceTypeValue(HardwareEnum.IO_TOTAL_THROUGHPUT_VALUE).build());

            findHotShardAndCreateSummary(
                    IOSysCallRateInfoTable, IO_SYSCALLRATE_DEFAULT_THRESHOLD_IN_PERCENTAGE, hotShardSummaryList,
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
            this.cpuUsageInfoTable.clear();
            this.IOThroughputInfoTable.clear();
            this.IOSysCallRateInfoTable.clear();
            return new ResourceFlowUnit(System.currentTimeMillis(), context, summary);
        } else {
            return new ResourceFlowUnit(System.currentTimeMillis());
        }
    }

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
