/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.temperature.ClusterTemperatureFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.temperature.CompactNodeTemperatureFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.ClusterTemperatureSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.CompactClusterLevelNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.CompactNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterTemperatureRca extends Rca<ClusterTemperatureFlowUnit> {
    private final NodeTemperatureRca nodeTemperatureRca;
    public static final String TABLE_NAME = ClusterTemperatureRca.class.getSimpleName();

    public ClusterTemperatureRca(NodeTemperatureRca nodeTemperatureRca) {
        super(5);
        this.nodeTemperatureRca = nodeTemperatureRca;
    }

    @Override
    public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
        throw new IllegalArgumentException(name() + "'s generateFlowUnitListFromWire() should not "
                + "be required.");
    }

    /**
     * @return
     */
    @Override
    public ClusterTemperatureFlowUnit operate() {
        List<CompactNodeTemperatureFlowUnit> flowUnits = nodeTemperatureRca.getFlowUnits();
        Map<String, CompactClusterLevelNodeSummary> nodeTemperatureSummaryMap =
                new HashMap<>();
        final int NUM_NODES = flowUnits.size();

        ClusterTemperatureSummary clusterTemperatureSummary = new ClusterTemperatureSummary(NUM_NODES);

        // For each dimension go through the temperature profiles sent by each node and figure
        // out the cluster level average along that dimension. Then this method recreates a
        // temperature profile for each of the nodes. This is required because what nodes sent was
        // respect to the nodes but at cluster level they need to be re-calibrated at the cluster
        // level. Note that temperature is a normalized value, normalized by total usage. At node
        // level, the total usage is at the node level (over all shards and shard-independent
        // factors), at the master the total usage is the sum over all nodes.
        for (TemperatureVector.Dimension dimension : TemperatureVector.Dimension.values()) {
            double totalForDimension = 0.0;
            for (CompactNodeTemperatureFlowUnit nodeFlowUnit : flowUnits) {
                CompactNodeSummary summary = nodeFlowUnit.getCompactNodeTemperatureSummary();
                totalForDimension += summary.getTotalConsumedByDimension(dimension);
            }
            double nodeAverageForDimension = totalForDimension / NUM_NODES;
            TemperatureVector.NormalizedValue normalizedAvgForDimension =
                    TemperatureVector.NormalizedValue.calculate(nodeAverageForDimension, totalForDimension);

            clusterTemperatureSummary.createClusterDimensionalTemperature(dimension,
                    normalizedAvgForDimension, totalForDimension);

            recalibrateNodeTemperaturesAtClusterLevelUsage(flowUnits, nodeTemperatureSummaryMap,
                    dimension, totalForDimension, nodeAverageForDimension);
        }
        clusterTemperatureSummary.addNodesSummaries(nodeTemperatureSummaryMap);
        return new ClusterTemperatureFlowUnit(System.currentTimeMillis(),
                new ResourceContext(Resources.State.UNKNOWN), clusterTemperatureSummary);
    }

    private void recalibrateNodeTemperaturesAtClusterLevelUsage(List<CompactNodeTemperatureFlowUnit> flowUnits,
                                                                Map<String,
                                                                        CompactClusterLevelNodeSummary> nodeTemperatureSummaryMap,
                                                                TemperatureVector.Dimension dimension,
                                                                double totalForDimension,
                                                                double avgForTheDimension) {
        for (CompactNodeTemperatureFlowUnit nodeFlowUnit : flowUnits) {
            CompactNodeSummary obtainedNodeTempSummary =
                    nodeFlowUnit.getCompactNodeTemperatureSummary();
            String key = obtainedNodeTempSummary.getNodeId();

            nodeTemperatureSummaryMap.putIfAbsent(key,
                    new CompactClusterLevelNodeSummary(obtainedNodeTempSummary.getNodeId(),
                            obtainedNodeTempSummary.getHostAddress()));
            CompactClusterLevelNodeSummary constructedCompactNodeTemperatureSummary =
                    nodeTemperatureSummaryMap.get(key);

            double obtainedTotal = obtainedNodeTempSummary.getTotalConsumedByDimension(dimension);
            TemperatureVector.NormalizedValue newClusterBasedValue =
                    TemperatureVector.NormalizedValue.calculate(obtainedTotal, totalForDimension);

            constructedCompactNodeTemperatureSummary.setTemperatureForDimension(dimension, newClusterBasedValue);
            constructedCompactNodeTemperatureSummary.setNumOfShards(dimension,
                    obtainedNodeTempSummary.getNumberOfShardsByDimension(dimension));
            constructedCompactNodeTemperatureSummary.setTotalConsumedByDimension(dimension, obtainedTotal);
        }
    }
}
