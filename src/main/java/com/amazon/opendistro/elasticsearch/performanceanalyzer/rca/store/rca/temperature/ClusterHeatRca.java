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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.temperature.ClusterTemperatureFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.temperature.CompactNodeTemperatureFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.ClusterTemperatureSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.CompactNodeTemperatureSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClusterHeatRca extends Rca<ResourceFlowUnit> {
    private final NodeHeatRca nodeHeatRca;

    public ClusterHeatRca(NodeHeatRca nodeHeatRca) {
        super(5);
        this.nodeHeatRca = nodeHeatRca;
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
    public ResourceFlowUnit operate() {
        List<CompactNodeTemperatureFlowUnit> flowUnits = nodeHeatRca.getFlowUnits();
        Map<String, CompactNodeTemperatureSummary> nodeTemperatureSummaryMap = new HashMap<>();
        final int NUM_NODES = flowUnits.size();

        ClusterTemperatureSummary clusterTemperatureSummary = new ClusterTemperatureSummary(NUM_NODES);
        for (TemperatureVector.Dimension dimension : TemperatureVector.Dimension.values()) {
            double totalForDimension = 0.0;
            for (CompactNodeTemperatureFlowUnit nodeFlowUnit : flowUnits) {
                CompactNodeTemperatureSummary summary = nodeFlowUnit.getCompactNodeTemperatureSummary();
                totalForDimension += summary.getTotalConsumedByDimension(dimension);
            }
            double nodeAverageForDimension = totalForDimension / NUM_NODES;
            TemperatureVector.NormalizedValue value =
                    TemperatureVector.NormalizedValue.calculate(nodeAverageForDimension, totalForDimension);
            clusterTemperatureSummary.setTemperatureByDimension(dimension, value, totalForDimension);

            for (CompactNodeTemperatureFlowUnit nodeFlowUnit : flowUnits) {
                CompactNodeTemperatureSummary obtainedNodeTempSummary =
                        nodeFlowUnit.getCompactNodeTemperatureSummary();
                String key = obtainedNodeTempSummary.getNodeId();

                nodeTemperatureSummaryMap.putIfAbsent(key,
                        new CompactNodeTemperatureSummary(obtainedNodeTempSummary.getNodeId(),
                                obtainedNodeTempSummary.getHostAddress(), CompactNodeTemperatureSummary.Level.MASTER));
                CompactNodeTemperatureSummary constructedCompactNodeTemperatureSummary = nodeTemperatureSummaryMap.get(key);

                double obtainedTotal = obtainedNodeTempSummary.getTotalConsumedByDimension(dimension);
                TemperatureVector.NormalizedValue newClusterBasedValue =
                        TemperatureVector.NormalizedValue.calculate(obtainedTotal, totalForDimension);

                constructedCompactNodeTemperatureSummary.setTemperatureForDimension(dimension, newClusterBasedValue);
                constructedCompactNodeTemperatureSummary.setNumOfShards(dimension,
                        obtainedNodeTempSummary.getNumberOfShardsByDimension(dimension));
                constructedCompactNodeTemperatureSummary.setTotalConsumedByDimension(dimension, obtainedTotal);
            }
        }
        clusterTemperatureSummary.addNodesSummaries(nodeTemperatureSummaryMap.values());
        return new ClusterTemperatureFlowUnit(System.currentTimeMillis(),
                new ResourceContext(Resources.State.UNKNOWN), clusterTemperatureSummary);
    }
}
