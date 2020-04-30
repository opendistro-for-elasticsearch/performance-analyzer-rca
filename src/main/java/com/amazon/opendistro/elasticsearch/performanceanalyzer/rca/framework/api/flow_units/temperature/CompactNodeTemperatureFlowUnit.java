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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.temperature;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.CompactNodeSummary;

/**
 * This is a grpc wrapper on top of the CompactNodeTemperatureSummary. The flow unit is passed
 * around from the data nodes to the master. Its compact, because it does not contain the
 * temperatures at the granularity of shards. As, some of our largest instances can have multiple
 * shards, it would be sending too many bytes over the wire.
 */
public class CompactNodeTemperatureFlowUnit extends ResourceFlowUnit {
    private final CompactNodeSummary compactNodeTemperatureSummary;

    public CompactNodeTemperatureFlowUnit(long timeStamp, ResourceContext context,
                                          CompactNodeSummary resourceSummary,
                                          boolean persistSummary) {
        super(timeStamp, context, resourceSummary, persistSummary);
        this.compactNodeTemperatureSummary = resourceSummary;
    }

    public CompactNodeTemperatureFlowUnit(long timeStamp) {
        super(timeStamp);
        compactNodeTemperatureSummary = null;
    }

    @Override
    public FlowUnitMessage buildFlowUnitMessage(String graphNode, String esNode) {
        FlowUnitMessage.Builder builder = FlowUnitMessage.newBuilder();
        builder.setGraphNode(graphNode);
        builder.setEsNode(esNode);
        builder.setTimeStamp(System.currentTimeMillis());
        if (compactNodeTemperatureSummary != null) {
            compactNodeTemperatureSummary.buildSummaryMessageAndAddToFlowUnit(builder);
        }
        return builder.build();
    }

    public static CompactNodeTemperatureFlowUnit buildFlowUnitFromWrapper(final FlowUnitMessage message) {
        CompactNodeSummary compactNodeTemperatureSummary =
                CompactNodeSummary.buildNodeTemperatureProfileFromMessage(message.getNodeTemperatureSummary());
        return new CompactNodeTemperatureFlowUnit(message.getTimeStamp(), new ResourceContext(Resources.State.UNKNOWN),
                compactNodeTemperatureSummary, false);
    }

    public CompactNodeSummary getCompactNodeTemperatureSummary() {
        return compactNodeTemperatureSummary;
    }

}
