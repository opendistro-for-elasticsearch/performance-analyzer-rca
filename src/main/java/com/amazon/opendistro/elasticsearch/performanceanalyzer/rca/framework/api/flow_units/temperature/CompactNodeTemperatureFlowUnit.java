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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.CompactNodeTemperatureSummary;

public class CompactNodeTemperatureFlowUnit extends ResourceFlowUnit {
    private final CompactNodeTemperatureSummary compactNodeTemperatureSummary;

    public CompactNodeTemperatureFlowUnit(long timeStamp, ResourceContext context,
                                          CompactNodeTemperatureSummary resourceSummary,
                                          boolean persistSummary) {
        super(timeStamp, context, resourceSummary, persistSummary);
        this.compactNodeTemperatureSummary = resourceSummary;
    }

    @Override
    public FlowUnitMessage buildFlowUnitMessage(String graphNode, String esNode) {
        FlowUnitMessage.Builder builder = FlowUnitMessage.newBuilder();
        builder.setGraphNode(graphNode);
        builder.setEsNode(esNode);
        builder.setTimeStamp(System.currentTimeMillis());
        compactNodeTemperatureSummary.buildSummaryMessageAndAddToFlowUnit(builder);
        return builder.build();
    }

    public static CompactNodeTemperatureFlowUnit buildFlowUnitFromWrapper(final FlowUnitMessage message) {
        CompactNodeTemperatureSummary compactNodeTemperatureSummary =
                CompactNodeTemperatureSummary.buildNodeTemperatureProfileFromMessage(message.getNodeTemperatureSummary());
        return new CompactNodeTemperatureFlowUnit(message.getTimeStamp(), new ResourceContext(Resources.State.UNKNOWN),
                compactNodeTemperatureSummary, false);
    }

    public CompactNodeTemperatureSummary getCompactNodeTemperatureSummary() {
        return compactNodeTemperatureSummary;
    }

}
