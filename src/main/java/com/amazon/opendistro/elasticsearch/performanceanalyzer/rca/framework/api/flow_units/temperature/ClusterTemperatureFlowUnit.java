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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.ClusterTemperatureSummary;

public class ClusterTemperatureFlowUnit extends ResourceFlowUnit {
    private final ClusterTemperatureSummary clusterTemperatureSummary;

    public ClusterTemperatureFlowUnit(long timeStamp, ResourceContext context,
                                      ClusterTemperatureSummary resourceSummary) {
        super(timeStamp, context, resourceSummary, true);
        clusterTemperatureSummary = resourceSummary;
    }

    @Override
    public FlowUnitMessage buildFlowUnitMessage(String graphNode, String esNode) {
        throw new IllegalStateException(this.getClass().getSimpleName() + " should not be passed "
                + "over the wire.");
    }
}
