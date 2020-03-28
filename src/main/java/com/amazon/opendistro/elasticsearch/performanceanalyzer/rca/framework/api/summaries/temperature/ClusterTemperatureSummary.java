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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.GeneratedMessageV3;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jooq.Field;
import org.jooq.impl.DSL;

public class ClusterTemperatureSummary extends GenericSummary {
    ClusterDimensionalTemperatureSummary[] nodeDimensionalTemperatureSummaries;

    private int numberOfNodes;

    public ClusterTemperatureSummary(int numberOfNodes) {
        this.numberOfNodes = numberOfNodes;
        this.nodeDimensionalTemperatureSummaries =
                new ClusterDimensionalTemperatureSummary[TemperatureVector.Dimension.values().length];
    }

    public void createClusterDimensionalTemperature(TemperatureVector.Dimension dimension,
                                                    TemperatureVector.NormalizedValue mean,
                                                    double totalUsage) {
        ClusterDimensionalTemperatureSummary summary = new ClusterDimensionalTemperatureSummary(dimension);
        summary.setMeanTemperature(mean);
        summary.setTotalUsage(totalUsage);
        nodeDimensionalTemperatureSummaries[dimension.ordinal()] = summary;
    }

    public void addNodesSummaries(Map<String, CompactNodeTemperatureSummary> nodeTemperatureSummaries) {
        for (CompactNodeTemperatureSummary nodeTemperatureSummaryVal : nodeTemperatureSummaries.values()) {
            for (TemperatureVector.Dimension dimension : TemperatureVector.Dimension.values()) {
                nodeDimensionalTemperatureSummaries[dimension.ordinal()].addNodeToZone(nodeTemperatureSummaryVal);
            }
        }
    }

    public List<GenericSummary> getNestedSummaryList() {
        List<GenericSummary> summaries = new ArrayList<>();
        for (GenericSummary summary : nodeDimensionalTemperatureSummaries) {
            summaries.add(summary);
        }
        return summaries;
    }

    @Override
    public <T extends GeneratedMessageV3> T buildSummaryMessage() {
        throw new IllegalArgumentException();
    }

    @Override
    public void buildSummaryMessageAndAddToFlowUnit(FlowUnitMessage.Builder messageBuilder) {
        throw new IllegalArgumentException();
    }

    @Override
    public String getTableName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public List<Field<?>> getSqlSchema() {
        List<Field<?>> schema = new ArrayList<>();
        schema.add(DSL.field(DSL.name("num_nodes"), Short.class));
        return schema;
    }

    @Override
    public List<Object> getSqlValue() {
        List<Object> values = new ArrayList<>();
        values.add(numberOfNodes);
        return values;
    }

    @Override
    public JsonElement toJson() {
        JsonObject summaryObj = new JsonObject();
        getNestedSummaryList().forEach(
                summary -> {
                    summaryObj.add(summary.getTableName(), summary.toJson());
                }
        );
        return summaryObj;
    }
}
