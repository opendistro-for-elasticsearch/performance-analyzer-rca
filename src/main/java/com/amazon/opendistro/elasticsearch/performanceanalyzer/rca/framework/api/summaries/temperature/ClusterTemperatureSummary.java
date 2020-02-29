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
import java.util.Collection;
import java.util.List;
import org.jooq.Field;
import org.jooq.impl.DSL;

public class ClusterTemperatureSummary extends GenericSummary {
    private List<CompactNodeTemperatureSummary> nodesSummaries;
    private TemperatureVector temperatureVector;
    private int numberOfNodes;
    private double[] totalUsageByDimension;

    public ClusterTemperatureSummary(int numberOfNodes) {
        nodesSummaries = new ArrayList<>();
        temperatureVector = new TemperatureVector();
        totalUsageByDimension = new double[TemperatureVector.Dimension.values().length];
        this.numberOfNodes = numberOfNodes;
    }

    public void setTemperatureByDimension(TemperatureVector.Dimension dimension,
                                          TemperatureVector.NormalizedValue value,
                                          double totalUsage) {
        temperatureVector.updateTemperatureForDimension(dimension, value);
        totalUsageByDimension[dimension.ordinal()] = totalUsage;
    }

    public void addNodesSummaries(Collection<CompactNodeTemperatureSummary> nodeTemperatureSummaries) {
        nodesSummaries.addAll(nodeTemperatureSummaries);
    }

    public List<GenericSummary> getNestedSummaryList() {
        List<GenericSummary> summaries = new ArrayList<>();
        for (GenericSummary summary: nodesSummaries) {
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
        schema.add(DSL.field(DSL.name("node_count"), Integer.class));
        for (TemperatureVector.Dimension dimension : TemperatureVector.Dimension.values()) {
            schema.add(DSL.field(DSL.name(dimension.NAME + "_mean"), Short.class));
            schema.add(DSL.field(DSL.name(dimension.NAME + "_total"), Double.class));
        }
        return schema;
    }

    @Override
    public List<Object> getSqlValue() {
        List<Object> values = new ArrayList<>();
        values.add(numberOfNodes);
        for (TemperatureVector.Dimension dimension : TemperatureVector.Dimension.values()) {
            TemperatureVector.NormalizedValue normalizedValue =
                    temperatureVector.getTemperatureFor(dimension);
            if (normalizedValue == null) {
                values.add(null);  // null for mean
                values.add(null);  // null for total
            } else {
                values.add(temperatureVector.getTemperatureFor(dimension).getPOINTS());
                values.add(totalUsageByDimension[dimension.ordinal()]);
            }
        }
        return values;
    }

    @Override
    public JsonElement toJson() {
        JsonObject summaryObj = new JsonObject();
        summaryObj.add("temperature", temperatureVector.toJson());
        getNestedSummaryList().forEach(
                summary -> {
                    summaryObj.add(summary.getTableName(), summary.toJson());
                }
        );
        return summaryObj;
    }
}
