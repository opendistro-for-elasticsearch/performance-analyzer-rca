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
import java.util.Arrays;
import java.util.List;
import org.jooq.Field;
import org.jooq.impl.DSL;

public class FullNodeTemperatureSummary extends GenericSummary {
    /**
     * A node has a temperature profile of its own. The temperature profile of a node is the mean
     * temperature along each dimension.
     */
    private TemperatureVector temperatureVector;

    /**
     * A node also has the complete list of shards in each dimension, broken down by the
     * different temperature zones.
     */
    private DimensionalTemperatureSummary[] nodeDimensionProfiles;

    private final String nodeId;
    private final String hostAddress;

    public FullNodeTemperatureSummary(String nodeId, String hostAddress) {
        this.nodeId = nodeId;
        this.hostAddress = hostAddress;
        this.nodeDimensionProfiles =
                new DimensionalTemperatureSummary[TemperatureVector.Dimension.values().length];
        this.temperatureVector = new TemperatureVector();
    }

    public TemperatureVector getTemperatureVector() {
        return temperatureVector;
    }

    public List<DimensionalTemperatureSummary> getNodeDimensionProfiles() {
        return Arrays.asList(nodeDimensionProfiles);
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHostAddress() {
        return hostAddress;
    }

    public void updateNodeDimensionProfile(DimensionalTemperatureSummary nodeDimensionProfile) {
        TemperatureVector.Dimension dimension = nodeDimensionProfile.getProfileForDimension();
        this.nodeDimensionProfiles[dimension.ordinal()] = nodeDimensionProfile;
        temperatureVector.updateTemperatureForDimension(dimension, nodeDimensionProfile.getMeanTemperature());
    }

    public List<GenericSummary> getNestedSummaryList() {
        List<GenericSummary> dimensionalSummaries = new ArrayList<>();
        for (DimensionalTemperatureSummary dimSummary : nodeDimensionProfiles) {
            dimensionalSummaries.add(dimSummary);
        }
        return dimensionalSummaries;
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
        for (TemperatureVector.Dimension dimension: TemperatureVector.Dimension.values()) {
            schema.add(DSL.field(DSL.name(dimension.NAME), Short.class));
        }
        return schema;
    }

    @Override
    public List<Object> getSqlValue() {
        List<Object> values = new ArrayList<>();
        for (TemperatureVector.Dimension dimension: TemperatureVector.Dimension.values()) {
            values.add(temperatureVector.getTemperatureFor(dimension));
        }
        return values;
    }

    @Override
    public JsonElement toJson() {
        JsonObject summaryObj = new JsonObject();
        for (TemperatureVector.Dimension dimension: TemperatureVector.Dimension.values()) {
            summaryObj.addProperty(dimension.NAME,
                    temperatureVector.getTemperatureFor(dimension).getPOINTS());
        }
        getNestedSummaryList().forEach(
                summary -> {
                    summaryObj.add(summary.getTableName(), summary.toJson());
                }
        );
        return summaryObj;
    }
}
