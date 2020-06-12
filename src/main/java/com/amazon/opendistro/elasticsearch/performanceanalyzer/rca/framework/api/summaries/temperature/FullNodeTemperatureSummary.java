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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.GeneratedMessageV3;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Field;
import org.jooq.impl.DSL;

public class FullNodeTemperatureSummary extends GenericSummary {
    private static final Logger LOG = LogManager.getLogger(FullNodeTemperatureSummary.class);

    public static final String TABLE_NAME = FullNodeTemperatureSummary.class.getSimpleName();

    /**
     * A node has a temperature profile of its own. The temperature profile of a node is the mean
     * temperature along each dimension.
     */
    private final TemperatureVector temperatureVector;

    /**
     * A node also has the complete list of shards in each dimension, broken down by the
     * different temperature zones.
     */
    private final NodeLevelDimensionalSummary[] nodeDimensionProfiles;

    private final String nodeId;
    private final String hostAddress;

    public FullNodeTemperatureSummary(String nodeId, String hostAddress) {
        this.nodeId = nodeId;
        this.hostAddress = hostAddress;
        this.nodeDimensionProfiles =
                new NodeLevelDimensionalSummary[TemperatureVector.Dimension.values().length];
        this.temperatureVector = new TemperatureVector();
    }

    public TemperatureVector getTemperatureVector() {
        return temperatureVector;
    }

    public List<NodeLevelDimensionalSummary> getNodeDimensionProfiles() {
        return Arrays.asList(nodeDimensionProfiles);
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getHostAddress() {
        return hostAddress;
    }

    public void updateNodeDimensionProfile(NodeLevelDimensionalSummary nodeDimensionProfile) {
        TemperatureVector.Dimension dimension = nodeDimensionProfile.getProfileForDimension();
        this.nodeDimensionProfiles[dimension.ordinal()] = nodeDimensionProfile;
        temperatureVector.updateTemperatureForDimension(dimension, nodeDimensionProfile.getMeanTemperature());
    }

    public List<GenericSummary> getNestedSummaryList() {
        List<GenericSummary> dimensionalSummaries = new ArrayList<>();
        for (NodeLevelDimensionalSummary dimSummary : nodeDimensionProfiles) {
            if (dimSummary != null) {
                dimensionalSummaries.add(dimSummary);
            }
        }
        return dimensionalSummaries;
    }

    @Override
    public <T extends GeneratedMessageV3> T buildSummaryMessage() {
        throw new IllegalStateException("FullNodeTemperatureSummary should not be transported "
                + "over the wire.");
    }

    @Override
    public void buildSummaryMessageAndAddToFlowUnit(FlowUnitMessage.Builder messageBuilder) {
        throw new IllegalStateException("FullNodeTemperatureSummary should not be received over "
                + "the wire.");
    }

    @Override
    public String getTableName() {
        return TABLE_NAME;
    }

    @Override
    public List<Field<?>> getSqlSchema() {
        List<Field<?>> schema = new ArrayList<>();

        schema.add(DSL.field(DSL.name(HotNodeSummary.SQL_SCHEMA_CONSTANTS.NODE_ID_COL_NAME), String.class));
        schema.add(DSL.field(DSL.name(HotNodeSummary.SQL_SCHEMA_CONSTANTS.HOST_IP_ADDRESS_COL_NAME), String.class));

        for (TemperatureVector.Dimension dimension : TemperatureVector.Dimension.values()) {
            schema.add(DSL.field(DSL.name(dimension.NAME), Short.class));
        }
        return schema;
    }

    @Override
    public List<Object> getSqlValue() {
        List<Object> values = new ArrayList<>();

        values.add(getNodeId());
        values.add(getHostAddress());

        for (TemperatureVector.Dimension dimension : TemperatureVector.Dimension.values()) {
            values.add(temperatureVector.getTemperatureFor(dimension));
        }
        return values;
    }

    @Override
    public JsonElement toJson() {
        JsonObject summaryObj = new JsonObject();
        for (TemperatureVector.Dimension dimension : TemperatureVector.Dimension.values()) {
            TemperatureVector.NormalizedValue value =
                    temperatureVector.getTemperatureFor(dimension);
            summaryObj.addProperty(dimension.NAME,
                    value != null ? value.getPOINTS() : null);
        }
        getNestedSummaryList().forEach(
                summary -> {
                    summaryObj.add(summary.getTableName(), summary.toJson());
                }
        );
        return summaryObj;
    }
}
