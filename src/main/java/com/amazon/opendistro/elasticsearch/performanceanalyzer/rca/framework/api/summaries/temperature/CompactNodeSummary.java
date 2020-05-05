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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.NodeTemperatureSummaryMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceTemperatureMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector.NormalizedValue;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.jooq.Field;
import org.jooq.impl.DSL;

/**
 * This is created at two places - on each node and again on the cluster. On the node we have the
 * full resolution with dimensions and zones in each dimensions and then shards in zones. But on
 * the elected master we only have the temperature of a node and not zone or shard level
 * information. So, this summary is kept generic so that both can use it.
 */
public class CompactNodeSummary extends GenericSummary {
    /**
     * This will determine the name of the SQLite when this summary is persisted.
     */
    public final String TABLE_NAME = CompactNodeSummary.class.getSimpleName();

    protected final String nodeId;
    protected final String hostAddress;

    protected TemperatureVector temperatureVector;

    /**
     * This is the actual usage. temperature vector is a normalized value and without the
     * context of total used, it means nothing.
     */
    protected double totalConsumedByDimension[];
    protected int numOfShards[];


    public static final String MEAN_SUFFIX_KEY = "_mean";
    public static final String TOTAL_SUFFIX_KEY = "_total";
    public static final String NUM_SHARDS_SUFFIX_KEY = "_num_shards";

    public CompactNodeSummary(final String nodeId, final String hostAddress) {
        super();
        this.nodeId = nodeId;
        this.hostAddress = hostAddress;
        this.temperatureVector = new TemperatureVector();
        this.totalConsumedByDimension = new double[TemperatureVector.Dimension.values().length];
        this.numOfShards = new int[TemperatureVector.Dimension.values().length];
    }

    public void fillFromNodeProfile(final FullNodeTemperatureSummary nodeProfile) {
        this.temperatureVector = nodeProfile.getTemperatureVector();
        this.totalConsumedByDimension = new double[TemperatureVector.Dimension.values().length];
        this.numOfShards = new int[TemperatureVector.Dimension.values().length];
        for (NodeLevelDimensionalSummary nodeDimensionProfile : nodeProfile.getNodeDimensionProfiles()) {
            if (nodeDimensionProfile != null) {
                int index = nodeDimensionProfile.getProfileForDimension().ordinal();
                totalConsumedByDimension[index] = nodeDimensionProfile.getTotalUsage();
                numOfShards[index] = nodeDimensionProfile.getNumberOfShards();
            }
        }
    }

    public double getTotalConsumedByDimension(TemperatureVector.Dimension dimension) {
        return totalConsumedByDimension[dimension.ordinal()];
    }

    public String getHostAddress() {
        return hostAddress;
    }

    public void setTotalConsumedByDimension(TemperatureVector.Dimension dimension,
                                            double totalConsumedByDimension) {
        this.totalConsumedByDimension[dimension.ordinal()] = totalConsumedByDimension;
    }

    public void setNumOfShards(TemperatureVector.Dimension dimension, int numOfShards) {
        this.numOfShards[dimension.ordinal()] = numOfShards;
    }

    public int getNumberOfShardsByDimension(TemperatureVector.Dimension dimension) {
        return numOfShards[dimension.ordinal()];
    }

    public void setTemperatureForDimension(TemperatureVector.Dimension dimension,
                                           TemperatureVector.NormalizedValue value) {
        temperatureVector.updateTemperatureForDimension(dimension, value);
    }

    public String getNodeId() {
        return nodeId;
    }

    public @Nullable
    TemperatureVector.NormalizedValue getTemperatureForDimension(TemperatureVector.Dimension dimension) {
        return temperatureVector.getTemperatureFor(dimension);
    }

    @Override
    public NodeTemperatureSummaryMessage buildSummaryMessage() {
        if (totalConsumedByDimension == null) {
            throw new IllegalArgumentException("totalConsumedByDimension is not initialized");
        }

        final NodeTemperatureSummaryMessage.Builder summaryBuilder =
                NodeTemperatureSummaryMessage.newBuilder();
        summaryBuilder.setNodeID(nodeId);
        summaryBuilder.setHostAddress(hostAddress);


        for (TemperatureVector.Dimension dimension : TemperatureVector.Dimension.values()) {
            int index = dimension.ordinal();
            ResourceTemperatureMessage.Builder builder = ResourceTemperatureMessage.newBuilder();
            builder.setResourceName(dimension.NAME);
            NormalizedValue normalizedMean = temperatureVector.getTemperatureFor(dimension);
            if (normalizedMean != null) {
                builder.setMeanUsage(normalizedMean.getPOINTS());
            } else {
                builder.setMeanUsage(0);
            }
            builder.setNumberOfShards(numOfShards[index]);
            builder.setTotalUsage(totalConsumedByDimension[index]);
            summaryBuilder.addCpuTemperature(index, builder);
        }
        return summaryBuilder.build();
    }

    @Override
    public void buildSummaryMessageAndAddToFlowUnit(FlowUnitMessage.Builder messageBuilder) {
        messageBuilder.setNodeTemperatureSummary(buildSummaryMessage());
    }

    @Override
    public String getTableName() {
        return TABLE_NAME;
    }

    public static CompactNodeSummary buildNodeTemperatureProfileFromMessage(NodeTemperatureSummaryMessage message) {
        CompactNodeSummary compactNodeTemperatureSummary =
                new CompactNodeSummary(message.getNodeID(), message.getHostAddress());

        compactNodeTemperatureSummary.totalConsumedByDimension = new double[TemperatureVector.Dimension.values().length];
        compactNodeTemperatureSummary.numOfShards = new int[TemperatureVector.Dimension.values().length];
        for (ResourceTemperatureMessage resourceMessage : message.getCpuTemperatureList()) {
            TemperatureVector.Dimension dimension =
                    TemperatureVector.Dimension.valueOf(resourceMessage.getResourceName());
            compactNodeTemperatureSummary.temperatureVector.updateTemperatureForDimension(dimension,
                    new TemperatureVector.NormalizedValue((short) resourceMessage.getMeanUsage()));
            compactNodeTemperatureSummary.totalConsumedByDimension[dimension.ordinal()] =
                    resourceMessage.getTotalUsage();
            compactNodeTemperatureSummary.numOfShards[dimension.ordinal()] =
                    resourceMessage.getNumberOfShards();
        }
        return compactNodeTemperatureSummary;
    }

    /**
     * @return Returns a list of columns that this table would contain.
     */
    @Override
    public List<Field<?>> getSqlSchema() {
        List<Field<?>> schema = new ArrayList<>();
        schema.add(DSL.field(DSL.name(HotNodeSummary.SQL_SCHEMA_CONSTANTS.NODE_ID_COL_NAME), String.class));
        schema.add(DSL.field(DSL.name(HotNodeSummary.SQL_SCHEMA_CONSTANTS.HOST_IP_ADDRESS_COL_NAME), String.class));

        for (TemperatureVector.Dimension dimension : TemperatureVector.Dimension.values()) {
            schema.add(DSL.field(DSL.name(dimension.NAME + MEAN_SUFFIX_KEY), String.class));
            schema.add(DSL.field(DSL.name(dimension.NAME + TOTAL_SUFFIX_KEY), String.class));
            schema.add(DSL.field(DSL.name(dimension.NAME + NUM_SHARDS_SUFFIX_KEY), String.class));
        }
        return schema;
    }

    @Override
    public List<Object> getSqlValue() {
        List<Object> value = new ArrayList<>();
        value.add(nodeId);
        value.add(hostAddress);

        for (TemperatureVector.Dimension dimension : TemperatureVector.Dimension.values()) {
            value.add(temperatureVector.getTemperatureFor(dimension));
            value.add(totalConsumedByDimension[dimension.ordinal()]);
            value.add(numOfShards[dimension.ordinal()]);
        }
        return value;
    }

    @Override
    public JsonElement toJson() {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty(HotNodeSummary.SQL_SCHEMA_CONSTANTS.NODE_ID_COL_NAME, nodeId);
        jsonObject.addProperty(HotNodeSummary.SQL_SCHEMA_CONSTANTS.HOST_IP_ADDRESS_COL_NAME, hostAddress);

        for (TemperatureVector.Dimension dimension : TemperatureVector.Dimension.values()) {
            TemperatureVector.NormalizedValue ret = temperatureVector.getTemperatureFor(dimension);
            if (ret == null) {
                ret = new TemperatureVector.NormalizedValue((short) 0);
            }
            jsonObject.addProperty(dimension.NAME + MEAN_SUFFIX_KEY, ret.getPOINTS());
            jsonObject.addProperty(dimension.NAME + TOTAL_SUFFIX_KEY,
                    totalConsumedByDimension[dimension.ordinal()]);
            jsonObject.addProperty(dimension.NAME + NUM_SHARDS_SUFFIX_KEY, numOfShards[dimension.ordinal()]);
        }
        return jsonObject;
    }
}
