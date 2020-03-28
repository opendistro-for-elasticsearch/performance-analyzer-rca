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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.HeatZoneAssigner;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.dimension.CpuUtilDimensionTemperatureRca;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.GeneratedMessageV3;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.jooq.Field;
import org.jooq.impl.DSL;

/**
 * A node dimension profile is categorization of all shards in the node into different heatZones.
 */
public class ClusterDimensionalTemperatureSummary extends GenericSummary {
    private final TemperatureVector.Dimension profileForDimension;
    private TemperatureVector.NormalizedValue meanTemperature;
    private double totalUsage;

    private final ClusterZoneProfileSummary[] zoneProfiles;
    private int numberOfNodes;

    public ClusterDimensionalTemperatureSummary(TemperatureVector.Dimension profileForDimension) {
        this.profileForDimension = profileForDimension;

        this.zoneProfiles = new ClusterZoneProfileSummary[HeatZoneAssigner.Zone.values().length];
        for (int i = 0; i < this.zoneProfiles.length; i++) {
            this.zoneProfiles[i] = new ClusterZoneProfileSummary(HeatZoneAssigner.Zone.values()[i]);
        }
    }

    public void setMeanTemperature(TemperatureVector.NormalizedValue meanTemperature) {
        this.meanTemperature = meanTemperature;
    }

    public void setTotalUsage(double totalUsage) {
        this.totalUsage = totalUsage;
    }

    public int getNumberOfNodes() {
        return numberOfNodes;
    }

    public void addNodeToZone(CompactNodeTemperatureSummary nodeSummary) {
        HeatZoneAssigner.Zone zone =
                HeatZoneAssigner.assign(nodeSummary.getTemperatureForDimension(profileForDimension),
                        meanTemperature,
                        CpuUtilDimensionTemperatureRca.THRESHOLD_NORMALIZED_VAL_FOR_HEAT_ZONE_ASSIGNMENT);
        ClusterZoneProfileSummary profile = zoneProfiles[zone.ordinal()];
        profile.addNode(nodeSummary);
        numberOfNodes += 1;
    }

    public TemperatureVector.NormalizedValue getMeanTemperature() {
        return meanTemperature;
    }

    public TemperatureVector.Dimension getProfileForDimension() {
        return profileForDimension;
    }

    public double getTotalUsage() {
        return totalUsage;
    }

    @Override
    public <T extends GeneratedMessageV3> T buildSummaryMessage() {
        throw new IllegalArgumentException("This should not be called.");
    }

    @Override
    public void buildSummaryMessageAndAddToFlowUnit(FlowUnitMessage.Builder messageBuilder) {
        throw new IllegalArgumentException("This should not be called.");
    }

    @Override
    public String getTableName() {
        return this.getClass().getSimpleName();
    }

    public List<GenericSummary> getNestedSummaryList() {
        List<GenericSummary> zoneSummaries = new ArrayList<>();
        for (ClusterZoneProfileSummary zone : zoneProfiles) {
            zoneSummaries.add(zone);
        }
        return zoneSummaries;
    }

    @Override
    public List<Field<?>> getSqlSchema() {
        List<Field<?>> schema = new ArrayList<>();
        schema.add(DSL.field(DSL.name("dimension"), String.class));
        schema.add(DSL.field(DSL.name("mean"), Short.class));
        schema.add(DSL.field(DSL.name("total"), Double.class));
        schema.add(DSL.field(DSL.name("numNodes"), Integer.class));

        return schema;
    }

    @Override
    public List<Object> getSqlValue() {
        List<Object> row = new ArrayList<>();
        row.add(getProfileForDimension().NAME);
        row.add(getMeanTemperature().getPOINTS());
        row.add(getTotalUsage());
        row.add(getNumberOfNodes());
        return row;
    }

    @Override
    public JsonElement toJson() {
        JsonObject summaryObj = new JsonObject();
        summaryObj.addProperty("dimension", getProfileForDimension().NAME);
        summaryObj.addProperty("mean", getMeanTemperature().getPOINTS());
        summaryObj.addProperty("total", getTotalUsage());
        summaryObj.addProperty("numNodes", getNumberOfNodes());
        getNestedSummaryList().forEach(
                summary -> {
                    summaryObj.add(summary.getTableName(), summary.toJson());
                }
        );
        return summaryObj;
    }

    class ClusterZoneProfileSummary extends GenericSummary {
        List<CompactNodeTemperatureSummary> nodeProfileSummaries;
        CompactNodeTemperatureSummary minNode;
        //ShardProfileSummary maxShard;
        CompactNodeTemperatureSummary maxNode;

        private final HeatZoneAssigner.Zone myZone;

        ClusterZoneProfileSummary(HeatZoneAssigner.Zone myZone) {
            this.myZone = myZone;
            nodeProfileSummaries = new ArrayList<>();
        }

        void addNode(CompactNodeTemperatureSummary node) {
            nodeProfileSummaries.add(node);
            if (minNode == null) {
                minNode = node;
            } else {
                if (getMinTemperature().isGreaterThan(
                        node.getTemperatureForDimension(profileForDimension))) {
                    minNode = node;
                }
            }

            if (maxNode == null) {
                maxNode = node;
            } else {
                if (node.getTemperatureForDimension(profileForDimension).isGreaterThan(
                        getMaxTemperature())) {
                    maxNode = node;
                }
            }
        }

        @Nullable
        TemperatureVector.NormalizedValue getMinTemperature() {
            if (minNode != null) {
                return minNode.getTemperatureForDimension(profileForDimension);
            }
            return null;
        }

        @Nullable
        TemperatureVector.NormalizedValue getMaxTemperature() {
            if (maxNode != null) {
                return maxNode.getTemperatureForDimension(profileForDimension);
            }
            return null;
        }

        @Override
        public <T extends GeneratedMessageV3> T buildSummaryMessage() {
            throw new IllegalArgumentException("");
        }

        @Override
        public void buildSummaryMessageAndAddToFlowUnit(FlowUnitMessage.Builder messageBuilder) {
            throw new IllegalArgumentException("");
        }

        @Override
        public String getTableName() {
            return this.getClass().getSimpleName();
        }

        @Override
        public List<Field<?>> getSqlSchema() {
            List<Field<?>> schema = new ArrayList<>();
            schema.add(DSL.field(DSL.name("zone"), String.class));
            schema.add(DSL.field(DSL.name("min"), String.class));
            schema.add(DSL.field(DSL.name("max"), String.class));
            return schema;
        }

        public List<GenericSummary> getNestedSummaryList() {
            List<GenericSummary> summaries = new ArrayList<>();
            for (CompactNodeTemperatureSummary nodeProfileSummary : nodeProfileSummaries) {
                summaries.add(nodeProfileSummary);
            }
            return summaries;
        }

        @Override
        public List<Object> getSqlValue() {
            List<Object> values = new ArrayList<>();
            values.add(myZone.name());
            values.add(minNode == null ? "" : minNode.toString());
            values.add(maxNode == null ? "" : maxNode.toString());
            return values;
        }

        @Override
        public JsonElement toJson() {
            JsonObject summaryObj = new JsonObject();
            summaryObj.addProperty("zone_name", myZone.name());
            summaryObj.add("min_node", minNode.toJson());
            summaryObj.add("max_node", maxNode.toJson());
            getNestedSummaryList().forEach(
                    summary -> {
                        summaryObj.add(summary.getTableName(), summary.toJson());
                    }
            );
            return summaryObj;
        }
    }
}
