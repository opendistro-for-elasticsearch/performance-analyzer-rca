/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.HeatZoneAssigner;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.SQLiteQueryUtils;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.dimension.CpuUtilDimensionTemperatureRca;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.GeneratedMessageV3;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SelectJoinStep;
import org.jooq.impl.DSL;

/**
 * This is the temperature summary at a cluster level. This categorizes all the nodes in the
 * cluster in 4 regions: hot, warm, luke-warm and cold for all the tracked dimensions. This
 * object is created on the elected master. One such object is created per tracked dimension.
 */
public class ClusterDimensionalSummary extends GenericSummary {
    /**
     * The name of the table in which this summary is persisted.
     */
    public static final String TABLE_NAME =
            ClusterDimensionalSummary.class.getSimpleName();

    /**
     * The name of the table in which Zonal categorizations of the nodes are persisted.
     */
    public static final String ZONE_PROFILE_TABLE_NAME =
            ZoneSummary.class.getSimpleName();

    private static final String DIM_KEY = "dimension";
    private static final String MEAN_KEY = "mean";
    private static final String TOTAL_KEY = "total";
    private static final String NUM_NODES_KEY = "numNodes";

    /**
     * This determines which dimension for which this profile is.
     */
    private final TemperatureDimension profileForDimension;

    /**
     * This is the mean temperature for this dimension over all the nodes in the cluster.
     */
    private TemperatureVector.NormalizedValue meanTemperature;

    /**
     * meanTemperature is a normalized value. The total tells us if this is something that one
     * should be concerned about.
     */
    private double totalUsage;

    /**
     * The number of nodes in the cluster.
     */
    private int numberOfNodes;

    /**
     * The zonal details go here. This is a wrapper over all nodes that belong to this zone. A
     * zone can be empty, i.e. have to nodes that belong to it.
     */
    private final ZoneSummary[] zoneProfiles;

    public ClusterDimensionalSummary(TemperatureDimension profileForDimension) {
        this.profileForDimension = profileForDimension;

        this.zoneProfiles = new ZoneSummary[HeatZoneAssigner.Zone.values().length];
        for (int i = 0; i < this.zoneProfiles.length; i++) {
            this.zoneProfiles[i] = new ZoneSummary(HeatZoneAssigner.Zone.values()[i]);
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

    public void setNumberOfNodes(int numberOfNodes) {
        this.numberOfNodes = numberOfNodes;
    }

    public void addNodeToZone(CompactClusterLevelNodeSummary nodeSummary) {
        HeatZoneAssigner.Zone zone =
                HeatZoneAssigner.assign(nodeSummary.getTemperatureForDimension(profileForDimension),
                        meanTemperature,
                        CpuUtilDimensionTemperatureRca.THRESHOLD_NORMALIZED_VAL_FOR_HEAT_ZONE_ASSIGNMENT);
        ZoneSummary profile = zoneProfiles[zone.ordinal()];
        profile.addNode(nodeSummary);
        numberOfNodes += 1;
    }

    public TemperatureVector.NormalizedValue getMeanTemperature() {
        return meanTemperature;
    }

    public TemperatureDimension getProfileForDimension() {
        return profileForDimension;
    }

    public double getTotalUsage() {
        return totalUsage;
    }

    /**
     * Should not be called as its not passed across network boundaries.
     */
    @Override
    public <T extends GeneratedMessageV3> T buildSummaryMessage() {
        throw new IllegalArgumentException("This should not be called.");
    }

    /**
     * Should not be called as its not passed across network boundaries.
     */
    @Override
    public void buildSummaryMessageAndAddToFlowUnit(FlowUnitMessage.Builder messageBuilder) {
        throw new IllegalArgumentException("This should not be called.");
    }

    @Override
    public String getTableName() {
        return TABLE_NAME;
    }

    public List<GenericSummary> getNestedSummaryList() {
        List<GenericSummary> zoneSummaries = new ArrayList<>();
        for (ZoneSummary zone : zoneProfiles) {
            zoneSummaries.add(zone);
        }
        return zoneSummaries;
    }

    /**
     * This determines the SQLite schema. It gives us the name and type of columns.
     * @return A list of columns for this table.
     */
    public static List<Field<?>> getCols() {
        List<Field<?>> schema = new ArrayList<>();
        schema.add(DSL.field(DSL.name(DIM_KEY), String.class));
        schema.add(DSL.field(DSL.name(MEAN_KEY), Short.class));
        schema.add(DSL.field(DSL.name(TOTAL_KEY), Double.class));
        schema.add(DSL.field(DSL.name(NUM_NODES_KEY), Integer.class));

        return schema;
    }

    @Override
    public List<Field<?>> getSqlSchema() {
        return getCols();
    }

    /**
     * Serializes the summary information into a row that can be persisted into a table.
     * @return a list of values.
     */
    @Override
    public List<Object> getSqlValue() {
        List<Object> row = new ArrayList<>();
        row.add(getProfileForDimension().NAME);
        row.add(getMeanTemperature().getPOINTS());
        row.add(getTotalUsage());
        row.add(getNumberOfNodes());
        return row;
    }

    /**
     * This serializes the object and all its inner classes into a JSON object as a response to a
     * REST call.
     * @return A JsonObject.
     */
    @Override
    public JsonElement toJson() {
        JsonObject summaryObj = new JsonObject();
        summaryObj.addProperty(DIM_KEY, getProfileForDimension().NAME);
        summaryObj.addProperty(MEAN_KEY, getMeanTemperature().getPOINTS());
        summaryObj.addProperty(TOTAL_KEY, getTotalUsage());
        summaryObj.addProperty(NUM_NODES_KEY, getNumberOfNodes());

        JsonArray arr = new JsonArray();
        for (ZoneSummary zone : zoneProfiles) {
            arr.add(zone.toJson());
        }

        summaryObj.add(ZONE_PROFILE_TABLE_NAME, arr);
        return summaryObj;
    }

    /**
     * The columns are:
     * ClusterDimensionTemperatureSummary_ID | dimension | mean | total | numNodes |
     * ClusterTemperatureSummary_ID
     *
     * <p>The build is used to create a summary object from a row of the table.
     *
     * @param record A database row
     * @param context The SQL query context that is used to get the data for the inner objects.
     * @return Returns the summary object
     */
    public static ClusterDimensionalSummary build(Record record, DSLContext context) {
        String dimensionName = record.get(DIM_KEY, String.class);

        TemperatureVector.NormalizedValue meanTemp =
                new TemperatureVector.NormalizedValue(record.get(MEAN_KEY, Short.class));
        double total = record.get(TOTAL_KEY, Double.class);
        int numNodes = record.get(NUM_NODES_KEY, Integer.class);
        int dimSummaryId = record.get(SQLiteQueryUtils.getPrimaryKeyColumnName(TABLE_NAME),
                Integer.class);

        ClusterDimensionalSummary summary =
                new ClusterDimensionalSummary(TemperatureDimension.valueOf(dimensionName));
        summary.setTotalUsage(total);
        summary.setMeanTemperature(meanTemp);
        summary.setNumberOfNodes(numNodes);

        Field<Integer> foreignKeyForDimensionalTable = DSL.field(
                SQLiteQueryUtils.getPrimaryKeyColumnName(TABLE_NAME), Integer.class);

        SelectJoinStep<Record> rcaQuery = SQLiteQueryUtils
                .buildSummaryQuery(context, ZONE_PROFILE_TABLE_NAME, dimSummaryId, foreignKeyForDimensionalTable);

        Result<Record> recordList = rcaQuery.fetch();
        for (Record zoneSummary : recordList) {
            buildZoneProfile(zoneSummary, summary, context);
        }
        return summary;
    }

    /**
     * The columns are :
     * ClusterZoneProfileSummary_ID | zone | min | max | ClusterDimensionalTemperatureSummary_ID
     *
     * @param record A database row
     * @param summary The summary object constructed from the database row
     */
    private static void buildZoneProfile(Record record,
                                         ClusterDimensionalSummary summary,
                                         DSLContext context) {
        String zoneName = record.get(ZoneSummary.ZONE_KEY, String.class);

        ZoneSummary zone =
                summary.zoneProfiles[HeatZoneAssigner.Zone.valueOf(zoneName).ordinal()];

        String allNodesStr = record.get(ZoneSummary.ALL_NODES_KEY, String.class);

        if (!allNodesStr.isEmpty()) {
            JsonArray json = new JsonParser().parse(allNodesStr).getAsJsonArray();

            for (int i = 0; i < json.getAsJsonArray().size(); i++) {
                JsonObject obj = json.get(i).getAsJsonObject();
                String hostIp =
                        obj.get(HotNodeSummary.SQL_SCHEMA_CONSTANTS.HOST_IP_ADDRESS_COL_NAME).getAsString();
                String nodeId =
                        obj.get(HotNodeSummary.SQL_SCHEMA_CONSTANTS.NODE_ID_COL_NAME).getAsString();

                CompactClusterLevelNodeSummary nodeSummary =
                        new CompactClusterLevelNodeSummary(nodeId, hostIp);
                zone.addNode(nodeSummary);
            }
        }
    }

    /**
     * A summary object to encapsulate all the details about a temperature zone.
     */
    public class ZoneSummary extends GenericSummary {
        private static final String ZONE_KEY = "zone";
        private static final String MIN_KEY = "min";
        private static final String MAX_KEY = "max";
        private static final String ALL_NODES_KEY = "all_nodes";

        /**
         * This is the zone for which details are stored here.
         */
        private final HeatZoneAssigner.Zone myZone;

        /**
         * The list of nodes that are part of this zone for a given dimension.
         */
        List<CompactClusterLevelNodeSummary> nodeProfileSummaries;

        /**
         * The EsNode with the minimum temperature value.
         */
        CompactClusterLevelNodeSummary minNode;

        /**
         * The ES node with the maximum temperature value.
         */
        CompactClusterLevelNodeSummary maxNode;

        ZoneSummary(HeatZoneAssigner.Zone myZone) {
            this.myZone = myZone;
            nodeProfileSummaries = new ArrayList<>();
        }

        /**
         * Adds a node to this zone.
         *
         * <p>It also compares the temperature for this node with the nodes gathered so far to
         * update the min and the max nodes.
         * @param node The node to be added.
         */
        void addNode(@Nonnull final CompactClusterLevelNodeSummary node) {
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
            return ZONE_PROFILE_TABLE_NAME;
        }

        private List<Field<?>> getColumns() {
            List<Field<?>> schema = new ArrayList<>();
            schema.add(DSL.field(DSL.name(ZONE_KEY), String.class));
            schema.add(DSL.field(DSL.name(MIN_KEY), String.class));
            schema.add(DSL.field(DSL.name(MAX_KEY), String.class));
            schema.add(DSL.field(DSL.name(ALL_NODES_KEY), String.class));
            return schema;
        }

        @Override
        public List<Field<?>> getSqlSchema() {
            return getColumns();
        }

        private JsonObject getNodeJson(CompactClusterLevelNodeSummary nodeSummary) {
            JsonObject jsonObject = new JsonObject();

            jsonObject.addProperty(HotNodeSummary.SQL_SCHEMA_CONSTANTS.HOST_IP_ADDRESS_COL_NAME,
                    nodeSummary.getHostAddress());
            jsonObject.addProperty(HotNodeSummary.SQL_SCHEMA_CONSTANTS.NODE_ID_COL_NAME,
                    nodeSummary.getNodeId());

            return jsonObject;
        }

        private JsonArray getAllNodesJson() {
            JsonArray jsonArray = new JsonArray();
            for (CompactClusterLevelNodeSummary nodeSummary : nodeProfileSummaries) {
                jsonArray.add(getNodeJson(nodeSummary));
            }
            return jsonArray;
        }

        private String getAllNodesString(final JsonArray json) {
            // The gson API throws an error while converting JSON to string if the Json array has
            // more than one element.
            StringBuilder sb = new StringBuilder();

            String delim = "[";
            for (JsonElement elem : json) {
                JsonObject obj = elem.getAsJsonObject();
                sb.append(delim).append(obj);
                delim = ",";
            }
            if (sb.length() > 0) {
                sb.append("]");
            }
            return sb.toString();
        }

        @Override
        public List<Object> getSqlValue() {
            List<Object> values = new ArrayList<>();
            values.add(myZone.name());
            values.add(minNode == null ? "" : minNode.getHostAddress());
            values.add(maxNode == null ? "" : maxNode.getHostAddress());

            JsonArray allNodes = getAllNodesJson();
            String allNodesStr = getAllNodesString(allNodes);
            values.add(allNodesStr);

            return values;
        }

        @Override
        public JsonElement toJson() {
            JsonObject summaryObj = new JsonObject();
            summaryObj.addProperty(ZONE_KEY, myZone.name());

            if (minNode == maxNode) {
                summaryObj.add(MIN_KEY, null);
                summaryObj.add(MAX_KEY, null);
            } else {
                summaryObj.add(MIN_KEY, minNode == null ? null : getNodeJson(minNode));
                summaryObj.add(MAX_KEY, maxNode == null ? null : getNodeJson(maxNode));
            }
            summaryObj.add(ALL_NODES_KEY, getAllNodesJson());

            return summaryObj;
        }
    }
}
