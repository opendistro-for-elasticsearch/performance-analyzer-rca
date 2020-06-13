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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.RawMetricsVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.ShardStore;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.SQLiteQueryUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.protobuf.GeneratedMessageV3;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.SelectJoinStep;
import org.jooq.impl.DSL;

/**
 * A node dimension profile is categorization of all shards in the node into different heatZones.
 */
public class NodeLevelDimensionalSummary extends GenericSummary {

    public static final String SUMMARY_TABLE_NAME = "NodeLevelDimensionalSummary";
    public static final String ZONE_SUMMARY_TABLE_NAME = "NodeLevelZoneSummary";

    private static final String DIMENSION_KEY = "dimension";
    private static final String MEAN_KEY = "mean";
    private static final String AVG_SHARDS_KEY = "avg";
    private static final String TOTAL_KEY = "total";
    private static final String NUM_SHARDS_KEY = "numShards";

    private final TemperatureDimension profileForDimension;
    private final TemperatureVector.NormalizedValue meanTemperature;
    private final double avgMetricValueOverShards;
    private final double totalMetricValueUsed;

    private final NodeLevelZoneSummary[] zoneProfiles;
    private int numberOfShards;

    public NodeLevelDimensionalSummary(final TemperatureDimension profileForDimension,
                                       final TemperatureVector.NormalizedValue meanTemperature,
                                       double avgMetricValueOverShards,
                                       double totalMetricValueUsed) {
        this.profileForDimension = profileForDimension;
        this.meanTemperature = meanTemperature;
        this.avgMetricValueOverShards = avgMetricValueOverShards;
        this.totalMetricValueUsed = totalMetricValueUsed;
        this.zoneProfiles = new NodeLevelZoneSummary[HeatZoneAssigner.Zone.values().length];
        for (int i = 0; i < this.zoneProfiles.length; i++) {
            this.zoneProfiles[i] = new NodeLevelZoneSummary(HeatZoneAssigner.Zone.values()[i]);
        }
    }

    public int getNumberOfShards() {
        return numberOfShards;
    }

    public void setNumberOfShards(int numberOfShards) {
        this.numberOfShards = numberOfShards;
    }

    public void addShardToZone(ShardProfileSummary shard, HeatZoneAssigner.Zone zone) {
        NodeLevelZoneSummary profile = zoneProfiles[zone.ordinal()];
        profile.addShard(shard);
    }

    @Override
    public String toString() {
        return toJson().toString();
    }

    public TemperatureVector.NormalizedValue getMeanTemperature() {
        return meanTemperature;
    }

    public TemperatureDimension getProfileForDimension() {
        return profileForDimension;
    }

    public double getTotalMetricValueUsed() {
        return totalMetricValueUsed;
    }

    public double getAvgMetricValueOverShards() {
        return avgMetricValueOverShards;
    }

    @VisibleForTesting
    public List<ShardProfileSummary> getShardsForZoneInReverseTemperatureOrder(HeatZoneAssigner.Zone zone) {
        return zoneProfiles[zone.ordinal()].getShardsInReverseTemperatureOrder();
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
        return SUMMARY_TABLE_NAME;
    }

    public List<GenericSummary> getNestedSummaryList() {
        List<GenericSummary> zoneSummaries = new ArrayList<>();
        for (NodeLevelZoneSummary zone : zoneProfiles) {
            zoneSummaries.add(zone);
        }
        return zoneSummaries;
    }

    @Override
    public List<Field<?>> getSqlSchema() {
        List<Field<?>> schema = new ArrayList<>();
        schema.add(DSL.field(DSL.name(DIMENSION_KEY), String.class));
        schema.add(DSL.field(DSL.name(MEAN_KEY), Short.class));
        schema.add(DSL.field(DSL.name(AVG_SHARDS_KEY), Short.class));
        schema.add(DSL.field(DSL.name(TOTAL_KEY), Double.class));
        schema.add(DSL.field(DSL.name(NUM_SHARDS_KEY), Integer.class));

        return schema;
    }

    @Override
    public List<Object> getSqlValue() {
        List<Object> row = new ArrayList<>();
        row.add(getProfileForDimension().NAME);
        row.add(getMeanTemperature().getPOINTS());
        row.add(getAvgMetricValueOverShards());
        row.add(getTotalMetricValueUsed());
        row.add(getNumberOfShards());
        return row;
    }

    @Override
    public JsonElement toJson() {
        JsonObject summaryObj = new JsonObject();
        summaryObj.addProperty(DIMENSION_KEY, getProfileForDimension().NAME);
        summaryObj.addProperty(MEAN_KEY, getMeanTemperature().getPOINTS());
        summaryObj.addProperty(AVG_SHARDS_KEY, getMeanTemperature().getPOINTS());
        summaryObj.addProperty(TOTAL_KEY, getTotalMetricValueUsed());
        summaryObj.addProperty(NUM_SHARDS_KEY, getNumberOfShards());

        JsonArray array = new JsonArray();
        getNestedSummaryList().forEach(
                summary -> {
                    array.add(summary.toJson());
                }
        );

        summaryObj.add(ZONE_SUMMARY_TABLE_NAME, array);
        return summaryObj;
    }

    /**
     * +------------------------------+---------------+----+----------------+-----------------+----------------+
     * |NodeLevelDimensionalSummary_ID|dimension      |mean|             avg|           total |numShards|RCA_ID|
     * +------------------------------+---------------+----+----------------+-----------------+----------------+
     * |                             1|CPU_Utilization|   1|    0.4027579542| 1.20827386264977|        3|    1|
     * +------------------------------+---------------+----+----------------+-----------------+----------------+
     *
     * @param record  A db row containing the values for a temperature dimension.
     * @param context the database context. It is used to query the nested summary tables.
     * @return Creates a new instance of the NodeLevelDimensionalSummary.
     */
    public static NodeLevelDimensionalSummary buildFromDb(final Record record, DSLContext context) {
        String dimensionName = record.get(DIMENSION_KEY, String.class);
        TemperatureDimension dimension = TemperatureDimension.valueOf(dimensionName);
        Short mean = record.get(MEAN_KEY, Short.class);
        TemperatureVector.NormalizedValue value = new TemperatureVector.NormalizedValue(mean);

        double total = record.get(TOTAL_KEY, Double.class);
        int shards = record.get(NUM_SHARDS_KEY, Integer.class);

        NodeLevelDimensionalSummary summary;
        if (shards > 0) {
            summary = new NodeLevelDimensionalSummary(dimension, value, (total / shards), total);
        } else {
            summary = new NodeLevelDimensionalSummary(dimension, value, total, total);
        }
        summary.setNumberOfShards(shards);

        // At this point we have filled in the NodeSummary with the data in the tuple. To
        // populate the nested objects, we need to query the nested summary table.
        // Of all the given rows in the nested summary table, we are only interested in the rows
        // that correspond to this tuple provided as argument to this method. The linking is done
        // using the ID column of this table that is a foreign key to the nested summary table.

        // Get the primary key value for this tuple.
        int dimensionId = record.get(
                SQLiteQueryUtils.getPrimaryKeyColumnName(SUMMARY_TABLE_NAME),
                Integer.class);

        // Find out the foreign key column name for the nested summary table.
        Field<Integer> foreignKeyForDimensionalTable = DSL.field(
                SQLiteQueryUtils.getPrimaryKeyColumnName(SUMMARY_TABLE_NAME), Integer.class);

        SelectJoinStep<Record> rcaQuery = SQLiteQueryUtils
                .buildSummaryQuery(context, ZONE_SUMMARY_TABLE_NAME, dimensionId,
                        foreignKeyForDimensionalTable);

        Result<Record> recordList = rcaQuery.fetch();
        ShardStore shardStore = new ShardStore();
        for (Record zoneSummary : recordList) {
            summary.buildZoneProfile(zoneSummary, shardStore);
        }
        return summary;
    }

    /**
     * +-----------------------+----+----+----+----------+------------------------------+
     * |NodeLevelZoneSummary_ID|zone|min |max |all_shards|NodeLevelDimensionalSummary_ID|
     * +-----------------------+----+----+----+----------+------------------------------+
     * |                      1|HOT |    |    |[]        |                             1|
     * +-----------------------+----+----+----+----------+------------------------------+
     *
     * @param record A database row containing the values for one of the 4 zones.
     */
    private void buildZoneProfile(final Record record, ShardStore shardStore
    ) {
        String zoneName = record.get(NodeLevelZoneSummary.ZONE_KEY, String.class);
        HeatZoneAssigner.Zone zone = HeatZoneAssigner.Zone.valueOf(zoneName);

        NodeLevelZoneSummary zoneSummary = zoneProfiles[zone.ordinal()];
        String jsonString = record.get(NodeLevelZoneSummary.ALL_KEY, String.class);
        JsonArray jsonArray = new JsonParser().parse(jsonString).getAsJsonArray();

        for (Iterator<JsonElement> it = jsonArray.iterator(); it.hasNext(); ) {
            JsonElement element = it.next();
            String indexName =
                    element.getAsJsonObject().get(ShardProfileSummary.INDEX_NAME_KEY).getAsString();
            int shardId =
                    element.getAsJsonObject().get(ShardProfileSummary.SHARD_ID_KEY).getAsInt();
            ShardProfileSummary shard = shardStore.getOrCreateIfAbsent(indexName, shardId);
            JsonArray temperatureProfiles =
                    element.getAsJsonObject().get(ShardProfileSummary.TEMPERATURE_KEY).getAsJsonArray();
            JsonArray rawMetrics =
                    element.getAsJsonObject().get(ShardProfileSummary.RAW_METRIC_KEY).getAsJsonArray();

            Iterator<JsonElement> temperatureProfileIterator = temperatureProfiles.iterator();
            Iterator<JsonElement> rawMetricsIterator = rawMetrics.iterator();

            while (temperatureProfileIterator.hasNext() && rawMetricsIterator.hasNext()) {
                JsonObject temperatureObj = temperatureProfileIterator.next().getAsJsonObject();
                JsonObject rawMetricsObj = rawMetricsIterator.next().getAsJsonObject();
                TemperatureDimension temperatureDimension =
                        TemperatureDimension.valueOf(temperatureObj.get(TemperatureVector.DIMENSION_KEY).getAsString());
                TemperatureVector.NormalizedValue temperatureValue =
                        new TemperatureVector.NormalizedValue((short) temperatureObj.get(TemperatureVector.VALUE_KEY).getAsInt());
                double rawMetricsValue = rawMetricsObj.get(RawMetricsVector.VALUE_KEY).getAsDouble();
                shard.addTemperatureForDimension(temperatureDimension, temperatureValue);
                shard.addRawMetricForDimension(temperatureDimension, rawMetricsValue);
            }
            zoneSummary.addShard(shard);
        }
    }

    public class NodeLevelZoneSummary extends GenericSummary {
        public static final String ZONE_KEY = "zone";
        public static final String ALL_KEY = "all_shards";

        List<ShardProfileSummary> shardProfileSummaries;
        private final HeatZoneAssigner.Zone myZone;

        NodeLevelZoneSummary(HeatZoneAssigner.Zone myZone) {
            this.myZone = myZone;
            shardProfileSummaries = new ArrayList<>();
        }

        void addShard(ShardProfileSummary shard) {
            shardProfileSummaries.add(shard);
        }

        public List<ShardProfileSummary> getShardsInReverseTemperatureOrder() {
            shardProfileSummaries.sort(new ShardProfileComparator());
            return Collections.unmodifiableList(shardProfileSummaries);
        }

        @Override
        public String toString() {
            return toJson().toString();
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
            return ZONE_SUMMARY_TABLE_NAME;
        }

        @Override
        public List<Field<?>> getSqlSchema() {
            List<Field<?>> schema = new ArrayList<>();
            schema.add(DSL.field(DSL.name(ZONE_KEY), String.class));
            schema.add(DSL.field(DSL.name(ALL_KEY), String.class));
            return schema;
        }

        public List<GenericSummary> getNestedSummaryList() {
            List<GenericSummary> shardSummaries = new ArrayList<>();
            for (ShardProfileSummary shardProfileSummary : getShardsInReverseTemperatureOrder()) {
                shardSummaries.add(shardProfileSummary);
            }
            return shardSummaries;
        }

        @Override
        public List<Object> getSqlValue() {
            List<Object> values = new ArrayList<>();
            values.add(myZone.name());
            JsonArray array = new JsonArray();

            // The reason we are not getting shards ordered by temperature as
            // we don't want to pay the cost of sorting while writes. We do writes
            // more often than reads and therefore, we would rather sort on reads.
            for (ShardProfileSummary shard : shardProfileSummaries) {
                if (shard != null) {
                    array.add(shard.toJson());
                }
            }
            values.add(array);
            return values;
        }

        @Override
        public JsonElement toJson() {
            JsonObject summaryObj = new JsonObject();
            summaryObj.addProperty(ZONE_KEY, myZone.name());

            JsonArray array = new JsonArray();
            getNestedSummaryList().forEach(
                    summary -> {
                        array.add(summary.toJson());
                    }
            );
            summaryObj.add(ALL_KEY, array);
            return summaryObj;
        }
    }

    private class ShardProfileComparator implements Comparator<ShardProfileSummary> {
        @Override
        public int compare(ShardProfileSummary o1, ShardProfileSummary o2) {
            return reverseSort(o1, o2);
        }

        private int reverseSort(ShardProfileSummary o1, ShardProfileSummary o2) {
            return o2.getHeatInDimension(profileForDimension).getPOINTS() - o1.getHeatInDimension(profileForDimension).getPOINTS();
        }
    }
}
