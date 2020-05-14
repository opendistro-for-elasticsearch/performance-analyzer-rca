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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.protobuf.GeneratedMessageV3;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.jooq.Field;
import org.jooq.impl.DSL;

/**
 * A node dimension profile is categorization of all shards in the node into different heatZones.
 */
public class NodeLevelDimensionalSummary extends GenericSummary {

  public static final String SUMMARY_TABLE_NAME = "NodeLevelDimensionalSummary";
  public static final String ZONE_SUMMARY_TABLE_NAME = "NodeLevelZoneSummary";

  private final TemperatureVector.Dimension profileForDimension;
    private final TemperatureVector.NormalizedValue meanTemperature;
    private final double totalUsage;

    private final NodeLevelZoneSummary[] zoneProfiles;
    private int numberOfShards;

    public NodeLevelDimensionalSummary(final TemperatureVector.Dimension profileForDimension,
                                       final TemperatureVector.NormalizedValue meanTemperature,
                                       double totalUsage) {
        this.profileForDimension = profileForDimension;
        this.meanTemperature = meanTemperature;
        this.totalUsage = totalUsage;
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
        return "NodeLevelDimensionalSummary{"
                + "profileForDimension=" + profileForDimension
                + ", meanTemperature=" + meanTemperature
                + ", totalUsage=" + totalUsage
                + ", zoneProfiles=" + Arrays.toString(zoneProfiles)
                + ", numberOfShards=" + numberOfShards
                + '}';
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
        for (NodeLevelZoneSummary zone : zoneProfiles) {
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
        schema.add(DSL.field(DSL.name("numShards"), Integer.class));

        return schema;
    }

    @Override
    public List<Object> getSqlValue() {
        List<Object> row = new ArrayList<>();
        row.add(getProfileForDimension().NAME);
        row.add(getMeanTemperature().getPOINTS());
        row.add(getTotalUsage());
        row.add(getNumberOfShards());
        return row;
    }

    @Override
    public JsonElement toJson() {
        JsonObject summaryObj = new JsonObject();
        summaryObj.addProperty("dimension", getProfileForDimension().NAME);
        summaryObj.addProperty("mean", getMeanTemperature().getPOINTS());
        summaryObj.addProperty("total", getTotalUsage());
        summaryObj.addProperty("numShards", getNumberOfShards());
        getNestedSummaryList().forEach(
                summary -> {
                    summaryObj.add(summary.getTableName(), summary.toJson());
                }
        );
        return summaryObj;
    }

    class NodeLevelZoneSummary extends GenericSummary {
        List<ShardProfileSummary> shardProfileSummaries;
        ShardProfileSummary minShard;
        //ShardProfileSummary maxShard;
        ShardProfileSummary maxShard;

        private final HeatZoneAssigner.Zone myZone;

        NodeLevelZoneSummary(HeatZoneAssigner.Zone myZone) {
            this.myZone = myZone;
            shardProfileSummaries = new ArrayList<>();
        }

        void addShard(ShardProfileSummary shard) {
            shardProfileSummaries.add(shard);
            if (minShard == null) {
                minShard = shard;
            } else {
                if (getMinTemperature().isGreaterThan(shard.getHeatInDimension(profileForDimension))) {
                    minShard = shard;
                }
            }

            if (maxShard == null) {
                maxShard = shard;
            } else {
                if (shard.getHeatInDimension(profileForDimension).isGreaterThan(getMaxTemperature())) {
                    maxShard = shard;
                }
            }
        }

        @Nullable
        TemperatureVector.NormalizedValue getMinTemperature() {
            if (minShard != null) {
                return minShard.getHeatInDimension(profileForDimension);
            }
            return null;
        }

        @Nullable
        TemperatureVector.NormalizedValue getMaxTemperature() {
            if (maxShard != null) {
                return maxShard.getHeatInDimension(profileForDimension);
            }
            return null;
        }

        @Override
        public String toString() {
            return "{"
                    + "myZone=" + myZone
                    + ", shardProfiles=" + shardProfileSummaries
                    + ", minShard=" + minShard
                    + ", maxShard=" + maxShard
                    + '}';
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
            List<GenericSummary> shardSummaries = new ArrayList<>();
            for (ShardProfileSummary shardProfileSummary : shardProfileSummaries) {
                shardSummaries.add(shardProfileSummary);
            }
            return shardSummaries;
        }

        @Override
        public List<Object> getSqlValue() {
            List<Object> values = new ArrayList<>();
            values.add(myZone.name());
            values.add(minShard == null ? "" : minShard.toString());
            values.add(maxShard == null ? "" : maxShard.toString());
            return values;
        }

        @Override
        public JsonElement toJson() {
            JsonObject summaryObj = new JsonObject();
            summaryObj.addProperty("zone_name", myZone.name());
            summaryObj.add("min_shard", minShard.toJson());
            summaryObj.add("max_shard", maxShard.toJson());
            getNestedSummaryList().forEach(
                    summary -> {
                        summaryObj.add(summary.getTableName(), summary.toJson());
                    }
            );
            return summaryObj;
        }
    }
}
