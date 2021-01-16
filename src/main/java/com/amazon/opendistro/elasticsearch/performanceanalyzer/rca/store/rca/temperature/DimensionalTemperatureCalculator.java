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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.temperature.DimensionalTemperatureFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.NodeLevelDimensionalSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.temperature.ShardProfileSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.HeatZoneAssigner;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.ShardStore;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.TemperatureMetricsBase;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.calculators.AvgShardBasedTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.calculators.ShardBasedTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.capacity.ShardTotalDiskUsageTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.capacity.calculators.TotalNodeTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.shardIndependent.DiskUsageShardIndependentTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.shardIndependent.calculators.ShardIndependentTemperatureCalculator;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;
import org.jooq.Result;

public class DimensionalTemperatureCalculator {
    private static final Logger LOG = LogManager.getLogger(DimensionalTemperatureCalculator.class);

    enum ColumnTypes {
        IndexName,
        ShardID,
        sum;
    }

    /**
     * The categorization of shards as hot, warm, lukeWarm and cold based on the average resource
     * utilization of the resource across all shards. This value is not the actual number but a
     * normalized value that is between 0 and 10.
     *
     * <p>The shard independent usage of the resource as a normalized value between 0 and 10.
     *
     * <p>The Node temperature as the actual value. (If normalized to 10 this will always be 10
     * as this is the base for the normalization).
     *
     * @param resourceByShardId        This gives the resource utilization at a shard level
     * @param resourceShardIndependent This is the additional component that use resource but
     *                                 cannot be accounted for at a shard level. For
     *                                 example, HttpServer consuming CPU will form part of this.
     * @param resourcePeakUsage        This is the total usage of the resource at the node level. This
     *                                 should be the sum of the other two.
     * @return The return is a composition of three things:
     */
    public static DimensionalTemperatureFlowUnit getTemperatureForDimension(
            ShardStore shardStore, TemperatureDimension metricType,
            ShardBasedTemperatureCalculator resourceByShardId,
            AvgShardBasedTemperatureCalculator avgResUsageByAllShards,
            ShardIndependentTemperatureCalculator resourceShardIndependent,
            TotalNodeTemperatureCalculator resourcePeakUsage,
            TemperatureVector.NormalizedValue threshold) {
        List<MetricFlowUnit> shardIdBasedFlowUnits = resourceByShardId.getFlowUnits();
        List<MetricFlowUnit> avgResUsageFlowUnits = avgResUsageByAllShards.getFlowUnits();
        List<MetricFlowUnit> shardIdIndependentFlowUnits = resourceShardIndependent.getFlowUnits();
        List<MetricFlowUnit> resourcePeakFlowUnits = resourcePeakUsage.getFlowUnits();

        LOG.info("shardIdBasedFlowUnits: {}", shardIdBasedFlowUnits);
        LOG.info("avgResUsageFlowUnits: {}", avgResUsageFlowUnits);
        LOG.info("shardIdIndependentFlowUnits: {}", shardIdIndependentFlowUnits);
        LOG.info("resourcePeakFlowUnits: {}", resourcePeakFlowUnits);

        // example:
        // [0: [[IndexName, ShardID, sum], [geonames, 0, 0.35558242693567], [geonames, 2, 0.0320651297686606]]]
        if (shardIdBasedFlowUnits.size() != 1) {
            if (shardIdBasedFlowUnits.get(0).isEmpty()) {
                LOG.info("Empty shardIdBasedFlowUnits");
                return new DimensionalTemperatureFlowUnit(System.currentTimeMillis());
            }

            if (shardIdBasedFlowUnits.get(0).getData().get(0).size() != 3) {
                // we expect it to have three columns but the number of rows is determined by the
                // number of indices and shards in the node.
                throw new IllegalArgumentException("Size more than expected: " + shardIdBasedFlowUnits);
            }
        }

        if (avgResUsageFlowUnits.size() != 1) {
            if (avgResUsageFlowUnits.get(0).isEmpty()) {
                LOG.info("Empty avgResUsageFlowUnits");
                return new DimensionalTemperatureFlowUnit(System.currentTimeMillis());
            }

            if (avgResUsageFlowUnits.get(0).getData().intoArrays().length != 1) {
                throw new IllegalArgumentException("Size more than expected:\n" + avgResUsageFlowUnits.get(0).getData()
                        + "\n found: " + avgResUsageFlowUnits.get(0).getData().intoArrays().length);
            }
        }

        /*
        For ShardIndependent flow units and Peak Resource flow units we dont have data for the shard
        Size dimension. Therefore handling it here so as to avoid it from the final calculation.
        */
        if (shardIdIndependentFlowUnits.size() > 1) {
            if (shardIdIndependentFlowUnits.get(0).isEmpty()) {
                LOG.info("Empty shardIdIndependentFlowUnits");
                return new DimensionalTemperatureFlowUnit(System.currentTimeMillis());
            }

            if (shardIdIndependentFlowUnits.get(0).getData().intoArrays().length != 1) {
                throw new IllegalArgumentException("Size more than expected: \n"
                        + shardIdIndependentFlowUnits.get(0).getData());
            }
        }

        if (resourcePeakFlowUnits.size() > 1) {
            if (resourcePeakFlowUnits.get(0).isEmpty()) {
                LOG.info("Empty resourcePeakFlowUnits");
                return new DimensionalTemperatureFlowUnit(System.currentTimeMillis());
            }
            if (resourcePeakFlowUnits.get(0).getData().intoArrays().length != 1) {
                throw new IllegalArgumentException("Size more than expected: \n"
                        + resourcePeakFlowUnits.get(0).getData());
            }
        }

        double avgValOverShards = -1.0;
        try {
            Result<Record> flowUnitData = avgResUsageFlowUnits.get(0).getData();
            if (flowUnitData == null) {
                return new DimensionalTemperatureFlowUnit(System.currentTimeMillis());
            }
            List<Double> values = flowUnitData.getValues(
                AvgShardBasedTemperatureCalculator.SHARD_AVG, Double.class);
            if (values != null && values.get(0) != null) {
                avgValOverShards = values.get(0);
            } else {
                // This means that there are no shards on this node. So we will return an empty FlowUnit.
                return new DimensionalTemperatureFlowUnit(System.currentTimeMillis());
            }
        } catch (Exception ex) {
            LOG.error("DBError getting shard average: {}.", ex);
            return new DimensionalTemperatureFlowUnit(System.currentTimeMillis());
        }

        double totalConsumedInNode = -1.0;
        try {
            Result<Record> flowUnitData = resourcePeakFlowUnits.get(0).getData();
            if (flowUnitData == null) {
                return new DimensionalTemperatureFlowUnit(System.currentTimeMillis());
            }
            List<Double> values = flowUnitData.getValues(
                    TemperatureMetricsBase.AGGR_OVER_AGGR_NAME, Double.class);
            if (values != null && values.get(0) != null) {
                totalConsumedInNode = values.get(0);
            } else {
                // This means that there are no shards on this node. So we will return an empty FlowUnit.
                return new DimensionalTemperatureFlowUnit(System.currentTimeMillis());
            }
        } catch (Exception ex) {
            LOG.error("DBError getting shard average: {}.", ex);
            return new DimensionalTemperatureFlowUnit(System.currentTimeMillis());
        }

        TemperatureVector.NormalizedValue avgUsageAcrossShards =
                TemperatureVector.NormalizedValue.calculate(avgValOverShards, totalConsumedInNode);

        Result<Record> rowsPerShard = shardIdBasedFlowUnits.get(0).getData();

        NodeLevelDimensionalSummary nodeDimensionProfile =
                new NodeLevelDimensionalSummary(metricType, avgUsageAcrossShards, totalConsumedInNode);

        // The shardIdBasedFlowUnits is supposed to contain one row per shard.
        nodeDimensionProfile.setNumberOfShards(rowsPerShard.size());

        for (Record record : rowsPerShard) {
            // Each row has columns like:
            // IndexName, ShardID, sum
            String indexName = record.getValue(ColumnTypes.IndexName.name(), String.class);
            int shardId = record.getValue(ColumnTypes.ShardID.name(), Integer.class);
            double usage = record.getValue(ColumnTypes.sum.name(), Double.class);

            TemperatureVector.NormalizedValue normalizedConsumptionByShard =
                    TemperatureVector.NormalizedValue.calculate(usage, totalConsumedInNode);
            HeatZoneAssigner.Zone heatZoneForShard =
                    HeatZoneAssigner.assign(normalizedConsumptionByShard, avgUsageAcrossShards, threshold);

            ShardProfileSummary shardProfileSummary = shardStore.getOrCreateIfAbsent(indexName, shardId);
            shardProfileSummary.addTemperatureForDimension(metricType, normalizedConsumptionByShard);
            nodeDimensionProfile.addShardToZone(shardProfileSummary, heatZoneForShard);
        }
        
        return new DimensionalTemperatureFlowUnit(System.currentTimeMillis(), nodeDimensionProfile);
    }

    public static DimensionalTemperatureFlowUnit getTemperatureForDimension(
            ShardStore shardStore, TemperatureDimension metricType,
            ShardBasedTemperatureCalculator resourceByShardId,
            AvgShardBasedTemperatureCalculator avgResUsageByAllShards,
            ShardTotalDiskUsageTemperatureCalculator shardSizePeakUsage,
            TemperatureVector.NormalizedValue threshold) {
        DiskUsageShardIndependentTemperatureCalculator diskUsageShardIndependent =
                new DiskUsageShardIndependentTemperatureCalculator();
        return getTemperatureForDimension(shardStore,
                metricType, resourceByShardId, avgResUsageByAllShards, diskUsageShardIndependent, shardSizePeakUsage, threshold);
    }
}
