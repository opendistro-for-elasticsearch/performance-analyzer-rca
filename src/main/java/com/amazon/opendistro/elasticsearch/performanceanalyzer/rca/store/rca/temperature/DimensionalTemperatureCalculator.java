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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.capacity.calculators.TotalNodeTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.shardIndependent.calculators.ShardIndependentTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotshard.IndexShardKey;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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
     *                                 Currently this is not used in the calculation for the mean
     *                                 node temperature. This will be needed whenever we will
     *                                 auto-suggest the movement of shards across nodes. Keeping it for
     *                                 future reference.
     * @param resourcePeakUsage        This is the total usage of the resource at the node level. This
     *                                 should be the sum of the other two.
     * @return The return is a composition of three things:
     */
    public static DimensionalTemperatureFlowUnit getTemperatureForDimension(
            ShardStore shardStore, TemperatureDimension dimension,
            ShardBasedTemperatureCalculator resourceByShardId,
            AvgShardBasedTemperatureCalculator avgResUsageByAllShards,
            ShardIndependentTemperatureCalculator resourceShardIndependent,
            TotalNodeTemperatureCalculator resourcePeakUsage,
            TemperatureVector.NormalizedValue threshold) {

        List<List<MetricFlowUnit>> allFlowUnits = new ArrayList<>();
        List<MetricFlowUnit> shardIdBasedFlowUnits = resourceByShardId.getFlowUnits();
        List<MetricFlowUnit> avgResUsageFlowUnits = avgResUsageByAllShards.getFlowUnits();
        List<MetricFlowUnit> resourcePeakFlowUnits = resourcePeakUsage.getFlowUnits();
        allFlowUnits.add(shardIdBasedFlowUnits);
        allFlowUnits.add(avgResUsageFlowUnits);
        allFlowUnits.add(resourcePeakFlowUnits);

        AtomicBoolean emptyFlowUnit = new AtomicBoolean(false);
        allFlowUnits.forEach(flowUnitAcrossOneCalculator -> {
            if (flowUnitAcrossOneCalculator.get(0).isEmpty()) {
                LOG.debug("Empty flowUnitAcrossOneCalculator");
                emptyFlowUnit.set(true);
            }
        });

        if (emptyFlowUnit.get()) {
            return new DimensionalTemperatureFlowUnit(System.currentTimeMillis());
        }

        AtomicBoolean noEntriesInFlowUnit = new AtomicBoolean(false);
        allFlowUnits.forEach(flowUnitAcrossOneCalculator -> {
            if (flowUnitAcrossOneCalculator.get(0).getData().size() == 0) {
                LOG.debug("No Entries in flowUnits");
                noEntriesInFlowUnit.set(true);
            }
        });

        if (noEntriesInFlowUnit.get()) {
            return new DimensionalTemperatureFlowUnit(System.currentTimeMillis());
        }

        LOG.debug("shardIdBasedFlowUnits " + shardIdBasedFlowUnits.get(0).getData());
        LOG.debug("avgResUsageFlowUnits " + avgResUsageFlowUnits.get(0).getData());
        LOG.debug("resourcePeakFlowUnits " + resourcePeakFlowUnits.get(0).getData());
        if (shardIdBasedFlowUnits.get(0).getData().get(0).size() != 3) {
            // example:
            // [0: [[IndexName, ShardID, sum], [geonames, 0, 0.35558242693567], [geonames, 2, 0.0320651297686606]]]
            // we expect it to have three columns but the number of rows is determined by the
            // number of indices and shards in the node.
            throw new IllegalArgumentException("Size more than expected: " + shardIdBasedFlowUnits);
        }

        if (avgResUsageFlowUnits.get(0).getData().get(0).size() != 1) {
            // example:
            // [0: [shard_avg], [0.0123598766010401]]
            // We expect it to have single column
            throw new IllegalArgumentException("Size more than expected:\n" + avgResUsageFlowUnits.get(0).getData()
                + "\n found: " + avgResUsageFlowUnits.get(0).getData().intoArrays().length);
        }

        if (resourcePeakFlowUnits.get(0).getData().get(0).size() != 1) {
            // example:
            // [0: [SUM_of_max], [0.03236403909818]]
            // We expect it to have single column
            throw new IllegalArgumentException("Size more than expected: \n"
                + resourcePeakFlowUnits.get(0).getData());
        }

        // avgUsageAcrossShards contains the average of resource consumed over all the shards.
        // e.g. If Total CPU consumed by shards is 50% and total number of shards on the node
        // are 5, This would have the value as 10.
        double avgUsageAcrossShards = -1.0;
        try {
            List<Double> values = avgResUsageFlowUnits.get(0).getData().getValues(
                AvgShardBasedTemperatureCalculator.SHARD_AVG, Double.class);
            if (values != null && values.get(0) != null) {
                avgUsageAcrossShards = values.get(0);
            } else {
                // This means that there are no shards on this node. So we will return an empty FlowUnit.
                return new DimensionalTemperatureFlowUnit(System.currentTimeMillis());
            }
        } catch (Exception ex) {
            LOG.error("DBError getting shard average:", ex);
            return new DimensionalTemperatureFlowUnit(System.currentTimeMillis());
        }

        // totalUsageInNode contains the value consumed by the resource on the node.
        double totalUsageInNode = -1.0;
        try {
            List<Double> values = resourcePeakFlowUnits.get(0).getData().getValues(
                    TemperatureMetricsBase.AGGR_OVER_AGGR_NAME, Double.class);
            if (values != null && values.get(0) != null) {
                totalUsageInNode = values.get(0);
            } else {
                // This means that there are no shards on this node. So we will return an empty FlowUnit.
                return new DimensionalTemperatureFlowUnit(System.currentTimeMillis());
            }
        } catch (Exception ex) {
            LOG.error("DBError getting shard average:", ex);
            return new DimensionalTemperatureFlowUnit(System.currentTimeMillis());
        }

        // normalizedConsumptionAcrossShards contains average normalized value (on a scale of 10) the of the resource
        // e.g. If the total resource(CPU) used is 50% and there are 6 shards on the node. 
        // normalizedConsumptionAcrossShards would be equal to ((50/6)*10)/50 = 1.67
        TemperatureVector.NormalizedValue normalizedConsumptionAcrossShards =
                TemperatureVector.NormalizedValue.calculate(avgUsageAcrossShards, totalUsageInNode);

        // valuesOverShards contains the usage of resource over every shard.
        Result<Record> valuesOverShards = shardIdBasedFlowUnits.get(0).getData();

        NodeLevelDimensionalSummary nodeDimensionProfile =
                new NodeLevelDimensionalSummary(dimension, normalizedConsumptionAcrossShards, totalUsageInNode);

        // The shardIdBasedFlowUnits is supposed to contain one row per shard.
        nodeDimensionProfile.setNumberOfShards(valuesOverShards.size());

        for (Record record : valuesOverShards) {
            // Each row has columns like:
            // IndexName, ShardID, sum
            String indexName = record.getValue(ColumnTypes.IndexName.name(), String.class);
            int shardId = record.getValue(ColumnTypes.ShardID.name(), Integer.class);
            double usageByShard = record.getValue(ColumnTypes.sum.name(), Double.class);

            // normalizedConsumptionByShard contains the normalized value of the resource consumed
            // by this shard. e.g. If the If the total resource(CPU) used is 50% and
            // CPU consumed by this shard is 6%, normalizedConsumptionByShard = (6*10/50) = 1.2
            TemperatureVector.NormalizedValue normalizedConsumptionByShard =
                    TemperatureVector.NormalizedValue.calculate(usageByShard, totalUsageInNode);

            // HeatZone for shard contains the zone assigned to this particular shard with respect to the dimension
            // specified. A Shard can be present in different zones for different dimensions.
            HeatZoneAssigner.Zone heatZoneForShard =
                    HeatZoneAssigner.assign(normalizedConsumptionByShard, normalizedConsumptionAcrossShards, threshold);

            IndexShardKey indexShardKey = new IndexShardKey(indexName, shardId);
            ShardProfileSummary shardProfileSummary = shardStore.getOrCreateIfAbsent(indexShardKey);
            shardProfileSummary.addTemperatureForDimension(dimension, normalizedConsumptionByShard);
            nodeDimensionProfile.addShardToZone(shardProfileSummary, heatZoneForShard);
        }
        
        return new DimensionalTemperatureFlowUnit(System.currentTimeMillis(), nodeDimensionProfile);
    }
}
