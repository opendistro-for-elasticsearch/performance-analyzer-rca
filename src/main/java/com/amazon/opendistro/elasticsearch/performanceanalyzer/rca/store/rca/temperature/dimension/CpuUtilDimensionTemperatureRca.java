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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.dimension;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.temperature.DimensionalTemperatureFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.ShardStore;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.AvgCpuUtilByShardsMetricBasedTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.byShard.CpuUtilByShardsMetricBasedTemperatureCalculator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.capacity.TotalCpuUtilForTotalNodeMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.metric.temperature.shardIndependent.ShardIndependentTemperatureCalculatorCpuUtilMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.temperature.DimensionalTemperatureCalculator;

public class CpuUtilDimensionTemperatureRca extends Rca<DimensionalTemperatureFlowUnit> {
    private final TotalCpuUtilForTotalNodeMetric CPU_UTIL_PEAK_USAGE;
    private final CpuUtilByShardsMetricBasedTemperatureCalculator CPU_UTIL_BY_SHARD;
    private final AvgCpuUtilByShardsMetricBasedTemperatureCalculator AVG_CPU_UTIL_BY_SHARD;
    private final ShardIndependentTemperatureCalculatorCpuUtilMetric CPU_UTIL_SHARD_INDEPENDENT;

    private final ShardStore shardStore;

    private final TemperatureVector.NormalizedValue THRESHOLD_PERCENT_FOR_HEAT_ZONE_ASSIGNMENT =
            new TemperatureVector.NormalizedValue((short) 2);

    public CpuUtilDimensionTemperatureRca(ShardStore shardStore,
                                          CpuUtilByShardsMetricBasedTemperatureCalculator cpuUtilByShard,
                                          AvgCpuUtilByShardsMetricBasedTemperatureCalculator avgCpuUtilByShards,
                                          ShardIndependentTemperatureCalculatorCpuUtilMetric shardIndependentCpuUtilMetric,
                                          TotalCpuUtilForTotalNodeMetric cpuUtilPeakUsage) {
        super(5);
        this.CPU_UTIL_PEAK_USAGE = cpuUtilPeakUsage;
        this.CPU_UTIL_BY_SHARD = cpuUtilByShard;
        this.CPU_UTIL_SHARD_INDEPENDENT = shardIndependentCpuUtilMetric;
        this.AVG_CPU_UTIL_BY_SHARD = avgCpuUtilByShards;
        this.shardStore = shardStore;
    }

    @Override
    public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
        throw new IllegalArgumentException("Generating from wire is not required.");
    }

    @Override
    public DimensionalTemperatureFlowUnit operate() {
        System.out.println("Executing: " + name());
        DimensionalTemperatureFlowUnit flowUnit = DimensionalTemperatureCalculator.getTemperatureForDimension(
                shardStore,
                TemperatureVector.Dimension.CpuUtil,
                CPU_UTIL_BY_SHARD,
                AVG_CPU_UTIL_BY_SHARD, CPU_UTIL_SHARD_INDEPENDENT, CPU_UTIL_PEAK_USAGE,
                THRESHOLD_PERCENT_FOR_HEAT_ZONE_ASSIGNMENT);
        return flowUnit;
    }
}
