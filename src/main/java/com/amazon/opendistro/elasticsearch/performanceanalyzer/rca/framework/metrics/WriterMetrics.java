/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.eval.Statistics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.measurements.MeasurementSet;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public enum WriterMetrics implements MeasurementSet {
    SHARD_STATE_COLLECTOR_EXECUTION_TIME("ShardStateCollectorExecutionTime", "millis", Arrays.asList(
            Statistics.MAX, Statistics.MIN, Statistics.MEAN, Statistics.COUNT, Statistics.SUM)),

    MASTER_THROTTLING_COLLECTOR_EXECUTION_TIME("MasterThrottlingCollectorExecutionTime", "millis", Arrays.asList(
            Statistics.MAX, Statistics.MIN, Statistics.MEAN, Statistics.COUNT, Statistics.SUM)),

    MASTER_THROTTLING_COLLECTOR_NOT_AVAILABLE("MasterThrottlingCollectorNotAvailable", "count", Arrays.asList(
            Statistics.MAX, Statistics.MIN, Statistics.MEAN, Statistics.COUNT, Statistics.SUM)),

    ADMISSION_CONTROL_COLLECTOR_EXECUTION_TIME("AdmissionControlCollectorExecutionTime", "millis", Arrays.asList(
            Statistics.MAX, Statistics.MIN, Statistics.MEAN, Statistics.COUNT, Statistics.SUM)),

    ADMISSION_CONTROL_COLLECTOR_NOT_AVAILABLE("AdmissionControlCollectorNotAvailable", "count", Arrays.asList(
            Statistics.MAX, Statistics.MIN, Statistics.MEAN, Statistics.COUNT, Statistics.SUM)),

    FAULT_DETECTION_COLLECTOR_EXECUTION_TIME("FaultDetectionCollectorExecutionTime", "millis", Arrays.asList(
            Statistics.MAX, Statistics.MIN, Statistics.MEAN, Statistics.COUNT, Statistics.SUM)),

    CLUSTER_APPLIER_SERVICE_STATS_COLLECTOR_EXECUTION_TIME("ClusterApplierServiceStatsCollectorExecutionTime",
            "millis", Arrays.asList(Statistics.MAX, Statistics.MIN, Statistics.MEAN, Statistics.COUNT,
            Statistics.SUM)),

    SHARD_INDEXING_PRESSURE_COLLECTOR_EXECUTION_TIME("ShardIndexingPressureCollectorExecutionTime", "millis", Arrays.asList(
        Statistics.MAX, Statistics.MIN, Statistics.MEAN, Statistics.COUNT, Statistics.SUM)),

    STALE_METRICS("StaleMetrics", "count", Arrays.asList(Statistics.COUNT)),
    ;

    /** What we want to appear as the metric name. */
    private String name;

    /**
     * The unit the measurement is in. This is not used for the statistics calculations but as an
     * information that will be dumped with the metrics.
     */
    private String unit;

    /**
     * Multiple statistics can be collected for each measurement like MAX, MIN and MEAN. This is a
     * collection of one or more such statistics.
     */
    private List<Statistics> statsList;

    WriterMetrics(String name, String unit, List<Statistics> stats) {
        this.name = name;
        this.unit = unit;
        this.statsList = stats;
    }

    WriterMetrics(String name, String unit, Statistics stats) {
        this(name, unit, Collections.singletonList(stats));
    }

    public String toString() {
        return new StringBuilder(name).append("-").append(unit).toString();
    }

    @Override
    public List<Statistics> getStatsList() {
        return statsList;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getUnit() {
        return unit;
    }
}
