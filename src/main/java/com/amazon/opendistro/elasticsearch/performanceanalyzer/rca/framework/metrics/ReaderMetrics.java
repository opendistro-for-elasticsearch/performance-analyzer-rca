/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

public enum ReaderMetrics implements MeasurementSet {

    /**
     * Size of generated metricsdb files.
     */
    METRICSDB_FILE_SIZE("MetricsdbFileSize", "bytes", Arrays.asList(Statistics.MAX, Statistics.MEAN)),

    /**
     * Number of compressed and uncompressed metricsdb files.
     */
    METRICSDB_NUM_FILES("MetricsdbNumFiles", "count", Statistics.SAMPLE),

    /**
     * Size of compressed and uncompressed metricsdb files.
     */
    METRICSDB_SIZE_FILES("MetricsdbSizeFiles", "bytes", Statistics.SAMPLE),

    /**
     * Number of uncompressed metricsdb files.
     */
    METRICSDB_NUM_UNCOMPRESSED_FILES("MetricsdbNumUncompressedFiles", "count", Statistics.SAMPLE),

    /**
     * Size of uncompressed metricsdb files.
     */
    METRICSDB_SIZE_UNCOMPRESSED_FILES("MetricsdbSizeUncompressedFiles", "bytes", Statistics.SAMPLE),

    /**
     * Whether or not batch metrics is enabled (0 for enabled, 1 for disabled).
     */
    BATCH_METRICS_ENABLED("BatchMetricsEnabled", "count", Statistics.SAMPLE),

    /**
     * Number of http requests where the client gave a bad request.
     */
    BATCH_METRICS_HTTP_CLIENT_ERROR("BatchMetricsHttpClientError", "count", Statistics.COUNT),

    /**
     * Number of http requests where the host could not generate a correct response.
     */
    BATCH_METRICS_HTTP_HOST_ERROR("BatchMetricsHttpHostError", "count", Statistics.COUNT),

    /**
     * Number of successful queries.
     */
    BATCH_METRICS_HTTP_SUCCESS("BatchMetricsHttpSuccess", "count", Statistics.COUNT),

    /**
     * Number of times a query for batch metrics exceeded the maximum number of requestable datapoints.
     */
    BATCH_METRICS_EXCEEDED_MAX_DATAPOINTS("ExceededBatchMetricsMaxDatapoints", "count", Statistics.COUNT),

    /**
     * Amount of time required to process valid batch metrics requests.
     */
    BATCH_METRICS_QUERY_PROCESSING_TIME("BatchMetricsQueryProcessingTime", "millis",
        Arrays.asList(Statistics.MAX, Statistics.MEAN, Statistics.SUM)),

    /**
     * Amount of time taken to emit Shard State metrics.
     */
    SHARD_STATE_EMITTER_EXECUTION_TIME("ShardStateEmitterExecutionTime", "millis",
        Arrays.asList(Statistics.MAX, Statistics.MIN, Statistics.MEAN, Statistics.COUNT, Statistics.SUM)),

    /**
     * Amount of time taken to emit Master throttling metrics.
     */
    MASTER_THROTTLING_EMITTER_EXECUTION_TIME("MasterThrottlingEmitterExecutionTime", "millis",
        Arrays.asList(Statistics.MAX, Statistics.MIN, Statistics.MEAN, Statistics.COUNT, Statistics.SUM)),

    FAULT_DETECTION_METRICS_EMITTER_EXECUTION_TIME("FaultDetectionMetricsEmitterExecutionTime", "millis",
        Arrays.asList(Statistics.MAX, Statistics.MIN, Statistics.MEAN, Statistics.COUNT, Statistics.SUM));
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

    ReaderMetrics(String name, String unit, List<Statistics> stats) {
        this.name = name;
        this.unit = unit;
        this.statsList = stats;
    }

    ReaderMetrics(String name, String unit, Statistics stats) {
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
