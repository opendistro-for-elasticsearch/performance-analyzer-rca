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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;

public class FaultDetectionStatsProcessor implements EventProcessor {
    private static final Logger LOG = LogManager.getLogger(FaultDetectionStatsProcessor.class);
    private FaultDetectionStatsSnapshot faultDetectionStatsSnapshot;
    private long startTime;
    private long endTime;
    private BatchBindStep handle;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<HashMap<String, String>> TYPE_REF = new TypeReference<HashMap<String, String>>() {};

    public FaultDetectionStatsProcessor(FaultDetectionStatsSnapshot faultDetectionStatsSnapshot) {
        this.faultDetectionStatsSnapshot = faultDetectionStatsSnapshot;
    }

    static FaultDetectionStatsProcessor buildFaultDetectionStatsProcessor(
            long currWindowStartTime,
            Connection conn,
            NavigableMap<Long, FaultDetectionStatsSnapshot>
                faultDetectionMetricsMap) {
        FaultDetectionStatsSnapshot faultDetectionStatsSnapshot = faultDetectionMetricsMap.get(currWindowStartTime);
        if (faultDetectionStatsSnapshot == null) {
            faultDetectionStatsSnapshot = new FaultDetectionStatsSnapshot(conn, currWindowStartTime);
            faultDetectionMetricsMap.put(currWindowStartTime, faultDetectionStatsSnapshot);
        }
        return new FaultDetectionStatsProcessor(faultDetectionStatsSnapshot);
    }

    @Override
    public void initializeProcessing(long startTime, long endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.handle = faultDetectionStatsSnapshot.startBatchPut();
    }

    @Override
    public void finalizeProcessing() {
        if (handle.size() > 0) {
            handle.execute();
        }
        LOG.debug("Final Fault Detection request metrics {}", faultDetectionStatsSnapshot.fetchAll());
    }

    /**
     * Sample event :
     * ^^fault_detection
     * {"current_time":1566413996768}
     * {"FollowerCheck_Latency":1.28,"LeaderCheck_Latency":1.8,"FollowerCheck_Failure":3.0,"LeaderCheck_Failure":4.0}$
     */
    @Override
    public void processEvent(Event event) {
        String[] lines = event.value.split(System.lineSeparator());
        for (String line : lines) {
            Map<String, String> faultDetectionMap = extractEntryData(line);
            if (!faultDetectionMap.containsKey(PerformanceAnalyzerMetrics.METRIC_CURRENT_TIME)) {
                try {
                    handle.bind(
                            Double.parseDouble(faultDetectionMap.get(
                                    AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_LATENCY.toString())),
                            Double.parseDouble(faultDetectionMap.get(
                                    AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_FAILURE.toString())),
                            Double.parseDouble(faultDetectionMap.get(
                                    AllMetrics.FaultDetectionMetric.LEADER_CHECK_LATENCY.toString())),
                            Double.parseDouble(faultDetectionMap.get(
                                    AllMetrics.FaultDetectionMetric.LEADER_CHECK_FAILURE.toString())));
                } catch (Exception ex) {
                    LOG.error("Fail to get fault detection stats ",  ex);
                }
            }
        }
    }

    static Map<String, String> extractEntryData(String line) {
        try {
            return MAPPER.readValue(line, TYPE_REF);
        } catch (IOException ioe) {
            LOG.error("Error occurred while parsing tmp file", ioe);
        }
        return new HashMap<>();
    }

    @Override
    public boolean shouldProcessEvent(Event event) {
        return event.key.contains(PerformanceAnalyzerMetrics.sFaultDetection);
    }

    @Override
    public void commitBatchIfRequired() {
        if (handle.size() > BATCH_LIMIT) {
            handle.execute();
            handle = faultDetectionStatsSnapshot.startBatchPut();
        }
    }
}
