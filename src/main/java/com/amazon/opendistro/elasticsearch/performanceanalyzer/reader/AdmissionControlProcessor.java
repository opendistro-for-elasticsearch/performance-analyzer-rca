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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.AdmissionControlDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.AdmissionControlValue;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.JsonConverter;
import org.jooq.BatchBindStep;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableMap;

public class AdmissionControlProcessor implements EventProcessor {

    private AdmissionControlSnapshot admissionControlSnapshot;
    private BatchBindStep handle;
    // private long startTime;
    // private long endTime;

    private AdmissionControlProcessor(AdmissionControlSnapshot admissionControlSnapshot) {
        this.admissionControlSnapshot = admissionControlSnapshot;
    }

    static AdmissionControlProcessor build(
            long currentWindowStartTime,
            Connection connection,
            NavigableMap<Long, AdmissionControlSnapshot> admissionControlSnapshotNavigableMap
    ) {

        if (admissionControlSnapshotNavigableMap.get(currentWindowStartTime) == null) {
            AdmissionControlSnapshot admissionControlSnapshot = new AdmissionControlSnapshot(
                    connection,
                    currentWindowStartTime
            );
            admissionControlSnapshotNavigableMap.put(currentWindowStartTime, admissionControlSnapshot);
            return new AdmissionControlProcessor(admissionControlSnapshot);
        }

        return new AdmissionControlProcessor(admissionControlSnapshotNavigableMap.get(currentWindowStartTime));
    }

    @Override
    public void initializeProcessing(long startTime, long endTime) {
        // this.startTime = startTime;
        // this.endTime = endTime;
        this.handle = admissionControlSnapshot.startBatchPut();
    }

    @Override
    public void finalizeProcessing() {
        if (handle.size() > 0) {
            handle.execute();
        }
    }

    @Override
    public void processEvent(Event event) {
        processEvent(event.value);
        if (handle.size() == 500) {
            handle.execute();
            handle = admissionControlSnapshot.startBatchPut();
        }
    }

    private void processEvent(String eventValue) {
        String[] lines = eventValue.split(System.lineSeparator());
        Arrays.stream(lines).forEach(line -> {
            Map<String, Object> map = JsonConverter.createMapFrom(line);
            String controller = (String) map.get(AdmissionControlDimension.Constants.CONTROLLER_NAME);
            long rejectionCount = getRejectionCount(map);
            handle.bind(controller, rejectionCount);
        });
    }

    private long getRejectionCount(Map<String, Object> map) {
        Object rejectionCountObject = map.get(AdmissionControlValue.Constants.REJECTION_COUNT);
        return rejectionCountObject instanceof String
                ? Long.parseLong((String)rejectionCountObject)
                : ((Number)rejectionCountObject).longValue();
    }

    @Override
    public boolean shouldProcessEvent(Event event) {
        return event.key.contains(PerformanceAnalyzerMetrics.sAdmissionControlMetricsPath);
    }

    @Override
    public void commitBatchIfRequired() {
        if (handle.size() >= BATCH_LIMIT) {
            handle.execute();
            handle = admissionControlSnapshot.startBatchPut();
        }
    }
}
