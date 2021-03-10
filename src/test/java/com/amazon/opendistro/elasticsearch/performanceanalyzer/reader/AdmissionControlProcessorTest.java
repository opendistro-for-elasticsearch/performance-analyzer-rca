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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.MetricStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.AdmissionControlSnapshot.Fields;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jooq.Record;
import org.jooq.Result;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AdmissionControlProcessorTest {

    private static final String DB_URL = "jdbc:sqlite:";
    private static final String TEST_CONTROLLER = "testController";
    private static final String TEST_REJECTION_COUNT = "1";

    private AdmissionControlProcessor admissionControlProcessor;
    private long currentTimestamp;
    private NavigableMap<Long, AdmissionControlSnapshot> snapshotMap;
    Connection connection;

    @Before
    public void setup() throws Exception {
        Class.forName("org.sqlite.JDBC");
        System.setProperty("java.io.tmpdir", "/tmp");
        connection = DriverManager.getConnection(DB_URL);
        this.currentTimestamp = System.currentTimeMillis();
        this.snapshotMap = new TreeMap<>();
        this.admissionControlProcessor = AdmissionControlProcessor.build(currentTimestamp, connection, snapshotMap);
    }

    @Test
    public void testHandleEvent() throws Exception {
        Event testEvent = buildTestAdmissionControlEvent();

        admissionControlProcessor.initializeProcessing(currentTimestamp, System.currentTimeMillis());

        assertTrue(admissionControlProcessor.shouldProcessEvent(testEvent));

        admissionControlProcessor.processEvent(testEvent);
        admissionControlProcessor.finalizeProcessing();

        AdmissionControlSnapshot snap = snapshotMap.get(currentTimestamp);
        Result<Record> result = snap.fetchAll();
        assertEquals(1, result.size());
        assertEquals(TEST_CONTROLLER, result.get(0).get(Fields.CONTROLLER_NAME.toString()));
        assertEquals(Long.parseLong(TEST_REJECTION_COUNT), result.get(0).get(Fields.REJECTION_COUNT.toString()));
    }

    private Event buildTestAdmissionControlEvent() {
        long currentTimeMillis = System.currentTimeMillis();
        String admissionControlEvent =
                new AdmissionControlMetrics(
                        TEST_CONTROLLER, 0L, 0L, Long.parseLong(TEST_REJECTION_COUNT))
                        .serialize();
        return new Event(PerformanceAnalyzerMetrics.sAdmissionControlMetricsPath, admissionControlEvent, currentTimeMillis);
    }

    static class AdmissionControlMetrics extends MetricStatus {

        private String controllerName;
        private long currentValue;
        private long thresholdValue;
        private long rejectionCount;

        public AdmissionControlMetrics() {
            super();
        }

        public AdmissionControlMetrics(String controllerName, long currentValue, long thresholdValue, long rejectionCount) {
            super();
            this.controllerName = controllerName;
            this.currentValue = currentValue;
            this.thresholdValue = thresholdValue;
            this.rejectionCount = rejectionCount;
        }

        @JsonProperty(AllMetrics.AdmissionControlDimension.Constants.CONTROLLER_NAME)
        public String getControllerName() {
            return controllerName;
        }

        @JsonProperty(AllMetrics.AdmissionControlValue.Constants.CURRENT_VALUE)
        public long getCurrentValue() {
            return currentValue;
        }

        @JsonProperty(AllMetrics.AdmissionControlValue.Constants.THRESHOLD_VALUE)
        public long getThresholdValue() {
            return thresholdValue;
        }

        @JsonProperty(AllMetrics.AdmissionControlValue.Constants.REJECTION_COUNT)
        public long getRejectionCount() {
            return rejectionCount;
        }
    }
}
