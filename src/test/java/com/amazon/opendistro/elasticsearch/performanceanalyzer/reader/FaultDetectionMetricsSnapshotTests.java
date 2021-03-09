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

import static org.junit.Assert.assertEquals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import java.sql.Connection;
import java.sql.DriverManager;
import org.jooq.BatchBindStep;
import org.jooq.Record;
import org.jooq.Result;
import org.junit.Before;
import org.junit.Test;

public class FaultDetectionMetricsSnapshotTests {
    private static final String DB_URL = "jdbc:sqlite:";
    private Connection conn;

    @Before
    public void setup() throws Exception {
        Class.forName("org.sqlite.JDBC");
        System.setProperty("java.io.tmpdir", "/tmp");
        conn = DriverManager.getConnection(DB_URL);
    }

    @Test
    public void testPutMetrics() {
        FaultDetectionStatsSnapshot faultDetectionMetricsSnapshot =
                new FaultDetectionStatsSnapshot(conn, 1535065195000L);
        BatchBindStep handle = faultDetectionMetricsSnapshot.startBatchPut();

        handle.bind("1.8", "2.0", "2.6", "3.0");
        handle.execute();
        Result<Record> rt = faultDetectionMetricsSnapshot.fetchAll();

        assertEquals(1, rt.size());
        assertEquals(
                "1.0", rt.get(0).get(AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_LATENCY.toString()));
        assertEquals(
                "2.0", rt.get(0).get(AllMetrics.FaultDetectionMetric.FOLLOWER_CHECK_FAILURE.toString()));
        assertEquals(
                "2.6", rt.get(0).get(AllMetrics.FaultDetectionMetric.LEADER_CHECK_LATENCY.toString()));
        assertEquals(
                "3.0", rt.get(0).get(AllMetrics.FaultDetectionMetric.LEADER_CHECK_FAILURE.toString()));
    }
}
