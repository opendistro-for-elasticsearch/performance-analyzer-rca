/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
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
        FaultDetectionMetricsSnapshot faultDetectionMetricsSnapshot =
                new FaultDetectionMetricsSnapshot(conn, 1535065195000L);
        BatchBindStep handle = faultDetectionMetricsSnapshot.startBatchPut();

        handle.bind("1", "sourceNode", "targetNodeId", "follower_check",1535065195000L, null, 0);
        handle.bind("1", "sourceNode", "targetNodeId", "follower_check", null, 1535065195050L, 0);
        handle.execute();
        Result<Record> rt = faultDetectionMetricsSnapshot.fetchAggregatedTable();

        assertEquals(1, rt.size());
        Double latency = Double.parseDouble(rt.get(0).get("sum_" + FaultDetectionMetricsSnapshot.Fields.LAT.toString()).toString());
        assertEquals(50d, latency.doubleValue(), 0);
        assertEquals(
                "sourceNode", rt.get(0).get(AllMetrics.FaultDetectionDimension.SOURCE_NODE_ID.toString()));
        assertEquals(
                "targetNodeId",
                rt.get(0).get(AllMetrics.FaultDetectionDimension.TARGET_NODE_ID.toString()));
        assertEquals(
                "follower_check",
                rt.get(0).get(FaultDetectionMetricsSnapshot.Fields.FAULT_DETECTION_TYPE.toString()));
        assertEquals(
                0,
                Integer.parseInt(rt.get(0).get("sum_" + FaultDetectionMetricsSnapshot.Fields.FAULT.toString()).toString()));
    }
}
