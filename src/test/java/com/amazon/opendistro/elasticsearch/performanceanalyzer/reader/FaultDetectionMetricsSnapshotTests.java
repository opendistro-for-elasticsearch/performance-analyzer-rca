package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import org.jooq.BatchBindStep;
import org.jooq.Record;
import org.jooq.Result;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;

import static org.junit.Assert.assertEquals;

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
                Integer.parseInt(rt.get(0).get("sum_" + FaultDetectionMetricsSnapshot.Fields.ERROR.toString()).toString()));
    }
}
