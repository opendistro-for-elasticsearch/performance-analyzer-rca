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

public class MasterThrottlingMetricsSnapshotTests {
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
        MasterThrottlingMetricsSnapshot masterThrottlingMetricsSnapshot =
                new MasterThrottlingMetricsSnapshot(conn, 1535065195000L);
        BatchBindStep handle = masterThrottlingMetricsSnapshot.startBatchPut();

        handle.bind(1, 5);
        handle.execute();
        Result<Record> rt = masterThrottlingMetricsSnapshot.fetchAggregatedMetrics();

        assertEquals(1, rt.size());
        Double total_throttled = Double.parseDouble(rt.get(0).get(
                "max_" + AllMetrics.MasterThrottlingValue.MASTER_THROTTLED_PENDING_TASK_COUNT
                .toString()).toString());
        assertEquals(
                5.0, total_throttled.doubleValue(), 0);

        Double retrying_task = Double.parseDouble(rt.get(0).get(
                "max_" + AllMetrics.MasterThrottlingValue.DATA_RETRYING_TASK_COUNT
                .toString()).toString());
        assertEquals(
                1.0, retrying_task.doubleValue(), 0);
    }
}
