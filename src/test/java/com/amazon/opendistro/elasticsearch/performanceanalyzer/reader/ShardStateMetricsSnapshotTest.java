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

public class ShardStateMetricsSnapshotTest {
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
        ShardStateMetricsSnapshot shardStateMetricsSnapshot =
                new ShardStateMetricsSnapshot(conn, 1535065195000L);
        BatchBindStep handle = shardStateMetricsSnapshot.startBatchPut();

        handle.bind("indexName", "shardId", "primary","nodeName", 1, 0, 0);
        handle.execute();
        Result<Record> rt = shardStateMetricsSnapshot.fetchAggregatedShardStateMetrics();

        assertEquals(1, rt.size());
        Double shard_active = Double.parseDouble(rt.get(0).get("sum_" + AllMetrics.ShardStateValue.SHARD_STATE_ACTIVE
                .toString()).toString());
        assertEquals(
                1.0, shard_active.doubleValue(),0);
        assertEquals(
                "indexName", rt.get(0).get(AllMetrics.ShardStateDimension.INDEX_NAME.toString()));
        assertEquals(
                "shardId",
                rt.get(0).get(AllMetrics.ShardStateDimension.SHARD_ID.toString()));
        assertEquals(
                "primary",
                rt.get(0).get(AllMetrics.ShardStateDimension.SHARD_TYPE.toString()));
        assertEquals(
                "nodeName",
                rt.get(0).get(AllMetrics.ShardStateDimension.NODE_NAME.toString()));
    }
}