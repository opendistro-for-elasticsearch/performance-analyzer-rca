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

        handle.bind("indexName", "shardId", "p","nodeName","Unassigned");
        handle.execute();
        Result<Record> rt = shardStateMetricsSnapshot.fetchAggregatedShardStateMetrics();

        assertEquals(1, rt.size());
        String shard_state = rt.get(0).get(AllMetrics.ShardStateValue.SHARD_STATE.toString()).toString();
        assertEquals(
                "Unassigned", shard_state);
        assertEquals(
                "indexName", rt.get(0).get(AllMetrics.ShardStateDimension.INDEX_NAME.toString()));
        assertEquals(
                "shardId",
                rt.get(0).get(AllMetrics.ShardStateDimension.SHARD_ID.toString()));
        assertEquals(
                "p",
                rt.get(0).get(AllMetrics.ShardStateDimension.SHARD_TYPE.toString()));
        assertEquals(
                "nodeName",
                rt.get(0).get(AllMetrics.ShardStateDimension.NODE_NAME.toString()));
    }
}