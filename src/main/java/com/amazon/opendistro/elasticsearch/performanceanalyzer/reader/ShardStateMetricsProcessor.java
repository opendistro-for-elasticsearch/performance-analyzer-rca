package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.tools.StringUtils;

public class ShardStateMetricsProcessor implements EventProcessor {
    private static final Logger LOG = LogManager.getLogger(ShardStateMetricsProcessor.class);
    private ShardStateMetricsSnapshot shardStateMetricsSnapshot;
    private BatchBindStep handle;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<HashMap<String, String>> TYPE_REF = new TypeReference<HashMap<String, String>>() {};

    private ShardStateMetricsProcessor(ShardStateMetricsSnapshot snapshot) {
        this.shardStateMetricsSnapshot = snapshot;
    }

    static ShardStateMetricsProcessor buildShardStateMetricEventsProcessor(
            long currWindowStartTime,
            Connection conn,
            NavigableMap<Long, ShardStateMetricsSnapshot> shardStateEventMetricsMap) {
        ShardStateMetricsSnapshot shardStateSnap = shardStateEventMetricsMap.get(currWindowStartTime);
        if (shardStateSnap == null) {
            shardStateSnap = new ShardStateMetricsSnapshot(conn, currWindowStartTime);
            shardStateEventMetricsMap.put(currWindowStartTime, shardStateSnap);
        }
        return new ShardStateMetricsProcessor(shardStateSnap);
    }

    @Override
    public void initializeProcessing(long startTime, long endTime) {
        this.handle = shardStateMetricsSnapshot.startBatchPut();
    }

    @Override
    public void finalizeProcessing() {
        if (handle.size() > 0) {
            handle.execute();
        }
        LOG.debug("Final ShardStateEvents metrics {}", shardStateMetricsSnapshot.fetchAll());
    }

    /**
     * Sample event :
     * ^shard_state_metrics
     * {"current_time":1600677426860}
     * {IndexName:"pmc"}
     * {"ShardID":2,"ShardType":"p","NodeName":"elasticsearch2","Shard_State":"Unassigned"}
     * {"ShardID":2,"ShardType":"r","NodeName":"elasticsearch2","Shard_State:"Initializing"}
     * {IndexName:"pmc1"}
     * {"ShardID":2,"ShardType":"primary","NodeName":"elasticsearch2","Shard_State":"Unassigned"}
     */
    @Override
    public void processEvent(Event event) {
        String[] lines = event.value.split(System.lineSeparator());
        String indexName = StringUtils.EMPTY;
        for (String line : lines) {
            Map<String, String> shardStateMap = extractEntryData(line);
            if (shardStateMap.containsKey(AllMetrics.ShardStateDimension.INDEX_NAME.toString())) {
                indexName = shardStateMap.get(AllMetrics.ShardStateDimension.INDEX_NAME.toString());
            } else {
                if (!shardStateMap.containsKey(PerformanceAnalyzerMetrics.METRIC_CURRENT_TIME)) {
                    handle.bind(
                            indexName,
                            shardStateMap.get(AllMetrics.ShardStateDimension.SHARD_ID.toString()),
                            shardStateMap.get(AllMetrics.ShardStateDimension.SHARD_TYPE.toString()),
                            shardStateMap.get(AllMetrics.ShardStateDimension.NODE_NAME.toString()),
                            shardStateMap.get(AllMetrics.ShardStateDimension.SHARD_STATE.toString()));
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
        return event.key.contains(PerformanceAnalyzerMetrics.sShardStatePath);
    }

    @Override
    public void commitBatchIfRequired() {
        if (handle.size() > BATCH_LIMIT) {
            handle.execute();
            handle = shardStateMetricsSnapshot.startBatchPut();
        }
    }
}
