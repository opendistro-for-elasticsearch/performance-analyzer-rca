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

public class MasterThrottlingMetricsEventProcessor implements EventProcessor {
    private static final Logger LOG = LogManager.getLogger(MasterThrottlingMetricsEventProcessor.class);
    private final MasterThrottlingMetricsSnapshot masterThrottlingMetricsSnapshot;
    private BatchBindStep handle;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<HashMap<String, String>> TYPE_REF = new TypeReference<HashMap<String, String>>() {};

    private MasterThrottlingMetricsEventProcessor(MasterThrottlingMetricsSnapshot snapshot) {
        this.masterThrottlingMetricsSnapshot = snapshot;
    }

    static MasterThrottlingMetricsEventProcessor buildMasterThrottlingMetricEventsProcessor(
            long currWindowStartTime,
            Connection conn,
            NavigableMap<Long, MasterThrottlingMetricsSnapshot> masterThroEventMetricsMap) {
        MasterThrottlingMetricsSnapshot masterThrottlingSnapshot = masterThroEventMetricsMap.get(currWindowStartTime);
        if (masterThrottlingSnapshot == null) {
            masterThrottlingSnapshot = new MasterThrottlingMetricsSnapshot(conn, currWindowStartTime);
            masterThroEventMetricsMap.put(currWindowStartTime, masterThrottlingSnapshot);
        }
        return new MasterThrottlingMetricsEventProcessor(masterThrottlingSnapshot);
    }

    @Override
    public void initializeProcessing(long startTime, long endTime) {
        this.handle = masterThrottlingMetricsSnapshot.startBatchPut();
    }

    @Override
    public void finalizeProcessing() {
        if (handle.size() > 0) {
            handle.execute();
        }
        LOG.debug("Final Master Throttling metrics {}", masterThrottlingMetricsSnapshot.fetchAll());
    }

    /**
     * Sample event:
     * ^master_throttling_metrics
     * {"current_time":1602617137529}
     * {"Data_RetryingPendingTasksCount":0,"Master_ThrottledPendingTasksCount":0}$
     *
     * @param event event
     */
    @Override
    public void processEvent(Event event) {
        String[] lines = event.value.split(System.lineSeparator());
        for (String line : lines) {
            Map<String, String> masterThrottlingMap = extractEntryData(line);
            if (!masterThrottlingMap.containsKey(PerformanceAnalyzerMetrics.METRIC_CURRENT_TIME)) {
                try {
                    handle.bind(
                            Long.parseLong(masterThrottlingMap.get(
                                    AllMetrics.MasterThrottlingValue.DATA_RETRYING_TASK_COUNT.toString())),
                            Long.parseLong(masterThrottlingMap.get(
                                    AllMetrics.MasterThrottlingValue.MASTER_THROTTLED_PENDING_TASK_COUNT.toString())));
                } catch (Exception ex) {
                    LOG.error( "Fail to get master throttling metrics ",  ex);
                }
            }
        }
    }

    @Override
    public boolean shouldProcessEvent(Event event) {
        return event.key.contains(PerformanceAnalyzerMetrics.sMasterThrottledTasksPath);
    }

    @Override
    public void commitBatchIfRequired() {
        if (handle.size() > BATCH_LIMIT) {
            handle.execute();
            handle = masterThrottlingMetricsSnapshot.startBatchPut();
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
}

