package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import java.io.File;
import java.sql.Connection;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;

public class FaultDetectionMetricsProcessor implements EventProcessor {
    private static final Logger LOG = LogManager.getLogger(FaultDetectionMetricsProcessor.class);
    private FaultDetectionMetricsSnapshot faultDetectionMetricsSnapshot;
    private long startTime;
    private long endTime;
    private BatchBindStep handle;

    public FaultDetectionMetricsProcessor(FaultDetectionMetricsSnapshot faultDetectionMetricsSnapshot) {
        this.faultDetectionMetricsSnapshot = faultDetectionMetricsSnapshot;
    }

    static FaultDetectionMetricsProcessor buildFaultDetectionMetricsProcessor(
            long currWindowStartTime,
            Connection conn,
            NavigableMap<Long, FaultDetectionMetricsSnapshot> faultDetectionMetricsMap) {
        if (faultDetectionMetricsMap.get(currWindowStartTime) == null) {
            FaultDetectionMetricsSnapshot faultDetectionMetricsSnapshot =
                    new FaultDetectionMetricsSnapshot(conn, currWindowStartTime);
            Map.Entry<Long, FaultDetectionMetricsSnapshot> entry = faultDetectionMetricsMap.lastEntry();
            if (entry != null) {
                faultDetectionMetricsSnapshot.rolloverInFlightRequests(entry.getValue());
            }
            faultDetectionMetricsMap.put(currWindowStartTime, faultDetectionMetricsSnapshot);
            return new FaultDetectionMetricsProcessor(faultDetectionMetricsSnapshot);
        } else {
            return new FaultDetectionMetricsProcessor(faultDetectionMetricsMap.get(currWindowStartTime));
        }
    }

    @Override
    public void initializeProcessing(long startTime, long endTime) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.handle = faultDetectionMetricsSnapshot.startBatchPut();
    }

    @Override
    public void finalizeProcessing() {
        if (handle.size() > 0) {
            handle.execute();
        }
        LOG.debug("Final Fault Detection request metrics {}", faultDetectionMetricsSnapshot.fetchAll());
    }

    @Override
    public void processEvent(Event event) {
        String[] keyItems = event.key.split(File.separatorChar == '\\' ? "\\\\" : File.separator);

        if (keyItems[0].equals(PerformanceAnalyzerMetrics.sFaultDetection)) {
            if (keyItems[3].equals(PerformanceAnalyzerMetrics.START_FILE_NAME)) {
                emitStartMetric(event, keyItems);
            } else if (keyItems[3].equals(PerformanceAnalyzerMetrics.FINISH_FILE_NAME)) {
                emitFinishMetric(event, keyItems);
            }
        }
    }

    @Override
    public boolean shouldProcessEvent(Event event) {
        return event.key.contains(PerformanceAnalyzerMetrics.sFaultDetection);
    }

    @Override
    public void commitBatchIfRequired() {
        if (handle.size() > BATCH_LIMIT) {
            handle.execute();
            handle = faultDetectionMetricsSnapshot.startBatchPut();
        }
    }

    // A keyItem is of the form : [fault_detection, follower_check, 76532, start]
    //
    // Example value part of the entry is:
    // current_time:1566413979979
    // StartTime:1566413987986
    // SourceNodeID:g52i9a93a762cd59dda8d3379b09a752a
    // TargetNodeID:b2a5a93a762cd59dda8d3379b09a752a
    // $
    private void emitStartMetric(Event entry, String[] keyItems) {
        Map<String, String> keyValueMap = ReaderMetricsProcessor.extractEntryData(entry.value);

        String sourceNodeId = keyValueMap.get(AllMetrics.FaultDetectionDimension.SOURCE_NODE_ID.toString());
        String targetNodeId = keyValueMap.get(AllMetrics.FaultDetectionDimension.TARGET_NODE_ID.toString());
        String startTimeVal = keyValueMap.get(AllMetrics.CommonMetric.START_TIME.toString());
        String fault_detection_type = keyItems[1];
        try {
            long st = Long.parseLong(startTimeVal);
            // A keyItem is of the form : [fault_detection, follower_check, 76543, start]
            String rid = keyItems[2];
            handle.bind(rid, sourceNodeId, targetNodeId, fault_detection_type, st, null, 0);
        } catch (NumberFormatException e) {
            LOG.error("Unable to parse string. StartTime:{}", startTimeVal);
            StatsCollector.instance().logException(StatExceptionCode.READER_PARSER_ERROR);
            throw e;
        }
    }

    // A keyItem is of the form : [threads, http, bulk, 43369, start]
    //
    // Example value part of the entry is:
    // current_time:1566413979979
    // StartTime:1566413987986
    // SourceNodeID:g52i9a93a762cd59dda8d3379b09a752a
    // TargetNodeID:b2a5a93a762cd59dda8d3379b09a752a
    // $
    private void emitFinishMetric(Event entry, String[] keyItems) {
        Map<String, String> keyValueMap = ReaderMetricsProcessor.extractEntryData(entry.value);

        String sourceNodeId = keyValueMap.get(AllMetrics.FaultDetectionDimension.SOURCE_NODE_ID.toString());
        String targetNodeId = keyValueMap.get(AllMetrics.FaultDetectionDimension.TARGET_NODE_ID.toString());
        String finishTimeVal = keyValueMap.get(AllMetrics.CommonMetric.FINISH_TIME.toString());
        String errorString = keyValueMap.get(PerformanceAnalyzerMetrics.ERROR);
        String fault_detection_type = keyItems[1];
        try {
            long et = Long.parseLong(finishTimeVal);
            int error = Integer.parseInt(errorString);
            // A keyItem is of the form : [fault_detection, follower_check, 76543, finish]
            String rid = keyItems[2];
            handle.bind(rid, sourceNodeId, targetNodeId, fault_detection_type, null, et, error);
        } catch (NumberFormatException e) {
            LOG.error("Unable to parse string. StartTime:{}, Error:{}", finishTimeVal, errorString);
            StatsCollector.instance().logException(StatExceptionCode.READER_PARSER_ERROR);
            throw e;
        }
    }
}
