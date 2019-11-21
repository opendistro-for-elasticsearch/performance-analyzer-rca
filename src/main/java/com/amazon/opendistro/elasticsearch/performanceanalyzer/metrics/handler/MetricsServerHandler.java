package com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.handler;

import java.util.List;
import java.util.Map;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;
import org.jooq.Result;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsRequest;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ReaderMetricsProcessor;

public class MetricsServerHandler {
    private static final Logger LOG = LogManager.getLogger(MetricsServerHandler.class);
    public MetricsServerHandler() { }

    public void collectAPIData(MetricsRequest request,
                                StreamObserver<MetricsResponse> responseObserver) {
        try {
            ReaderMetricsProcessor mp = ReaderMetricsProcessor.getInstance();
            Map.Entry<Long, MetricsDB> dbEntry = mp.getMetricsDB();

            MetricsDB db = dbEntry.getValue();
            Long dbTimestamp = dbEntry.getKey();

            List<String> metricList = request.getMetricListList();
            List<String> aggList = request.getAggListList();
            List<String> dimList = request.getDimListList();

            collectStats(db, dbTimestamp, metricList, aggList, dimList, responseObserver);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void collectStats(MetricsDB db, Long dbTimestamp, List<String> metricList,
                              List<String> aggList, List<String> dimList,
        StreamObserver<MetricsResponse> responseObserver) throws Exception {
        String localResponse;
        if (db != null) {
            Result<Record> metricResult = db.queryMetric(
                    metricList, aggList, dimList);
            if (metricResult == null) {
                localResponse = "{}";
            } else {
                localResponse = metricResult.formatJSON();
            }
        } else {
            //Empty JSON.
            localResponse = "{}";
        }
        String localResponseWithTimestamp = String.format("{\"timestamp\": %d, \"data\": %s}", dbTimestamp, localResponse);
        sendResponse(localResponseWithTimestamp, responseObserver);
    }

    private void sendResponse(String result,
        StreamObserver<MetricsResponse> responseObserver) {
        responseObserver.onNext(MetricsResponse.newBuilder().setMetricsResult(result).build());
        responseObserver.onCompleted();
    }
}
