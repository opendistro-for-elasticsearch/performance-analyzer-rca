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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.handler;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsRequest;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ReaderMetricsProcessor;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;
import org.jooq.Result;

public class MetricsServerHandler {
  private static final Logger LOG = LogManager.getLogger(MetricsServerHandler.class);

  public MetricsServerHandler() {}

  public void collectAPIData(
      MetricsRequest request, StreamObserver<MetricsResponse> responseObserver) {
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

  public void collectStats(
      MetricsDB db,
      Long dbTimestamp,
      List<String> metricList,
      List<String> aggList,
      List<String> dimList,
      StreamObserver<MetricsResponse> responseObserver)
      throws Exception {
    String localResponse;
    if (db != null) {
      Result<Record> metricResult = db.queryMetric(metricList, aggList, dimList);
      if (metricResult == null) {
        localResponse = "{}";
      } else {
        localResponse = metricResult.formatJSON();
      }
    } else {
      // Empty JSON.
      localResponse = "{}";
    }
    String localResponseWithTimestamp =
        String.format("{\"timestamp\": %d, \"data\": %s}", dbTimestamp, localResponse);
    sendResponse(localResponseWithTimestamp, responseObserver);
  }

  private void sendResponse(String result, StreamObserver<MetricsResponse> responseObserver) {
    responseObserver.onNext(MetricsResponse.newBuilder().setMetricsResult(result).build());
    responseObserver.onCompleted();
  }
}
