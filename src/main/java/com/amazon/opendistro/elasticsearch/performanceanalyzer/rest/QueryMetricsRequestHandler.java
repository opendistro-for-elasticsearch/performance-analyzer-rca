/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rest;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsRequest;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.MetricsResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsRestUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.model.MetricAttributes;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.model.MetricsModel;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ReaderMetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.JsonConverter;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.jooq.Record;
import org.jooq.Result;

/**
 * Request handler that supports querying MetricsDB on every EC2 instance. Example query –
 * "http://localhost:9600/_metricsdb?metrics=cpu,rss,memory%20agg=sum,avg,sum%20dims=index,operation,shard."
 * We can fetch multiple metrics using this interface and also specify the dimensions/aggregations
 * for fetching the metrics. We create a new metricsDB every 5 seconds and API only supports
 * querying the latest snapshot.
 */
public class QueryMetricsRequestHandler extends MetricsHandler implements HttpHandler {

  private static final Logger LOG = LogManager.getLogger(QueryMetricsRequestHandler.class);
  private static final int TIME_OUT_VALUE = 2;
  private static final TimeUnit TIME_OUT_UNIT = TimeUnit.SECONDS;
  private NetClient netClient;
  MetricsRestUtil metricsRestUtil;

  public QueryMetricsRequestHandler(NetClient netClient, MetricsRestUtil metricsRestUtil) {
    this.netClient = netClient;
    this.metricsRestUtil = metricsRestUtil;
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    String requestMethod = exchange.getRequestMethod();
    LOG.info(
        "{} {} {}",
        exchange.getRequestMethod(),
        exchange.getRemoteAddress(),
        exchange.getRequestURI());
    ReaderMetricsProcessor mp = ReaderMetricsProcessor.getInstance();
    if (mp == null) {
      sendResponse(
          exchange,
          "{\"error\":\"Metrics Processor is not initialized. The reader has run into an issue or has just started.\"}",
          HttpURLConnection.HTTP_UNAVAILABLE);

      LOG.warn(
          "Metrics Processor is not initialized. The reader has run into an issue or has just started.");
      return;
    }

    Map.Entry<Long, MetricsDB> dbEntry = mp.getMetricsDB();
    if (dbEntry == null) {
      sendResponse(
          exchange,
          "{\"error\":\"There are no metrics databases. The reader has run into an issue or has just started.\"}",
          HttpURLConnection.HTTP_UNAVAILABLE);

      LOG.warn(
          "There are no metrics databases. The reader has run into an issue or has just started.");
      return;
    }
    MetricsDB db = dbEntry.getValue();
    Long dbTimestamp = dbEntry.getKey();

    if (requestMethod.equalsIgnoreCase("GET")) {
      LOG.debug("Query handler called.");

      if (isUnitLookUp(exchange)) {
        getMetricUnits(exchange);
        return;
      }

      Map<String, String> params = getParamsMap(exchange.getRequestURI().getQuery());

      exchange.getResponseHeaders().set("Content-Type", "application/json");
      try {

        String nodes = params.get("nodes");
        List<String> metricList = metricsRestUtil.parseArrayParam(params, "metrics", false);
        List<String> aggList = metricsRestUtil.parseArrayParam(params, "agg", false);
        List<String> dimList = metricsRestUtil.parseArrayParam(params, "dim", true);

        if (metricList.size() != aggList.size()) {
          sendResponse(
              exchange,
              "{\"error\":\"metrics/aggregations should have the same number of entries.\"}",
              HttpURLConnection.HTTP_BAD_REQUEST);
          return;
        }

        if (!validParams(exchange, metricList, dimList, aggList)) {
          return;
        }

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
        ConcurrentHashMap<String, String> nodeResponses = new ConcurrentHashMap<>();
        final List<ClusterDetailsEventProcessor.NodeDetails> allNodes = ClusterDetailsEventProcessor
            .getNodesDetails();
        String localNodeId = "local";
        if (allNodes.size() != 0) {
          localNodeId = allNodes.get(0).getId();
        }
        nodeResponses.put(localNodeId, localResponseWithTimestamp);
        String response = metricsRestUtil.nodeJsonBuilder(nodeResponses);

        if (nodes == null || !nodes.equals("all") || allNodes.size() <= 1) {
          sendResponse(exchange, response, HttpURLConnection.HTTP_OK);
        } else if (nodes.equals("all")) {
          CountDownLatch doneSignal = new CountDownLatch(allNodes.size() - 1);
          for (int i = 1; i < allNodes.size(); i++) {
            ClusterDetailsEventProcessor.NodeDetails node = allNodes.get(i);
            LOG.debug("Collecting remote stats");
            try {
              collectRemoteStats(node, metricList, aggList, dimList, nodeResponses, doneSignal);
            } catch (Exception e) {
              LOG.error(
                  "Unable to collect stats for node, addr:{}, exception: {} ExceptionCode: {}",
                  node.getHostAddress(),
                  e,
                  StatExceptionCode.REQUEST_REMOTE_ERROR.toString());
              StatsCollector.instance().logException(StatExceptionCode.REQUEST_REMOTE_ERROR);
            }
          }
          boolean completed = doneSignal.await(TIME_OUT_VALUE, TIME_OUT_UNIT);
          if (!completed) {
            LOG.debug("Timeout while collecting remote stats");
            StatsCollector.instance().logException(StatExceptionCode.REQUEST_REMOTE_ERROR);
          }
          sendResponseWhenRequestCompleted(nodeResponses, exchange);
        }
      } catch (InvalidParameterException e) {
        LOG.error("DB file path : {}", db.getDBFilePath());
        LOG.error(
            (Supplier<?>)
                () ->
                    new ParameterizedMessage(
                        "QueryException {} ExceptionCode: {}.",
                        e.toString(),
                        StatExceptionCode.REQUEST_ERROR.toString()),
            e);
        StatsCollector.instance().logException(StatExceptionCode.REQUEST_ERROR);
        String response = "{\"error\":\"" + e.getMessage() + "\"}";
        sendResponse(exchange, response, HttpURLConnection.HTTP_BAD_REQUEST);
      } catch (Exception e) {
        LOG.error("DB file path : {}", db.getDBFilePath());
        LOG.error(
            (Supplier<?>)
                () ->
                    new ParameterizedMessage(
                        "QueryException {} ExceptionCode: {}.",
                        e.toString(),
                        StatExceptionCode.REQUEST_ERROR.toString()),
            e);
        StatsCollector.instance().logException(StatExceptionCode.REQUEST_ERROR);
        String response = "{\"error\":\"" + e.toString() + "\"}";
        sendResponse(exchange, response, HttpURLConnection.HTTP_INTERNAL_ERROR);
      }
    } else {
      exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1);
      exchange.close();
    }
  }

  void collectRemoteStats(
      ClusterDetailsEventProcessor.NodeDetails node,
      List<String> metricList,
      List<String> aggList,
      List<String> dimList,
      final ConcurrentHashMap<String, String> nodeResponses,
      final CountDownLatch doneSignal)
      throws Exception {
    // create a request
    MetricsRequest request =
        MetricsRequest.newBuilder()
            .addAllMetricList(metricList)
            .addAllAggList(aggList)
            .addAllDimList(dimList)
            .build();
    ThreadSafeStreamObserver responseObserver =
        new ThreadSafeStreamObserver(node, nodeResponses, doneSignal);
    try {
      this.netClient.getMetrics(node.getHostAddress(), request, responseObserver);
    } catch (Exception e) {
      LOG.error("Metrics : Exception occurred while getting Metrics {}", e.getCause());
    }
  }

  private boolean isUnitLookUp(HttpExchange exchange) throws IOException {
    if (exchange.getRequestURI().toString().equals(Util.METRICS_QUERY_URL + "/units")) {
      return true;
    }
    return false;
  }

  private void getMetricUnits(HttpExchange exchange) throws IOException {
    Map<String, String> metricUnits = new HashMap<>();
    for (Map.Entry<String, MetricAttributes> entry : MetricsModel.ALL_METRICS.entrySet()) {
      String metric = entry.getKey();
      String unit = entry.getValue().unit;
      metricUnits.put(metric, unit);
    }
    sendResponse(
        exchange, JsonConverter.writeValueAsString(metricUnits), HttpURLConnection.HTTP_OK);
  }

  private boolean validParams(
      HttpExchange exchange, List<String> metricList, List<String> dimList, List<String> aggList)
      throws IOException {
    for (String metric : metricList) {
      if (MetricsModel.ALL_METRICS.get(metric) == null) {
        sendResponse(
            exchange,
            String.format("{\"error\":\"%s is an invalid metric.\"}", metric),
            HttpURLConnection.HTTP_BAD_REQUEST);
        return false;
      } else {
        for (String dim : dimList) {
          if (!MetricsModel.ALL_METRICS.get(metric).dimensionNames.contains(dim)) {
            sendResponse(
                exchange,
                String.format(
                    "{\"error\":\"%s is an invalid dimension for %s metric.\"}", dim, metric),
                HttpURLConnection.HTTP_BAD_REQUEST);
            return false;
          }
        }
      }
    }
    for (String agg : aggList) {
      if (!MetricsDB.AGG_VALUES.contains(agg)) {
        sendResponse(
            exchange,
            String.format("{\"error\":\"%s is an invalid aggregation type.\"}", agg),
            HttpURLConnection.HTTP_BAD_REQUEST);
        return false;
      }
    }

    return true;
  }

  private void sendResponseWhenRequestCompleted(
      ConcurrentHashMap<String, String> nodeResponses, HttpExchange exchange) {
    if (nodeResponses.size() == 0) {
      return;
    }
    String response = metricsRestUtil.nodeJsonBuilder(nodeResponses);
    try {
      sendResponse(exchange, response, HttpURLConnection.HTTP_OK);
    } catch (Exception e) {
      LOG.error("Exception occurred while sending response {}", e.getCause());
    }
  }

  private void sendResponse(HttpExchange exchange, String response, int status) throws IOException {
    try (OutputStream os = exchange.getResponseBody()) {
      exchange.sendResponseHeaders(status, response.length());
      os.write(response.getBytes());
    } catch (Exception e) {
      response = e.toString();
      exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, response.length());
    }
  }

  private static class ThreadSafeStreamObserver implements StreamObserver<MetricsResponse> {
    private final CountDownLatch doneSignal;
    private final ConcurrentHashMap<String, String> nodeResponses;
    private final ClusterDetailsEventProcessor.NodeDetails node;

    ThreadSafeStreamObserver(
        ClusterDetailsEventProcessor.NodeDetails node,
        ConcurrentHashMap<String, String> nodeResponses,
        CountDownLatch doneSignal) {
      this.node = node;
      this.doneSignal = doneSignal;
      this.nodeResponses = nodeResponses;
    }

    public void onNext(MetricsResponse value) {
      nodeResponses.putIfAbsent(node.getId(), value.getMetricsResult());
    }

    @Override
    public void onError(Throwable t) {
      LOG.info("Metrics : Error occurred while getting Metrics for " + node.getHostAddress());
      doneSignal.countDown();
    }

    @Override
    public void onCompleted() {
      doneSignal.countDown();
    }
  }
}
