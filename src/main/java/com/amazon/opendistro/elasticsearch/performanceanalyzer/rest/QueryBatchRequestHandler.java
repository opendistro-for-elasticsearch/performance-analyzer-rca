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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.jooq.Record;
import org.jooq.Result;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.security.InvalidParameterException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Request handler that supports querying MetricsDB on every EC2 instance. Example query –
 * "http://localhost:9600/_metricsdb?metrics=cpu,rss,memory%20agg=sum,avg,sum%20dims=index,operation,shard."
 * We can fetch multiple metrics using this interface and also specify the dimensions/aggregations
 * for fetching the metrics. We create a new metricsDB every 5 seconds and API only supports
 * querying the latest snapshot.
 */
public class QueryBatchRequestHandler extends MetricsHandler implements HttpHandler {

  private static final Logger LOG = LogManager.getLogger(QueryBatchRequestHandler.class);

  private static final int TIME_OUT_VALUE = 2;
  private static final TimeUnit TIME_OUT_UNIT = TimeUnit.SECONDS;
  private NetClient netClient;
  MetricsRestUtil metricsRestUtil;

  private static final int defaultMaxDatapoints = 100800;
  private static final long defaultSamplingPeriod = 5000;  // Must be a multiple of 5000

  public QueryBatchRequestHandler(NetClient netClient, MetricsRestUtil metricsRestUtil) {
    this.netClient = netClient;
    this.metricsRestUtil = metricsRestUtil;
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    String requestMethod = exchange.getRequestMethod();

    ReaderMetricsProcessor mp = ReaderMetricsProcessor.getInstance();
    if (mp == null) {
      sendResponse(
          exchange,
          "{\"error\":\"Metrics Processor is not initialized. The reader has run into an issue or has just started.\"}",
          HttpURLConnection.HTTP_UNAVAILABLE);
      LOG.warn("Metrics Processor is not initialized. The reader has run into an issue or has just started.");
      return;
    }

    NavigableSet<Long> batchMetrics = mp.getBatchMetrics();
    long currentTime = System.currentTimeMillis();
    if (batchMetrics == null) {
      sendResponse(
              exchange,
              "{\"error\":\"The batch metrics api has not been enabled for this node.\"}",
              HttpURLConnection.HTTP_UNAVAILABLE);
      LOG.warn("The batch metrics api has not been enabled for this node.");
      return;
    }
    if (batchMetrics.isEmpty()) {
      sendResponse(
              exchange,
              "{\"error\":\"There are no metrics databases. The reader has run into an issue or has just started.\"}",
              HttpURLConnection.HTTP_UNAVAILABLE);
      LOG.warn("There are no metrics databases. The reader has run into an issue or has just started.");
      return;
    }

    if (requestMethod.equalsIgnoreCase("GET")) {

      exchange.getResponseHeaders().set("Content-Type", "application/json");

      Map<String, String> params = getParamsMap(exchange.getRequestURI().getQuery());

      try {
        // Parse and validate parameters
        String[] validParamsTmp = {"metrics", "starttime", "endtime", "samplingperiod", "maxdatapoints"};
        Set<String> validParams = new HashSet<>(Arrays.asList(validParamsTmp));
        for (String param : params.keySet()) {
          if (!validParams.contains(param)) {
            throw new InvalidParameterException(String.format("%s is an invalid parameter", param));
          }
        }

        List<String> metricsList = metricsRestUtil.parseArrayParam(params, "metrics", false);
        String startTimeParam = params.get("starttime");
        String endTimeParam = params.get("endtime");
        String samplingPeriodParam = params.get("samplingperiod");
        String maxDatapointsParam = params.get("maxdatapoints");

        for (String metric : metricsList) {
          if (!MetricsModel.ALL_METRICS.containsKey(metric)) {
            throw new InvalidParameterException(String.format("%s is an invalid metric", metric));
          }
        }

        if (startTimeParam == null || startTimeParam.isEmpty()) {
          throw new InvalidParameterException("starttime parameter must be set");
        }
        long startTime;
        try {
          startTime = Long.parseUnsignedLong(startTimeParam);
        } catch (NumberFormatException e) {
          throw new InvalidParameterException(String.format("%s is an invalid starttime", startTimeParam));
        }

        if (endTimeParam == null || endTimeParam.isEmpty()) {
          throw new InvalidParameterException("endtime parameter must be set");
        }
        long endTime;
        try {
          endTime = Long.parseUnsignedLong(endTimeParam);
        } catch (NumberFormatException e) {
          throw new InvalidParameterException(String.format("%s is an invalid endtime", endTimeParam));
        }

        long samplingPeriod = defaultSamplingPeriod;
        if (samplingPeriodParam != null && !samplingPeriodParam.isEmpty()) {
          samplingPeriod = Long.parseLong(samplingPeriodParam);
          if (samplingPeriod < 5 || samplingPeriod % 5 != 0) {
            throw new InvalidParameterException(String.format("%s is an invalid sampling period", samplingPeriodParam));
          }
          if (samplingPeriod >= PluginSettings.instance().getBatchMetricsRetentionPeriod()*60) {
            throw new InvalidParameterException("sampling period must be less than the retention period");
          }
          samplingPeriod *= 1000;
        }

        if (startTime >= endTime) {
          throw new InvalidParameterException("starttime must be less than the endtime");
        }
        startTime -= startTime % samplingPeriod;
        endTime -= endTime % samplingPeriod;
        if (startTime == endTime) {
          throw new InvalidParameterException("starttime and endtime cannot be equal when rounded down to the nearest sampling period");
        }
        if (endTime > currentTime) {
          throw new InvalidParameterException("endtime can be no greater than the system time at the node");
        }
        if (startTime < currentTime - PluginSettings.instance().getBatchMetricsRetentionPeriod()*60*1000) {
          throw new InvalidParameterException("starttime must be within the retention period");
        }

        int maxDatapoints = defaultMaxDatapoints + 1;

        // Handle the query
        StringBuilder responseJson = new StringBuilder();
        responseJson.append("{");
        Long metricsTimestamp = batchMetrics.ceiling(startTime);
        int numMetrics = metricsList.size();
        MetricsDB metrics;
        Result<Record> results;
        if (metricsTimestamp != null && metricsTimestamp < endTime) {
          responseJson.append("\"");
          responseJson.append(metricsTimestamp);
          responseJson.append("\":{\"");
          responseJson.append(metricsList.get(0));
          responseJson.append("\":");
          metrics = MetricsDB.fetchExisting(metricsTimestamp);
          results = metrics.queryMetric(metricsList.get(0), maxDatapoints);
          maxDatapoints -= results.size();
          if (maxDatapoints == 0) {
            throw new InvalidParameterException("requested data exceeds the 100,800 datapoints limit");
          }
          responseJson.append(results.formatJSON());
          for (int i = 1; i < numMetrics; i++) {
            responseJson.append(",\"");
            responseJson.append(metricsList.get(i));
            responseJson.append("\":");
            results = metrics.queryMetric(metricsList.get(i), maxDatapoints);
            maxDatapoints -= results.size();
            if (maxDatapoints == 0) {
              throw new InvalidParameterException("requested data exceeds the 100,800 datapoints limit");
            }
            responseJson.append(results.formatJSON());
          }
          responseJson.append("}");
          metrics.close();
          metricsTimestamp = metricsTimestamp - metricsTimestamp % samplingPeriod + samplingPeriod;
          metricsTimestamp = batchMetrics.ceiling(metricsTimestamp);
          while (metricsTimestamp != null && metricsTimestamp < endTime) {
            responseJson.append(",\"");
            responseJson.append(metricsTimestamp);
            responseJson.append("\":{\"");
            responseJson.append(metricsList.get(0));
            responseJson.append("\":");
            metrics = MetricsDB.fetchExisting(metricsTimestamp);
            results = metrics.queryMetric(metricsList.get(0), maxDatapoints);
            maxDatapoints -= results.size();
            if (maxDatapoints == 0) {
              throw new InvalidParameterException("requested data exceeds the 100,800 datapoints limit");
            }
            responseJson.append(results.formatJSON());
            for (int i = 1; i < numMetrics; i++) {
              responseJson.append(",\"");
              responseJson.append(metricsList.get(i));
              responseJson.append("\":");
              results = metrics.queryMetric(metricsList.get(i), maxDatapoints);
              maxDatapoints -= results.size();
              if (maxDatapoints == 0) {
                throw new InvalidParameterException("requested data exceeds the 100,800 datapoints limit");
              }
              responseJson.append(results.formatJSON());
            }
            responseJson.append("}");
            metrics.close();
            metricsTimestamp = metricsTimestamp - metricsTimestamp % samplingPeriod + samplingPeriod;
            metricsTimestamp = batchMetrics.ceiling(metricsTimestamp);
          }
        }
        responseJson.append("}");
        String response = responseJson.toString();
        sendResponse(exchange, response, HttpURLConnection.HTTP_OK);
      } catch (InvalidParameterException e) {
        LOG.error(
                (Supplier<?>)
                        () ->
                                new ParameterizedMessage(
                                        "QueryException {} ExceptionCode: {}.",
                                        e.toString(),
                                        StatExceptionCode.REQUEST_ERROR.toString()),
                e);
        StatsCollector.instance().logException(StatExceptionCode.REQUEST_ERROR);
        String response = "{\"error\":\"" + e.getMessage() + ".\"}";
        sendResponse(exchange, response, HttpURLConnection.HTTP_BAD_REQUEST);
      } catch (Exception e) {
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

  private void sendResponse(HttpExchange exchange, String response, int status) throws IOException {
    try (OutputStream os = exchange.getResponseBody()) {
      exchange.sendResponseHeaders(status, response.length());
      os.write(response.getBytes());
    } catch (Exception e) {
      response = e.toString();
      exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, response.length());
    }
  }
}
