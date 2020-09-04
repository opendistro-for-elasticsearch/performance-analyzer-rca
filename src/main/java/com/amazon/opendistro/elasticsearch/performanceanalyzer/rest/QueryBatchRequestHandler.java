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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsRestUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.model.MetricsModel;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ReaderMetricsProcessor;
import com.google.common.annotations.VisibleForTesting;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.jooq.Record;
import org.jooq.Result;

/**
 * Request handler that supports querying batch metrics from an EC2 instance
 *
 * <p>Return 1 minute of CPU_Utilization metrics sampled at a 5s sampling period:
 * "http://localhost:9600/_opendistro/_performanceanalyzer/batch?metrics=CPU_Utilization&starttime=1566413975000&endtime=1566413980000"
 *
 * <p>Return 1 minute of CPU_Utilization and Latency metrics sampled at a 10s sampling period:
 * "http://localhost:9600/_opendistro/_performanceanalyzer/batch?metrics=CPU_Utilization,Latency&starttime=1566413975000&endtime=1566413980000&samplingperiod=10"
 *
 * <p>Return format:
 * {
 *   "1594412650000": {
 *     "CPU_Utilization": {
 *       "fields": [
 *         {
 *           "name: "IndexName",
 *           "type": "VARCHAR"
 *         },
 *         <...>
 *       ]
 *       "records": [
 *         [
 *           "pmc",
 *           <...>
 *         ],
 *         <...>
 *       ]
 *     }
 *   }
 * }
 */
public class QueryBatchRequestHandler extends MetricsHandler implements HttpHandler {

  private static final Logger LOG = LogManager.getLogger(QueryBatchRequestHandler.class);

  private static final int TIME_OUT_VALUE = 2;
  private static final TimeUnit TIME_OUT_UNIT = TimeUnit.SECONDS;
  private NetClient netClient;
  MetricsRestUtil metricsRestUtil;

  public static final int DEFAULT_MAX_DATAPOINTS = 100800;  // Must be non-negative
  public static final long DEFAULT_SAMPLING_PERIOD_MILLIS = 5000;  // Must be a multiple of 5000

  public QueryBatchRequestHandler(NetClient netClient, MetricsRestUtil metricsRestUtil) {
    this.netClient = netClient;
    this.metricsRestUtil = metricsRestUtil;
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    String requestMethod = exchange.getRequestMethod();
    if (!requestMethod.equalsIgnoreCase("GET")) {
      exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1);
      exchange.close();
      return;
    }

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

    exchange.getResponseHeaders().set("Content-Type", "application/json");

    Map<String, String> params = getParamsMap(exchange.getRequestURI().getQuery());

    try {
      // Parse and validate parameters
      String[] validParamsTmp = {"", "metrics", "starttime", "endtime", "samplingperiod"};
      Set<String> validParams = new HashSet<>(Arrays.asList(validParamsTmp));
      for (String param : params.keySet()) {
        if (!validParams.contains(param)) {
          throw new InvalidParameterException(String.format("%s is an invalid parameter", param));
        }
      }

      List<String> metrics = metricsRestUtil.parseArrayParam(params, "metrics", false);
      String startTimeParam = params.get("starttime");
      String endTimeParam = params.get("endtime");
      String samplingPeriodParam = params.get("samplingperiod");

      for (String metric : metrics) {
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

      long samplingPeriod = DEFAULT_SAMPLING_PERIOD_MILLIS;
      if (samplingPeriodParam != null && !samplingPeriodParam.isEmpty()) {
        samplingPeriod = Long.parseLong(samplingPeriodParam);
        if (samplingPeriod < 5 || samplingPeriod % 5 != 0) {
          throw new InvalidParameterException(String.format("%s is an invalid sampling period", samplingPeriodParam));
        }
        if (samplingPeriod >= PluginSettings.instance().getBatchMetricsRetentionPeriodMinutes() * 60) {
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
      if (startTime < currentTime - PluginSettings.instance().getBatchMetricsRetentionPeriodMinutes() * 60 * 1000) {
        throw new InvalidParameterException("starttime must be within the retention period");
      }

      String queryResponse = queryFromBatchMetrics(batchMetrics, metrics, startTime, endTime, samplingPeriod,
              DEFAULT_MAX_DATAPOINTS);
      sendResponse(exchange, queryResponse, HttpURLConnection.HTTP_OK);
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
  }

  @VisibleForTesting
  public int appendMetrics(Long timestamp, List<String> metrics, StringBuilder builder, int maxDatapoints) throws Exception {
    maxDatapoints += 1;
    builder.append("\"");
    builder.append(timestamp);
    builder.append("\":{");
    MetricsDB db = MetricsDB.fetchExisting(timestamp);
    for (int metricIndex = 0, numMetrics = metrics.size(); metricIndex < numMetrics; metricIndex++) {
      String metric = metrics.get(metricIndex);
      Result<Record> results = db.queryMetric(metric, MetricsModel.ALL_METRICS.get(metric).dimensionNames, maxDatapoints);
      if (results != null) {
        maxDatapoints -= results.size();
        if (maxDatapoints == 0) {
          throw new InvalidParameterException(String.format("requested data exceeds the %d datapoints limit", DEFAULT_MAX_DATAPOINTS));
        }
        builder.append("\"");
        builder.append(metric);
        builder.append("\":");
        builder.append(results.formatJSON());
        for (metricIndex += 1; metricIndex < numMetrics; metricIndex++) {
          metric = metrics.get(metricIndex);
          results = db.queryMetric(metric, MetricsModel.ALL_METRICS.get(metric).dimensionNames, maxDatapoints);
          if (results != null) {
            maxDatapoints -= results.size();
            if (maxDatapoints == 0) {
              throw new InvalidParameterException(String.format("requested data exceeds the %d datapoints limit", DEFAULT_MAX_DATAPOINTS));
            }
            builder.append(",\"");
            builder.append(metric);
            builder.append("\":");
            builder.append(results.formatJSON());
          }
        }
      }
    }
    builder.append("}");
    db.remove();
    return maxDatapoints - 1;
  }

  /**
   * Requires non-empty batchMetrics, valid non-empty metrics, valid startTime, valid endTime,
   * valid samplingPeriod (in milliseconds), and non-negative maxDatapoints.
   */
  @VisibleForTesting
  public String queryFromBatchMetrics(NavigableSet<Long> batchMetrics, List<String> metrics, long startTime,
                                      long endTime, long samplingPeriod, int maxDatapoints) throws Exception {
    StringBuilder responseJson = new StringBuilder();
    responseJson.append("{");
    Long metricsTimestamp = batchMetrics.ceiling(startTime);
    if (metricsTimestamp != null && metricsTimestamp < endTime) {
      maxDatapoints = appendMetrics(metricsTimestamp, metrics, responseJson, maxDatapoints);
      metricsTimestamp = metricsTimestamp - metricsTimestamp % samplingPeriod + samplingPeriod;
      metricsTimestamp = batchMetrics.ceiling(metricsTimestamp);
      while (metricsTimestamp != null && metricsTimestamp < endTime) {
        responseJson.append(",");
        maxDatapoints = appendMetrics(metricsTimestamp, metrics, responseJson, maxDatapoints);
        metricsTimestamp = metricsTimestamp - metricsTimestamp % samplingPeriod + samplingPeriod;
        metricsTimestamp = batchMetrics.ceiling(metricsTimestamp);
      }
    }
    responseJson.append("}");
    return responseJson.toString();
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
