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

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca.HIGH_HEAP_USAGE_CLUSTER_RCA_TABLE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsRestUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.Persistable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.response.RcaResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.GsonBuilder;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;

/**
 *  Request handler that supports querying RCAs
 *
 *  <p>To dump all RCA related tables from SQL :
 *  curl --url "localhost:9650/_opendistro/_performanceanalyzer/rca?all" -XGET
 *
 *  <p>To get response for all the available RCA, use:
 *  curl --url "localhost:9650/_opendistro/_performanceanalyzer/rca" -XGET
 *
 *  <p>To get response for a specific RCA, use:
 *  curl --url "localhost:9650/_opendistro/_performanceanalyzer/rca?name=HighHeapUsageClusterRca" -XGET
 */
public class QueryRcaRequestHandler extends MetricsHandler implements HttpHandler {

  private static final Logger LOG = LogManager.getLogger(QueryRcaRequestHandler.class);
  private static final int HTTP_CLIENT_CONNECTION_TIMEOUT = 200;
  private static final String DUMP_ALL = "all";
  private Persistable persistable;
  private MetricsRestUtil metricsRestUtil;
  private Set<String> validRCA;

  public QueryRcaRequestHandler() {
    metricsRestUtil = new MetricsRestUtil();
    validRCA = ImmutableSet.of(HIGH_HEAP_USAGE_CLUSTER_RCA_TABLE);
  }

  @Override
  public void handle(HttpExchange exchange) throws IOException {
    String requestMethod = exchange.getRequestMethod();

    if (requestMethod.equalsIgnoreCase("GET")) {
      LOG.debug("RCA Query handler called.");
      exchange.getResponseHeaders().set("Content-Type", "application/json");

      try {
        synchronized (this) {
          String query = exchange.getRequestURI().getQuery();
          //first check if we want to dump all SQL tables for debugging purpose
          if (query.equals(DUMP_ALL)) {
            sendResponse(exchange, dumpAllRcaTables(), HttpURLConnection.HTTP_OK);
          }
          else {
            Map<String, String> params = getParamsMap(query);
            List<String> rcaList = metricsRestUtil.parseArrayParam(params, "name", true);

            if (!validParams(rcaList)) {
              sendResponse(exchange, "{\"error\":\"Invalid RCA.\"}",
                  HttpURLConnection.HTTP_BAD_REQUEST);
              return;
            }
            if (rcaList.isEmpty()) {
              rcaList = ImmutableList.copyOf(validRCA);
            }

            String response = getRcaData(persistable, rcaList);
            sendResponse(exchange, response, HttpURLConnection.HTTP_OK);
          }
        }
      } catch (InvalidParameterException e) {
        LOG.error(
                (Supplier<?>)
                        () ->
                                new ParameterizedMessage(
                                        "QueryException {} ExceptionCode: {}.",
                                        e.toString(),
                                        StatExceptionCode.REQUEST_ERROR.toString()),
                e);
        String response = "{\"error\":\"" + e.getMessage() + "\"}";
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
        String response = "{\"error\":\"" + e.toString() + "\"}";
        sendResponse(exchange, response, HttpURLConnection.HTTP_INTERNAL_ERROR);
      }
    } else {
      exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1);
      exchange.close();
    }
  }

  private boolean validParams(List<String> rcaList) {
    return rcaList.stream()
            .allMatch(e -> validRCA.contains(e));
  }

  private String getRcaData(Persistable persistable, List<String> rcaList) {
    LOG.debug("RCA: in getRcaData");
    List<RcaResponse> rcaResponseList = null;
    if (persistable != null) {
      rcaResponseList =  rcaList.stream().map(rca -> persistable.readRca(rca))
              .filter(r -> r != null)
              .collect(Collectors.toList());
    }
    return new GsonBuilder()
            .setFieldNamingStrategy(FieldNamingPolicy.UPPER_CAMEL_CASE)
            .create()
            .toJson(rcaResponseList);
  }

  private String dumpAllRcaTables() {
    String jsonResponse = "";
    if (persistable != null) {
      jsonResponse = persistable.read();
    }
    return jsonResponse;
  }

  public void sendResponse(HttpExchange exchange, String response, int status) throws IOException {
    try (OutputStream os = exchange.getResponseBody()) {
      exchange.sendResponseHeaders(status, response.length());
      os.write(response.getBytes());
    } catch (Exception e) {
      response = e.toString();
      exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, response.length());
    }
  }

  public synchronized void setPersistable(Persistable persistable) {
    this.persistable = persistable;
  }
}
