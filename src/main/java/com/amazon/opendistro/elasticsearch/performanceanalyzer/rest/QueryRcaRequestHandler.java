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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsRestUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.Version;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Stats;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.SQLiteQueryUtils;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.Persistable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.Map;
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
 *
 *  <p>For temperature profiles, one can get the local node temperature using a request url as:
 *  curl "localhost:9600/_opendistro/_performanceanalyzer/rca?name=NodeTemperatureRca&local=true"
 *
 * <p>The cluster level RCA can only be queried from the elected master using this rest API:
 * curl "localhost:9600/_opendistro/_performanceanalyzer/rca?name=ClusterTemperatureRca"
 *
 *
 * <p>curl "localhost:9600/_opendistro/_performanceanalyzer/rca?name=NodeTemperatureRca&local=true"|jq
 * {
 *   "NodeTemperatureRca": [
 *     {
 *       "rca_name": "NodeTemperatureRca",
 *       "timestamp": 1589592178829,
 *       "state": "unknown",
 *       "CompactNodeSummary": [
 *         {
 *           "node_id": "v9_TNhEeSP2Q3DJO8fd6BA",
 *           "host_address": "172.17.0.2",
 *           "CPU_Utilization_mean": 0,
 *           "CPU_Utilization_total": 0.0310837676896351,
 *           "CPU_Utilization_num_shards": 2,
 *           "Heap_AllocRate_mean": 0,
 *           "Heap_AllocRate_total": 4021355.87442904,
 *           "Heap_AllocRate_num_shards": 2,
 *           "IO_READ_SYSCALL_RATE_mean": 0,
 *           "IO_READ_SYSCALL_RATE_total": 0,
 *           "IO_READ_SYSCALL_RATE_num_shards": 0,
 *           "IO_WriteSyscallRate_mean": 0,
 *           "IO_WriteSyscallRate_total": 0,
 *           "IO_WriteSyscallRate_num_shards": 0
 *         }
 *       ]
 *     }
 *   ]
 * }
 */
public class QueryRcaRequestHandler extends MetricsHandler implements HttpHandler {

  private static final Logger LOG = LogManager.getLogger(QueryRcaRequestHandler.class);
  private static final String DUMP_ALL = "all";
  private static final String VERSION_PARAM = "v";
  private static final String LOCAL_PARAM = "local";
  private static final String VERSION_RESPONSE_PROPERTY = "version";
  public static final String NAME_PARAM = "name";
  private Persistable persistable;
  private MetricsRestUtil metricsRestUtil;
  private AppContext appContext;

  public QueryRcaRequestHandler(final AppContext appContext) {
    this.appContext = appContext;
    metricsRestUtil = new MetricsRestUtil();
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
          if (query != null && query.equalsIgnoreCase(VERSION_PARAM)) {
            sendResponse(exchange, getVersion(), HttpURLConnection.HTTP_OK);
            return;
          }
          //first check if we want to dump all SQL tables for debugging purpose
          if (query != null && query.equals(DUMP_ALL)) {
            sendResponse(exchange, dumpAllRcaTables(), HttpURLConnection.HTTP_OK);
          }
          else {
            Map<String, String> params = getParamsMap(query);
            if (isLocalTemperatureProfileRequest(params)) {
              handleLocalRcaRequest(params, exchange);
            } else {
              handleClusterRcaRequest(params, exchange);
            }
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

  private void handleClusterRcaRequest(Map<String, String> params, HttpExchange exchange)
      throws IOException {
    List<String> rcaList = metricsRestUtil.parseArrayParam(params, NAME_PARAM, true);
    // query all cluster level RCAs if no RCA is specified in name.
    if (rcaList.isEmpty()) {
      rcaList = SQLiteQueryUtils.getClusterLevelRca();
    }
    //check if RCA is valid
    if (!validParams(rcaList)) {
      JsonObject errResponse = new JsonObject();
      JsonArray errReason = new JsonArray();
      SQLiteQueryUtils.getClusterLevelRca().forEach(errReason::add);
      errResponse.addProperty("error", "Invalid RCA.");
      errResponse.add("valid_cluster_rca", errReason);
      sendResponse(exchange, errResponse.toString(),
          HttpURLConnection.HTTP_BAD_REQUEST);
      return;
    }
    //check if we are querying from elected master
    if (!validNodeRole()) {
      JsonObject errResponse = new JsonObject();
      errResponse.addProperty("error", "Node being queried is not elected master.");
      sendResponse(exchange, errResponse.toString(),
          HttpURLConnection.HTTP_BAD_REQUEST);
      return;
    }
    String response = getRcaData(persistable, rcaList).toString();
    sendResponse(exchange, response, HttpURLConnection.HTTP_OK);
  }

  private boolean isLocalTemperatureProfileRequest(final Map<String, String> params) {
    final List<String> temperatureProfileRcas = SQLiteQueryUtils.getTemperatureProfileRcas();
    if (params.containsKey(LOCAL_PARAM)) {
      if (!Boolean.parseBoolean(params.get(LOCAL_PARAM))) {
        return false;
      }

      String requestedRca = params.get(NAME_PARAM);
      return temperatureProfileRcas.contains(requestedRca);
    }

    return false;
  }

  private void handleLocalRcaRequest(final Map<String, String> params,
      final HttpExchange exchange) throws IOException {
    String rcaRequested = params.get(NAME_PARAM);
    if (rcaRequested == null || rcaRequested.isEmpty()) {
      JsonObject errorResponse = new JsonObject();
      errorResponse.addProperty("error", "name parameter is empty or null");
      sendResponse(exchange, errorResponse.toString(), HttpURLConnection.HTTP_BAD_REQUEST);
      return;
    }

    try {
      if (Stats.getInstance().getMutedGraphNodes().contains(rcaRequested)) {
        JsonObject errorResponse = new JsonObject();
        StringBuilder builder = new StringBuilder();
        builder.append("The Rca '").append(rcaRequested).append("' is muted. Consider removing it "
                + "from the rca.conf's 'muted-rcas' list");
        errorResponse.addProperty("error", builder.toString());
        sendResponse(exchange, errorResponse.toString(), HttpURLConnection.HTTP_BAD_REQUEST);
      } else {
        String response = getTemperatureProfileRca(persistable, rcaRequested).toString();
        sendResponse(exchange, response, HttpURLConnection.HTTP_OK);
      }
    } catch (Exception ex) {
      JsonObject errorResponse = new JsonObject();
      errorResponse.addProperty("error", ex.getMessage());
      sendResponse(exchange, errorResponse.toString(), HttpURLConnection.HTTP_BAD_REQUEST);
    }
  }

  // check whether RCAs are cluster level RCAs
  private boolean validParams(List<String> rcaList) {
    return rcaList.stream()
                  .allMatch(SQLiteQueryUtils::isClusterLevelRca);
  }

  // check if we are querying from elected master
  private boolean validNodeRole() {
    return appContext.getMyInstanceDetails().getIsMaster();
  }

  private JsonElement getRcaData(Persistable persistable, List<String> rcaList) {
    LOG.debug("RCA: in getRcaData");
    JsonObject jsonObject = new JsonObject();
    if (persistable != null) {
      rcaList.forEach(rca ->
          jsonObject.add(rca, persistable.read(rca))
      );
    }
    return jsonObject;
  }

  private JsonElement getTemperatureProfileRca(final Persistable persistable, String rca) {
    JsonObject responseJson = new JsonObject();
    if (persistable != null) {
      responseJson.add(rca, persistable.read(rca));
    }
    return responseJson;
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

  /**
   * Gets the current RCA framework version.
   * @return The version number as a json string.
   */
  public String getVersion() {
    final JsonObject versionObject = new JsonObject();
    versionObject.addProperty(VERSION_RESPONSE_PROPERTY, Version.getRcaVersion());

    return versionObject.toString();
  }
}
