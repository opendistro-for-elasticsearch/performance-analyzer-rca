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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.RemoteNodeRcaRequest;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.RemoteNodeRcaResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsRestUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.Version;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Stats;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.HeatZoneAssigner;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.temperature.TemperatureDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.SQLiteQueryUtils;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.Persistable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
  public static final String PARAM_VALUE_DELIMITER = ",";
  public static final String DIMENSION_PARAM = "dim";
  public static final String ZONE_PARAM = "zone";
  public static final String TOP_K_PARAM = "topk";

  private static final int TIME_OUT_VALUE = 2;
  private static final TimeUnit TIME_OUT_UNIT = TimeUnit.SECONDS;

  public static final TemperatureDimension DEFAULT_DIMENSION = TemperatureDimension.CPU_Utilization;
  public static final HeatZoneAssigner.Zone DEFAULT_ZONE = HeatZoneAssigner.Zone.HOT;
  public static final int DEFAULT_TOP_K = 3;

  private Persistable persistable;
  private MetricsRestUtil metricsRestUtil;
  private NetClient netClient;

  public QueryRcaRequestHandler(NetClient netClient) {
    this.netClient = netClient;
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
    //check if we are querying from elected master
    if (!validNodeRole()) {
      JsonObject errResponse = new JsonObject();
      errResponse.addProperty("error", "Node being queried is not elected master.");
      sendResponse(exchange, errResponse.toString(),
          HttpURLConnection.HTTP_BAD_REQUEST);
      return;
    }

    // Temperature RCAs does not mix well with other RCAs. Therefore, all of them have to be temperature RCAs.
    if (SQLiteQueryUtils.getMasterAccessibleNodeTemperatureRCASet().containsAll(getTemperatureRcasFromParam(params))) {
      // When you ask for the temperature details of nodes, we want to make sure they are data nodes.
      String[] nodes = params.get(QueryMetricsRequestHandler.NODES_PARAM).split(",");
      if (allDataNodeIps(nodes)) {
        try {
          List<String> rcas = getTemperatureRcasFromParam(params);
          List<String> dimensions = getDimensionFromParams(params);
          List<String> zones = getZoneFromParams(params);
          int topK = getTopKFromParams(params);

          String response = gatherTemperatureFromDataNodes(nodes, rcas, dimensions, zones, topK);
          sendResponse(exchange, response, HttpURLConnection.HTTP_OK);
          return;
        } catch (IllegalArgumentException ex) {
          JsonObject errResponse = new JsonObject();
          errResponse.addProperty("error", ex.getMessage());
          sendResponse(exchange, errResponse.toString(), HttpURLConnection.HTTP_BAD_REQUEST);
          return;

        }
      } else {
        JsonObject errResponse = new JsonObject();

        StringBuilder errMsgBuilder =
            new StringBuilder("One or more IPs provided that don't belong to data nodes. DataNode Ips: [");

        errMsgBuilder
                .append(getDataNodeIPSet())
                .append("]. Provided list: [")
                .append(String.join(", ", nodes)).append("]");
        errResponse.addProperty("error", errMsgBuilder.toString());
        sendResponse(exchange, errResponse.toString(), HttpURLConnection.HTTP_BAD_REQUEST);
        return;
      }
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
    String response = getRcaData(persistable, rcaList).toString();
    sendResponse(exchange, response, HttpURLConnection.HTTP_OK);
  }

  private List<String> getTemperatureRcasFromParam(Map<String, String> params) {
    // NAME_PARAM is a mandatory parameter and the method "parseArrayParam" checks for it.
    // So this is guaranteed to be not null.
    return Arrays.stream(
        params
            .get(NAME_PARAM)
            .split(PARAM_VALUE_DELIMITER))
        .map(a -> a.trim())
        .collect(Collectors.toList());
  }

  private List<String> getDimensionFromParams(Map<String, String> params) {
    String dimensionStr = params.getOrDefault(DIMENSION_PARAM, DEFAULT_DIMENSION.NAME);
    return Arrays.stream(dimensionStr.split(PARAM_VALUE_DELIMITER)).map(a -> {
      String dim = a.trim();
      boolean isValidDim = false;
      List<String> validDim = new ArrayList<>();

      for (TemperatureDimension dimension: TemperatureDimension.values()) {
        validDim.add(dimension.NAME);
        if (dimension.NAME.equals(dim)) {
          isValidDim = true;
          break;
        }
      }

      if (!isValidDim) {
        StringBuilder err =  new StringBuilder();
        err.append("Unknown dimension '")
        .append(dim)
        .append("'. Valid dimensions are: ")
        .append(validDim);

        throw new IllegalArgumentException(err.toString());
      }
      return dim;
    }).collect(Collectors.toList());
  }

  private List<String> getZoneFromParams(Map<String, String> params) {
    String zoneStr = params.getOrDefault(ZONE_PARAM, DEFAULT_ZONE.name());
    return Arrays.stream(zoneStr.split(PARAM_VALUE_DELIMITER)).map(a -> {
      String zoneName = a.trim();

      boolean isValid = false;
      List<String> zones = new ArrayList<>();

      for (HeatZoneAssigner.Zone zone: HeatZoneAssigner.Zone.values()) {
        zones.add(zone.name());

        if (zone.name().equals(zoneName)) {
          isValid = true;
          break;
        }
      }
      if (!isValid) {
        StringBuilder err =  new StringBuilder();
        err.append("Unknown zone '")
            .append(zoneName)
            .append("'. Valid zones are: ")
            .append(zones);

        throw new IllegalArgumentException(err.toString());
      }

      return zoneName;
    }).collect(Collectors.toList());
  }

  private int getTopKFromParams(Map<String, String> params) {
    String topKStr = params.getOrDefault(TOP_K_PARAM, String.valueOf(DEFAULT_TOP_K));
    try {
      int topK = Integer.parseInt(topKStr);
      if (topK < 1) {
        throw new IllegalArgumentException(TOP_K_PARAM + " cannot be less than 1. Provided: " + topKStr);
      }
      return topK;
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException("Invalid number type found for " + TOP_K_PARAM + ". Provide: " + topKStr);
    }
  }

  private Map<String, ClusterDetailsEventProcessor.NodeDetails> getIpDataNodeDetailsMap() {
    Map<String, ClusterDetailsEventProcessor.NodeDetails> map = new HashMap<>();
    for (ClusterDetailsEventProcessor.NodeDetails nodeDetails: ClusterDetailsEventProcessor.getDataNodesDetails()) {
      map.put(nodeDetails.getHostAddress(), nodeDetails);
    }
    return map;
  }

  private String gatherTemperatureFromDataNodes(String[] nodes,
                                                List<String> rcas,
                                                List<String> dimensions,
                                                List<String> zones,
                                                int topK) {
    CountDownLatch doneSignal = new CountDownLatch(nodes.length - 1);
    ConcurrentHashMap<String, String> nodeResponses = new ConcurrentHashMap<>();

    Map<String, ClusterDetailsEventProcessor.NodeDetails> nodeDetailsMap = getIpDataNodeDetailsMap();
    for (String nodeIp: nodes) {
      ClusterDetailsEventProcessor.NodeDetails node = nodeDetailsMap.get(nodeIp);
      try {
        collectRemoteRcas(node, topK, rcas, dimensions, zones, nodeResponses, doneSignal);
      } catch (Exception e) {
        LOG.error(
            "Unable to collect Rcas from node, addr:{}, exception: {} ExceptionCode: {}",
            nodeIp,
            e,
            StatExceptionCode.REQUEST_REMOTE_ERROR.toString());
        StatsCollector.instance().logException(StatExceptionCode.REQUEST_REMOTE_ERROR);
      }
    }
    boolean completed = false;
    try {
      completed = doneSignal.await(TIME_OUT_VALUE, TIME_OUT_UNIT);
    } catch (InterruptedException e) {
      LOG.error(
          "Timeout before all nodes could respond. exception: {} ExceptionCode: {}",
          e,
          StatExceptionCode.REQUEST_REMOTE_ERROR.toString());
      StatsCollector.instance().logException(StatExceptionCode.REQUEST_REMOTE_ERROR);
    }
    if (!completed) {
      LOG.debug("Timeout while collecting remote stats");
      StatsCollector.instance().logException(StatExceptionCode.REQUEST_REMOTE_ERROR);
    }

    return nodeResponses.toString();
  }


  void collectRemoteRcas(
      ClusterDetailsEventProcessor.NodeDetails node,
      int topK,
      List<String> rcaNames,
      List<String> dimensionNames,
      List<String> zones,
      final ConcurrentHashMap<String, String> nodeResponses,
      final CountDownLatch doneSignal) {
    RemoteNodeRcaRequest request = RemoteNodeRcaRequest
        .newBuilder()
        .addAllRcaName(rcaNames)
        .addAllDimensionName(dimensionNames)
        .addAllZone(zones)
        .setTopK(topK)
        .build();

    ThreadSafeStreamObserver responseObserver = new ThreadSafeStreamObserver(node, nodeResponses, doneSignal);
    try {
      this.netClient.getRemoteRca(node.getHostAddress(), request, responseObserver);
    } catch (Exception e) {
      LOG.error("Metrics : Exception occurred while getting Metrics {}", e.getCause());
    }
  }

  private Set getDataNodeIPSet() {
    return ClusterDetailsEventProcessor
        .getDataNodesDetails()
        .stream()
        .map(nodeDetail -> nodeDetail.getHostAddress())
        .collect(Collectors.toSet());
  }

  /**
   * A helper method to check if all the ips are those of data nodes.
   * If there is even one IP that is a non-data node IP, the method returns false.
   * @param ips The list of IPs provided as the request.
   * @return true if all IPs provided belong to the data nodes.
   */
  private boolean allDataNodeIps(String[] ips) {
    Set dataNodeIps = getDataNodeIPSet();

    for (String ip: ips) {
      if (!dataNodeIps.contains(ip.trim())) {
        return false;
      }
    }
    return true;
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
    ClusterDetailsEventProcessor.NodeDetails currentNode = ClusterDetailsEventProcessor
        .getCurrentNodeDetails();
    return currentNode.getIsMasterNode();
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

  public static JsonElement getTemperatureProfileRca(final Persistable persistable, String rca) {
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

  static class ThreadSafeStreamObserver implements StreamObserver<RemoteNodeRcaResponse> {
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

    public void onNext(RemoteNodeRcaResponse value) {
      nodeResponses.putIfAbsent(node.getId(), value.getTemperatureJson());
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
