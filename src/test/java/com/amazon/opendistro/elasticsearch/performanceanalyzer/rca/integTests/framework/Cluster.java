/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerWebServer;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.ClusterType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.Consts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.HostTag;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.threads.ThreadProvider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.threads.exceptions.PAThreadException;
import com.google.gson.JsonElement;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.commons.io.FileUtils;
import org.jooq.Record;
import org.jooq.Result;

public class Cluster {
  // A cluster can have 0 (single node) to 5 (multi node with dedicated masters) hosts. The following three
  // maps specify what each host will be tagged as.
  private static final Map<Integer, HostTag> hostIdToHostTagMapForDedicatedMaster =
      new HashMap<Integer, HostTag>() {{
        put(0, HostTag.ELECTED_MASTER);
        put(1, HostTag.STANDBY_MASTER_0);
        put(2, HostTag.STANDBY_MASTER_1);
        put(3, HostTag.DATA_0);
        put(4, HostTag.DATA_1);
      }};

  private static final Map<Integer, HostTag> hostIdToHostTagMapCoLocatedMaster =
      new HashMap<Integer, HostTag>() {{
        put(0, HostTag.ELECTED_MASTER);
        put(1, HostTag.DATA_0);
      }};

  private static final HostTag hostIdToHostTagMapSingleNode = HostTag.DATA_0;

  // A queue where exceptions thrown by all the hosts will land.
  private final BlockingQueue<PAThreadException> exceptionQueue;
  private final boolean useHttps;
  private final ClusterType clusterType;

  // The list of all the hosts in the cluster.
  private final List<Host> hostList;

  // The top level directory that will be used by all the hosts for this iteration of the test.
  private final File clusterDir;

  // Same as the thread provide object in PerformanceAnalyzerApp.java
  private final ThreadProvider threadProvider;

  // If you want to get all the hosts that are assigned with role, say data_node, then this is the map
  // to query.
  private final Map<AllMetrics.NodeRole, List<Host>> roleToHostMap;

  private final boolean rcaEnabled;
  private Thread errorHandlingThread;

  // To get a host by a tag. Its the reverse mapping from what the top three maps contain.
  private final Map<HostTag, Host> tagToHostMapping;

  /**
   * @param type       The type of cluster - can be dedicated master, colocated master or single node.
   * @param clusterDir The directory that will be used by the cluster for files.
   * @param useHttps   Should the http and grpc connections use https.
   */
  public Cluster(final ClusterType type, final File clusterDir, final boolean useHttps) {
    this.clusterType = type;
    this.hostList = new ArrayList<>();
    this.roleToHostMap = new HashMap<>();
    this.clusterDir = clusterDir;
    // We start off with the RCA turned off and turn it on only right before we
    // invoke a test method.
    this.rcaEnabled = false;
    this.useHttps = useHttps;
    this.threadProvider = new ThreadProvider();
    this.exceptionQueue = new ArrayBlockingQueue<>(1);
    this.tagToHostMapping = new HashMap<>();

    switch (type) {
      case SINGLE_NODE:
        createSingleNodeCluster();
        break;
      case MULTI_NODE_CO_LOCATED_MASTER:
        createMultiNodeCoLocatedMaster();
        break;
      case MULTI_NODE_DEDICATED_MASTER:
        createMultiNodeDedicatedMaster();
        break;
    }

    for (Host host : hostList) {
      host.setClusterDetails(hostList);
    }
  }

  private void createMultiNodeDedicatedMaster() {
    int currWebServerPort = PluginSettings.WEBSERVICE_DEFAULT_PORT;
    int currGrpcServerPort = PluginSettings.RPC_DEFAULT_PORT;
    int hostIdx = 0;

    createHost(hostIdx, AllMetrics.NodeRole.ELECTED_MASTER, currWebServerPort, currGrpcServerPort);

    currWebServerPort += 1;
    currGrpcServerPort += 1;
    hostIdx += 1;

    for (int i = 0; i < Consts.numStandbyMasterNodes; i++) {
      createHost(hostIdx, AllMetrics.NodeRole.MASTER, currWebServerPort, currGrpcServerPort);

      currWebServerPort += 1;
      currGrpcServerPort += 1;
      hostIdx += 1;
    }

    for (int i = 0; i < Consts.numDataNodes; i++) {
      createHost(hostIdx, AllMetrics.NodeRole.DATA, currWebServerPort, currGrpcServerPort);

      currWebServerPort += 1;
      currGrpcServerPort += 1;
      hostIdx += 1;
    }
  }

  private Host createHost(int hostIdx,
                          AllMetrics.NodeRole role,
                          int webServerPort,
                          int grpcServerPort) {
    HostTag hostTag = getTagForHostIdForHostTagAssignment(hostIdx);
    Host host = new Host(hostIdx,
        useHttps,
        role,
        webServerPort,
        grpcServerPort,
        this.clusterDir,
        rcaEnabled,
        hostTag);
    tagToHostMapping.put(hostTag, host);
    hostList.add(host);

    List<Host> hostByRole = roleToHostMap.get(role);

    if (hostByRole == null) {
      hostByRole = new ArrayList<>();
      hostByRole.add(host);
      roleToHostMap.put(role, hostByRole);
    } else {
      hostByRole.add(host);
    }
    return host;
  }

  private HostTag getTagForHostIdForHostTagAssignment(int hostId) {
    switch (clusterType) {
      case MULTI_NODE_DEDICATED_MASTER:
        return hostIdToHostTagMapForDedicatedMaster.get(hostId);
      case MULTI_NODE_CO_LOCATED_MASTER:
        return hostIdToHostTagMapCoLocatedMaster.get(hostId);
      case SINGLE_NODE:
        return hostIdToHostTagMapSingleNode;
    }
    throw new IllegalStateException("No cluster type matches");
  }

  public void createServersAndThreads() {
    this.errorHandlingThread = PerformanceAnalyzerApp.startErrorHandlingThread(threadProvider, exceptionQueue);
    for (Host host : hostList) {
      host.createServersAndThreads(threadProvider);
    }
  }

  public void startRcaControllerThread() {
    for (Host host : hostList) {
      host.startRcaControllerThread();
    }
  }

  private void createSingleNodeCluster() {
    int currWebServerPort = PluginSettings.WEBSERVICE_DEFAULT_PORT;
    int currGrpcServerPort = PluginSettings.RPC_DEFAULT_PORT;
    int hostIdx = 0;

    createHost(hostIdx, AllMetrics.NodeRole.ELECTED_MASTER, currWebServerPort, currGrpcServerPort);
  }

  private void createMultiNodeCoLocatedMaster() {
    int currWebServerPort = PluginSettings.WEBSERVICE_DEFAULT_PORT;
    int currGrpcServerPort = PluginSettings.RPC_DEFAULT_PORT;
    int hostIdx = 0;

    createHost(hostIdx, AllMetrics.NodeRole.ELECTED_MASTER, currWebServerPort, currGrpcServerPort);

    currWebServerPort += 1;
    currGrpcServerPort += 1;
    hostIdx += 1;

    for (int i = 0; i < Consts.numDataNodes - 1; i++) {
      createHost(hostIdx, AllMetrics.NodeRole.DATA, currWebServerPort, currGrpcServerPort);

      currWebServerPort += 1;
      currGrpcServerPort += 1;
      hostIdx += 1;
    }
  }

  public void deleteCluster() throws IOException {
    for (List<Host> hosts : roleToHostMap.values()) {
      for (Host host : hosts) {
        host.deleteHost();
      }
    }
    errorHandlingThread.interrupt();
    deleteClusterDir();
  }

  public void deleteClusterDir() throws IOException {
    for (Host host : hostList) {
      host.deleteHostDir();
    }
    FileUtils.deleteDirectory(clusterDir);
  }

  public void stopRcaScheduler() throws Exception {
    for (Host host : hostList) {
      host.stopRcaScheduler();
    }
  }

  public void startRcaScheduler() throws Exception {
    for (Host host : hostList) {
      host.startRcaScheduler();
    }
  }

  public void updateGraph(final Class rcaGraphClass)
      throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
    for (Host host : hostList) {
      host.updateRcaGraph(rcaGraphClass);
    }
  }

  public void updateMetricsDB(AMetric[] metricAnnotations) throws Exception {
    for (Host host : hostList) {
      host.updateMetricsDB(metricAnnotations);
    }
  }

  public JsonElement getAllRcaDataOnHost(HostTag hostTag, String rcaName) {
    return tagToHostMapping.get(hostTag).getDataForRca(rcaName);
  }

  public <T> Object constructObjectFromDBOnHost(HostTag hostTag, Class<T> className) throws Exception {
    return tagToHostMapping.get(hostTag).constructObjectFromDB(className);
  }

  public String getRcaRestResponse(final String queryUrl, final Map<String, String> params, HostTag hostByTag) {
    return verifyTag(hostByTag).makeRestRequest(queryUrl, params);
  }

  public Map<String, Result<Record>> getRecordsForAllTablesOnHost(HostTag hostTag) {
    return verifyTag(hostTag).getRecordsForAllTables();
  }

  private Host verifyTag(HostTag hostTag) {
    Host host = tagToHostMapping.get(hostTag);
    if (host == null) {
      throw new IllegalArgumentException("No host with tag '" + hostTag + "' exists. "
          + "Available tags are: " + tagToHostMapping.keySet());
    }
    return host;
  }
}
