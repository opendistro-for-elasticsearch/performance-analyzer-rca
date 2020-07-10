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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.TestClusterType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.threads.ThreadProvider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.threads.exceptions.PAThreadException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.commons.io.FileUtils;

public class Cluster {
  private final boolean useHttps;
  private TestClusterType clusterType;
  private List<Host> hostList;

  File clusterDir;

  private static final int numberOfCoLocatedMasterHosts = 3;

  private static final int numberOfDataNodesInDedicatedMasterCluster = 3;
  private static final int numberOfStandbyMasterNodesInDedicatedMasterCluster = 2;

  private static ThreadProvider threadProvider;

  private Map<AllMetrics.NodeRole, List<Host>> roleToHostMap;
  private boolean rcaEnabled;

  public final BlockingQueue<PAThreadException> exceptionQueue;
  private final Thread errorHandlingThread;

  public Cluster(TestClusterType type, boolean useHttps, boolean rcaEnabled, File clusterDir) {
    this.clusterType = type;
    this.hostList = new ArrayList<>();
    this.roleToHostMap = new HashMap<>();
    this.clusterDir = clusterDir;
    this.rcaEnabled = rcaEnabled;
    this.useHttps = useHttps;
    this.threadProvider = new ThreadProvider();
    this.exceptionQueue = new ArrayBlockingQueue<>(1);

    switch (type) {
      case SINGLE_NODE:
        // createSingleNodeCluster(useHttps, rcaEnabled);
        break;
      case MULTI_NODE_CO_LOCATED_MASTER:
        // createMultiNodeCoLocatedMaster(useHttps, rcaEnabled);
        break;
      case MULTI_NODE_DEDICATED_MASTER:
        createMultiNodeDedicatedMaster();
        break;
    }

    for (Host host : hostList) {
      host.setClusterDetails(hostList);
    }
    this.errorHandlingThread = PerformanceAnalyzerApp.startErrorHandlingThread(threadProvider, exceptionQueue);
  }

  public void setUpCluster() throws Exception {
    for (Host host : hostList) {
      host.createServersAndThreads(threadProvider);
    }
  }

  public void startClusterThreads() {
    for (Host host : hostList) {
      host.startRcaController();
    }
  }

  private void createSingleNodeCluster(boolean useHttps, boolean rcaEnabled) {
    // Host host = new Host(
    //     0,
    //     useHttps,
    //     AllMetrics.NodeRole.ELECTED_MASTER,
    //     PerformanceAnalyzerWebServer.WEBSERVICE_DEFAULT_PORT,
    //     Util.RPC_PORT, clusterDir, rcaEnabled);
    // hostList.add(host);
  }

  private void createMultiNodeCoLocatedMaster(boolean useHttps, boolean rcaEnabled) {
    // int currWebServerPort = PerformanceAnalyzerWebServer.WEBSERVICE_DEFAULT_PORT;
    // int currGrpcServerPort = Util.RPC_PORT;
    // int hostIdx = 0;

    // Host host =
    //     new Host(0, useHttps, AllMetrics.NodeRole.ELECTED_MASTER, currWebServerPort, currGrpcServerPort, clusterDir, rcaEnabled);
    // roleToHostMap.put(AllMetrics.NodeRole.ELECTED_MASTER, Collections.singletonList(host));

    // currWebServerPort += 1;
    // currGrpcServerPort += 1;
    // hostIdx += 1;

    // List<Host> otherNodes = new ArrayList<>();
    // for (int i = 0; i < numberOfCoLocatedMasterHosts - 1; i++) {
    //   otherNodes.add(
    //       new Host(hostIdx,
    //           useHttps,
    //           AllMetrics.NodeRole.DATA,
    //           currWebServerPort,
    //           currGrpcServerPort,
    //           clusterDir,
    //           rcaEnabled));

    //   currWebServerPort += 1;
    //   currGrpcServerPort += 1;
    //   hostIdx += 1;
    // }
    // roleToHostMap.put(AllMetrics.NodeRole.DATA, otherNodes);
  }

  private void createMultiNodeDedicatedMaster() {
    int currWebServerPort = PerformanceAnalyzerWebServer.WEBSERVICE_DEFAULT_PORT;
    int currGrpcServerPort = Util.RPC_PORT;
    int hostIdx = 0;

    createHost(hostIdx, AllMetrics.NodeRole.ELECTED_MASTER, currWebServerPort, currGrpcServerPort);

    currWebServerPort += 1;
    currGrpcServerPort += 1;
    hostIdx += 1;

    for (int i = 0; i < numberOfStandbyMasterNodesInDedicatedMasterCluster; i++) {
      createHost(hostIdx, AllMetrics.NodeRole.MASTER, currWebServerPort, currGrpcServerPort);

      currWebServerPort += 1;
      currGrpcServerPort += 1;
      hostIdx += 1;
    }

    for (int i = 0; i < numberOfDataNodesInDedicatedMasterCluster; i++) {
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
    FileUtils.deleteDirectory(clusterDir);
  }

  private void createHost(int hostIdx,
                          AllMetrics.NodeRole role,
                          int webServerPort,
                          int grpcServerPort) {
    Host host = new Host(hostIdx,
        useHttps,
        role,
        webServerPort,
        grpcServerPort,
        this.clusterDir,
        rcaEnabled);
    hostList.add(host);

    List<Host> hostByRole = roleToHostMap.get(role);

    if (hostByRole == null) {
      hostByRole = new ArrayList<>();
      hostByRole.add(host);
      roleToHostMap.put(role, hostByRole);
    } else {
      hostByRole.add(host);
    }
  }

  public void pauseCluster() throws Exception {
    for (Host host : hostList) {
      host.pause();
    }
  }

  public void unPauseCluster() throws Exception {
    for (Host host : hostList) {
      host.unPause();
    }
  }

  public void updateGraph(final List<ConnectedComponent> connectedComponents) {
    for (Host host : hostList) {
      host.updateRcaGraph(connectedComponents);
    }
  }

  public void updateDbProvider(final Queryable dbProvider) {
    for (Host host: hostList) {
      host.setDbProvider(dbProvider);
    }
  }
}
