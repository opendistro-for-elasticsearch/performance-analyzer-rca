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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeRole;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetServer;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.exceptions.MalformedConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.MetricsDBProvider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ThresholdMain;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.NodeStateManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.SubscriptionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.WireHopper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.handler.PublishRequestHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.handler.SubscribeServerHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.NetPersistor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.Persistable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.PersistenceFactory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.RCAScheduler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor.NodeDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rest.QueryRcaRequestHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

//TODO(Karthik) : Please add a doc comment for this class.
public class RcaController {

  private static final Logger LOG = LogManager.getLogger(RcaController.class);
  private static final String RCA_ENABLED_CONF_FILE = "rca_enabled.conf";
  public static final String CAT_MASTER_URL = "http://localhost:9200/_cat/master?h=ip";

  private final ScheduledExecutorService netOpsExecutorService;
  private final boolean useHttps;

  private boolean rcaEnabledDefaultValue = false;
  private boolean rcaEnabled = false;
  private NodeRole currentRole = NodeRole.UNKNOWN;
  private RCAScheduler rcaScheduler;
  private Thread rcaNetServerThread;

  private NetPersistor netPersistor;
  private NetClient rcaNetClient;
  private NetServer rcaNetServer;
  private NodeStateManager nodeStateManager;
  private HttpServer httpServer;
  private QueryRcaRequestHandler queryRcaRequestHandler;

  private SubscriptionManager subscriptionManager;
  private GRPCConnectionManager connectionManager;

  public RcaController(
      final ScheduledExecutorService netOpsExecutorService,
      final GRPCConnectionManager grpcConnectionManager,
      final NetClient rcaNetClient,
      final NetServer rcaNetServer,
      final HttpServer httpServer) {
    this.netOpsExecutorService = netOpsExecutorService;
    this.connectionManager = grpcConnectionManager;
    this.rcaNetClient = rcaNetClient;
    this.rcaNetServer = rcaNetServer;
    this.httpServer = httpServer;
    netPersistor = new NetPersistor();
    this.useHttps = PluginSettings.instance().getHttpsEnabled();
    subscriptionManager = new SubscriptionManager(grpcConnectionManager, rcaNetClient);
    nodeStateManager = new NodeStateManager();
    queryRcaRequestHandler = new QueryRcaRequestHandler();
    startRpcServer();
  }

  private void addRcaRequestHandler() {
    httpServer.createContext(Util.RCA_QUERY_URL, queryRcaRequestHandler);
  }

  private void removeRcaRequestHandler() {
    try {
      httpServer.removeContext(Util.RCA_QUERY_URL);
    } catch (IllegalArgumentException e) {
      LOG.debug("Http(s) context for path: {} was not found to remove.", Util.RCA_QUERY_URL);
    }
  }

  /**
   * Starts various pollers.
   */
  public void startPollers() {
    startRcaConfPoller();
    startNodeRolePoller();
    startRcaNanny();
  }

  private void startRpcServer() {
    if (rcaNetServer == null) {
      this.rcaNetServer = new NetServer(Util.RPC_PORT, 1, useHttps);
    }
    this.rcaNetServerThread = new Thread(rcaNetServer);
    this.rcaNetServerThread.start();
  }

  private void startRcaConfPoller() {
    netOpsExecutorService.scheduleAtFixedRate(this::readRcaEnabledFromConf, 0, 5, TimeUnit.SECONDS);
  }

  private void startNodeRolePoller() {
    netOpsExecutorService.scheduleAtFixedRate(
        () -> {
          final String electedMasterAddress = getElectedMasterHostAddress();
          final NodeDetails nodeDetails = ClusterDetailsEventProcessor.getCurrentNodeDetails();

          if (nodeDetails != null) {
            // The first entry in the node details array is the current node.
            final NodeRole currentNodeRole = NodeRole.valueOf(nodeDetails.getRole());
            handleNodeRoleChange(nodeDetails, electedMasterAddress);
          }
        },
        1,
        5,
        TimeUnit.SECONDS);
  }

  private String getElectedMasterHostAddress() {
    try {
      final URL url = new URL(CAT_MASTER_URL);
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod("GET");
      BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
      String inputLine = in.readLine();
      in.close();

      return inputLine;
    } catch (IOException e) {
      LOG.error("Could not get the elected master node: {}", e.getMessage());
      e.printStackTrace();
    }

    return "";
  }

  private void handleNodeRoleChange(
      final NodeDetails currentNode, final String electedMasterHostAddress) {
    final NodeRole currentNodeRole = NodeRole.valueOf(currentNode.getRole());
    if (currentNode.getHostAddress().equalsIgnoreCase(electedMasterHostAddress)) {
      currentRole = NodeRole.ELECTED_MASTER;
    } else {
      currentRole = currentNodeRole;
    }
  }

  /**
   * Reads the enabled/disabled value for RCA from the conf file.
   */
  private void readRcaEnabledFromConf() {
    Path filePath = Paths.get(Util.DATA_DIR, RCA_ENABLED_CONF_FILE);

    Util.invokePrivileged(
        () -> {
          try (Scanner sc = new Scanner(filePath)) {
            String nextLine = sc.nextLine();
            rcaEnabled = Boolean.parseBoolean(nextLine);
          } catch (Exception e) {
            LOG.error("Error reading RCA Enabled from Conf file: {}", e.getMessage());
            e.printStackTrace();
            rcaEnabled = rcaEnabledDefaultValue;
          }
        });
  }

  private void startRcaNanny() {
    netOpsExecutorService.scheduleAtFixedRate(
        () -> {
          if (rcaScheduler != null && rcaScheduler.isRunning()) {
            if (!rcaEnabled) {
              // Need to shutdown the rca scheduler
              stop();
            } else {
              subscriptionManager.dumpStats();
              if (rcaScheduler.getRole() != currentRole) {
                restart();
              }
            }
          } else {
            if (rcaEnabled && NodeRole.UNKNOWN != currentRole) {
              start();
            }
          }
        },
        2,
        5,
        TimeUnit.SECONDS);
  }

  private void start() {
    final RcaConf rcaConf = pickRcaConfForRole(currentRole);
    try {
      subscriptionManager.setCurrentLocus(rcaConf.getTagMap().get("locus"));
      List<ConnectedComponent> connectedComponents = RcaUtil.getAnalysisGraphComponents(rcaConf);
      Queryable db = new MetricsDBProvider();
      ThresholdMain thresholdMain = new ThresholdMain(RcaConsts.THRESHOLDS_PATH, rcaConf);
      Persistable persistable = PersistenceFactory.create(rcaConf);
      addRcaRequestHandler();
      queryRcaRequestHandler.setPersistable(persistable);
      WireHopper net =
          new WireHopper(netPersistor, nodeStateManager, rcaNetClient, subscriptionManager);
      this.rcaScheduler =
          new RCAScheduler(connectedComponents, db, rcaConf, thresholdMain, persistable, net);
      rcaNetServer.setSendDataHandler(new PublishRequestHandler(netPersistor, nodeStateManager));
      rcaNetServer.setSubscribeHandler(new SubscribeServerHandler(net, subscriptionManager));
      rcaScheduler.setRole(currentRole);
      rcaScheduler.start();
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | InvocationTargetException
        | InstantiationException
        | IllegalAccessException
        | MalformedConfig
        | SQLException e) {
      LOG.error("Couldn't build connected components or persistable.. Ran into {}", e.getMessage());
      e.printStackTrace();
      return;
    }
  }

  private void stop() {
    rcaScheduler.shutdown();
    rcaNetClient.shutdown();
    rcaNetServer.shutdown();
    removeRcaRequestHandler();
  }

  private void restart() {
    stop();
    start();
  }

  private RcaConf pickRcaConfForRole(final NodeRole nodeRole) {
    if (NodeRole.ELECTED_MASTER == nodeRole) {
      LOG.debug("picking elected master conf");
      return new RcaConf(RcaConsts.RCA_CONF_MASTER_PATH);
    }

    if (NodeRole.MASTER == nodeRole) {
      LOG.debug("picking idle master conf");
      return new RcaConf(RcaConsts.RCA_CONF_IDLE_MASTER_PATH);
    }

    if (NodeRole.DATA == nodeRole) {
      LOG.debug("picking data node conf");
      return new RcaConf(RcaConsts.RCA_CONF_PATH);
    }

    LOG.debug("picking default conf");
    return new RcaConf(RcaConsts.RCA_CONF_PATH);
  }
}
