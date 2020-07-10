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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.ClientServers;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.RCAScheduler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.RcaSchedulerState;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.spec.MetricsDBProviderTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.threads.ThreadProvider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.threads.exceptions.PAThreadException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;

/**
 * This class simulates a cluster node that executes an RCA graph. Each node has its own
 * - GRPC server,
 * - web server
 * - RCAController and everything that it starts.
 */
public class Host {
  enum RcaState {
    ENABLED,
    DISABLED
  }

  private final boolean useHttps;
  /**
   * This uniquely identifies a host.
   */
  private int hostId;

  /**
   * For Integration tests, where all the virtual nodes are part of the same JVM, Ip string does not matter. But for
   * the sake of having this value filled, the string is 127.0.0.(hostId).
   */
  private String hostIp;

  private AllMetrics.NodeRole role;

  private GRPCConnectionManager connectionManager;
  private ClientServers clientServers;
  private ScheduledExecutorService netOperationsExecutor;
  private RcaControllerIt rcaController;

  private int webServerPort;
  private int grpcServerPort;

  private Thread grpcThread;
  private Thread webServerThread;
  private Thread rcaControllerThread;

  private File hostDir;
  private boolean rcaEnabled;

  private boolean paused;

  private ThreadProvider threadProvider;

  private File rcaEnabledFile;

  /**
   * Each host has its own AppContext instance.
   */
  private final AppContext appContext;

  public Host(int hostId,
              boolean useHttps,
              AllMetrics.NodeRole role,
              int httpServerPort,
              int grpcServerPort,
              File clusterDir,
              boolean rcaEnabled) {
    this.rcaEnabled = rcaEnabled;
    this.useHttps = useHttps;
    this.appContext = new AppContext();

    this.hostId = hostId;

    //TODO: make sure this works with the grpc and the webserver.
    this.hostIp = createHostIp(hostId);
    this.role = role;

    this.webServerPort = httpServerPort;
    this.grpcServerPort = grpcServerPort;

    this.hostDir = createHostDir(clusterDir, hostId);
    this.paused = true;
  }

  public void createServersAndThreads(final ThreadProvider threadProvider) throws Exception {
    this.threadProvider = threadProvider;
    Objects.requireNonNull(appContext.getClusterDetailsEventProcessor(),
        "ClusterDetailsEventProcessor cannot be null in the AppContext");

    rcaEnabledFile = Paths.get(hostDir.getAbsolutePath(), RcaController.RCA_ENABLED_CONF_FILE).toFile();
    createRcaEnabledFile(rcaEnabled, rcaEnabledFile);

    this.connectionManager = new GRPCConnectionManager(useHttps);
    this.clientServers = PerformanceAnalyzerApp.createClientServers(connectionManager,
        grpcServerPort,
        null,
        null,
        useHttps,
        String.valueOf(webServerPort),
        null,  // A null host is fine as this will use the loopback address
        this.appContext);

    this.grpcThread = PerformanceAnalyzerApp.startGrpcServerThread(clientServers.getNetServer(), threadProvider);
    this.webServerThread = PerformanceAnalyzerApp.startWebServerThread(clientServers.getHttpServer(), threadProvider);

    netOperationsExecutor =
        Executors.newScheduledThreadPool(
            3, new ThreadFactoryBuilder().setNameFormat("test-network-thread-%d").build());

    this.rcaController = new RcaControllerIt(
        threadProvider,
        netOperationsExecutor,
        connectionManager,
        clientServers,
        hostDir.getAbsolutePath(),
        RcaConsts.RCA_STATE_CHECK_INTERVAL_IN_MS,
        RcaConsts.nodeRolePollerPeriodicityInSeconds * 1000,
        role,
        appContext);
    rcaController.setDbProvider(new MetricsDBProviderTestHelper());
  }

  public void setDbProvider(final Queryable db) {
    rcaController.setDbProvider(db);
  }

  public void setClusterDetails(final List<Host> allHosts) {
    List<ClusterDetailsEventProcessor.NodeDetails> nodeDetails = new ArrayList<>();

    // The first node in the list is always the node-itself.
    nodeDetails.add(hostToNodeDetails(this));

    for (Host host : allHosts) {
      if (host.hostId != this.hostId) {
        nodeDetails.add(hostToNodeDetails(host));
      }
    }
    ClusterDetailsEventProcessor clusterDetailsEventProcessor = new ClusterDetailsEventProcessor();
    clusterDetailsEventProcessor.setNodesDetails(nodeDetails);
    appContext.setClusterDetailsEventProcessor(clusterDetailsEventProcessor);
  }

  public static ClusterDetailsEventProcessor.NodeDetails hostToNodeDetails(final Host host) {
    return new ClusterDetailsEventProcessor.NodeDetails(
        host.role,
        "host" + host.hostId,
        host.hostIp,
        host.isElectedMaster());
  }

  public void deleteHost() throws IOException {
    RCAScheduler rcaScheduler = rcaController.getRcaScheduler();
    if (rcaScheduler != null && rcaScheduler.getState() == RcaSchedulerState.STATE_STARTED) {
      rcaScheduler.shutdown();
    }
    netOperationsExecutor.shutdown();
    try {
      netOperationsExecutor.awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    clientServers.getHttpServer().stop(0);
    clientServers.getNetClient().stop();
    clientServers.getNetServer().stop();

    connectionManager.shutdown();

    try {
      Thread.sleep(1000);
    } catch (InterruptedException ie) {
      ie.printStackTrace();
    }

    webServerThread.interrupt();

    clientServers.getNetServer().setAttemptedShutdown();
    grpcThread.interrupt();

    rcaController.setDeliberateInterrupt();
    rcaControllerThread.interrupt();

    FileUtils.deleteDirectory(hostDir);

    System.out.println("Host '" + hostId + "' with role '" + rcaController.getCurrentRole() + "' cleaned up");
  }

  private static File createHostDir(File clusterDir, int hostId) {
    File hostFile = Paths.get(clusterDir.getAbsolutePath(), "host." + hostId).toFile();
    if (!hostFile.exists() && !hostFile.mkdirs()) {
      throw new IllegalStateException("Couldn't create dir: " + hostFile);
    }
    return hostFile;
  }

  private static void createRcaEnabledFile(boolean enableRca, File rcaEnabledFile) {
    try (FileWriter f2 = new FileWriter(rcaEnabledFile, false /*To create a new file*/)) {
      f2.write(String.valueOf(enableRca));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void setRcaState(RcaState rcaState) {
    try (FileWriter f2 = new FileWriter(rcaEnabledFile, false /*To create a new file*/)) {
      boolean value = true;
      switch (rcaState) {
        case ENABLED:
          value = true;
          break;
        case DISABLED:
          value = false;
          break;
      }
      f2.write(String.valueOf(value));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public boolean isElectedMaster() {
    return AllMetrics.NodeRole.ELECTED_MASTER == this.role;
  }

  public static String createHostIp(int hostId) {
    return "127.0.0.1";
  }

  public void startRcaController() {
    this.rcaControllerThread = PerformanceAnalyzerApp.startRcaTopLevelThread(rcaController, threadProvider);
    paused = false;
  }

  public void pause() throws Exception {
    setRcaState(RcaState.DISABLED);
    rcaController.waitForRcaState(false);
    paused = true;
  }

  public void unPause() throws Exception {
    setRcaState(RcaState.ENABLED);
    rcaController.waitForRcaState(false);
    paused = false;
  }

  public void updateRcaGraph(final List<ConnectedComponent> connectedComponents) {
    Assert.assertTrue(paused);
    rcaController.setRcaGraphComponents(connectedComponents);
  }
}
