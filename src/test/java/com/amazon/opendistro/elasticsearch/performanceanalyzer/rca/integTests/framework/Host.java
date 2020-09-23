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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.ATable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.ATuple;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.Consts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.HostTag;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.overrides.RcaControllerIt;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.overrides.RcaItMetricsDBProvider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.RCAScheduler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.RcaSchedulerState;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.threads.ThreadProvider;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;
import org.jooq.Result;
import org.junit.Assert;

/**
 * This class simulates a cluster node that executes an RCA graph. Each node has its own
 * - GRPC server,
 * - web server
 * - RCAController and everything that it starts.
 */
public class Host {
  private static final Logger LOG = LogManager.getLogger(Host.class);
  private final boolean useHttps;
  /**
   * Each host has its own AppContext instance.
   */
  private final AppContext appContext;
  private final HostTag myTag;
  /**
   * This uniquely identifies a host.
   */
  private final int hostId;
  /**
   * For Integration tests, where all the virtual nodes are part of the same JVM, Ip string does not matter. But for
   * the sake of having this value filled, the string is 127.0.0.(hostId).
   */
  private final String hostIp;
  private final AllMetrics.NodeRole role;
  private final int webServerPort;
  private final int grpcServerPort;
  private final File hostDir;
  private final boolean rcaEnabled;
  private GRPCConnectionManager connectionManager;
  private ClientServers clientServers;
  private ScheduledExecutorService netOperationsExecutor;
  private RcaControllerIt rcaController;
  private Thread grpcThread;
  private Thread webServerThread;
  private Thread rcaControllerThread;
  private ThreadProvider threadProvider;
  private Path rcaEnabledFile;

  public Host(int hostId,
              boolean useHttps,
              AllMetrics.NodeRole role,
              int httpServerPort,
              int grpcServerPort,
              File clusterDir,
              boolean rcaEnabled,
              HostTag myTag) {
    this.rcaEnabled = rcaEnabled;
    this.useHttps = useHttps;
    this.appContext = new AppContext();

    this.hostId = hostId;
    this.myTag = myTag;

    //TODO: make sure this works with the grpc and the webserver.
    this.hostIp = createHostIp();
    this.role = role;

    this.webServerPort = httpServerPort;
    this.grpcServerPort = grpcServerPort;

    this.hostDir = createHostDir(clusterDir, myTag);
  }

  public static String createHostIp() {
    return "127.0.0.1";
  }

  private static File createHostDir(File clusterDir, HostTag hostTag) {
    File hostFile = Paths.get(clusterDir.getAbsolutePath(), hostTag.toString()).toFile();
    if (!hostFile.exists() && !hostFile.mkdirs()) {
      throw new IllegalStateException("Couldn't create dir: " + hostFile);
    }
    return hostFile;
  }

  public void createServersAndThreads(final ThreadProvider threadProvider) {
    this.threadProvider = threadProvider;
    Objects.requireNonNull(appContext.getClusterDetailsEventProcessor(),
        "ClusterDetailsEventProcessor cannot be null in the AppContext");

    rcaEnabledFile = Paths.get(hostDir.getAbsolutePath(), RcaController.RCA_ENABLED_CONF_FILE);
    RcaSchedulerState state = rcaEnabled ? RcaSchedulerState.STATE_STARTED : RcaSchedulerState.STATE_STOPPED;
    setExpectedRcaState(state);

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
        10,
        10,
        role,
        appContext,
        null);
  }

  // We create a temporary file and then swap it for the rca.enabled file.
  public void setExpectedRcaState(RcaSchedulerState rcaState) {
    Path rcaEnabledTmp = Paths.get(rcaEnabledFile + ".tmp");
    try (FileWriter f2 = new FileWriter(rcaEnabledTmp.toFile(), false /*To create a new file*/)) {
      boolean value = true;
      switch (rcaState) {
        case STATE_NOT_STARTED:
          break;
        case STATE_STOPPED_DUE_TO_EXCEPTION:
          break;
        case STATE_STARTED:
          value = true;
          break;
        case STATE_STOPPED:
          value = false;
          break;
      }
      f2.write(String.valueOf(value));
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }
    rcaEnabledTmp.toFile().renameTo(rcaEnabledFile.toFile());
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
        host.getMyTag().toString(),
        host.hostIp,
        host.isElectedMaster(),
        host.grpcServerPort);
  }

  public HostTag getMyTag() {
    return myTag;
  }

  public boolean isElectedMaster() {
    return AllMetrics.NodeRole.ELECTED_MASTER == this.role;
  }

  public void deleteHost() throws IOException {
    try {
      stopRcaScheduler();
    } catch (Exception e) {
      LOG.error("** Error shutting down the scheduler while deleting host.", e);
    }
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
    clientServers.getHttpServer().stop(10);
    clientServers.getNetClient().stop();
    clientServers.getNetServer().shutdown();

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

    LOG.info("RCA Controller thread for host {} is being interrupted." + hostId);
    rcaControllerThread.interrupt();
    deleteHostDir();
    LOG.info("Host '{} with role '{}' cleaned up.", hostId, rcaController.getCurrentRole());
  }

  public void deleteHostDir() throws IOException {
    FileUtils.deleteDirectory(hostDir);
  }

  public void stopRcaScheduler() throws Exception {
    RCAScheduler sched = rcaController.getRcaScheduler();
    CountDownLatch shutdownLatch = null;
    if (sched != null) {
      shutdownLatch = new CountDownLatch(1);
      sched.setSchedulerTrackingLatch(shutdownLatch);
    }
    setExpectedRcaState(RcaSchedulerState.STATE_STOPPED);
    if (shutdownLatch != null) {
      shutdownLatch.await(10, TimeUnit.SECONDS);
    }
    LOG.info("RCA Scheduler is STOPPED by TestRunner on node: {}", myTag);
  }

  public void startRcaControllerThread() {
    this.rcaControllerThread = PerformanceAnalyzerApp.startRcaTopLevelThread(
        rcaController,
        threadProvider,
        appContext.getMyInstanceDetails().getInstanceId().toString());
  }

  public void startRcaScheduler() throws Exception {
    setExpectedRcaState(RcaSchedulerState.STATE_STARTED);
    rcaController.waitForRcaState(RcaSchedulerState.STATE_STARTED);
  }

  public void updateRcaGraph(final Class rcaGraphClass)
      throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    rcaController.setRcaGraphComponents(rcaGraphClass);
  }

  public void updateMetricsDB(AMetric[] metricAnnotations) throws Exception {
    RcaItMetricsDBProvider dbProvider =
        new RcaItMetricsDBProvider(Paths.get(hostDir.getPath(), "metricsdb").toString());
    for (AMetric metric : metricAnnotations) {
      boolean foundDataForHost = false;
      // Each metric can have only one data table that can be associated to a host.
      // Which one is determined by the hostTag. The first matching is added to the host
      // for the current metric.
      dataLoop:
      for (ATable table : metric.tables()) {
        for (HostTag dataTag : table.hostTag()) {
          if (myTag == dataTag) {
            // First data-tag to match the hostTags is considered to be a match
            for (ATuple tuple : table.tuple()) {
              String metricName;
              try {
                metricName = (String) metric.name().getField("NAME").get(null);
              } catch (Exception ex) {
                LOG.error("Error getting metric name.", ex);
                throw ex;
              }
              dbProvider.insertRow(
                  metricName,
                  metric.dimensionNames(),
                  tuple.dimensionValues(),
                  tuple.min(),
                  tuple.max(),
                  tuple.avg(),
                  tuple.sum());
            }
            foundDataForHost = true;
            // We found a data table matching the tags of the host. Let's move to the
            // next metric.
            break dataLoop;
          }
        }
      }
      if (!foundDataForHost) {
        // This is not an error though. For example, a dedicated master node cannot emit
        // a shard related metric.
        System.out.println("No data found for host " + hostId + " for metric " + metric.name());
      }
    }
    rcaController.setDbProvider(dbProvider);
  }

  public JsonElement getDataForRca(String rcaName) {
    JsonElement data = this.rcaController.getPersistenceProvider().read(rcaName);
    JsonObject obj = new JsonObject();
    obj.addProperty(Consts.HOST_ID_KEY, hostId);
    obj.addProperty(Consts.HOST_ROLE_KEY, role.toString());
    obj.add(Consts.DATA_KEY, data);
    return obj;
  }

  public <T> T constructObjectFromDB(Class<T> className) {
    try {
      return this.rcaController.getPersistenceProvider().read(className);
    } catch (Exception e) {
      return null;
    }
  }

  public Map<String, Result<Record>> getRecordsForAllTables() {
    return this.rcaController.getPersistenceProvider().getRecordsForAllTables();
  }

  public String makeRestRequest(final Map<String, String> kvRequestParams) {
    StringBuilder queryString = new StringBuilder();

    String appender = "";
    for (Map.Entry<String, String> entry : kvRequestParams.entrySet()) {
      queryString.append(appender).append(entry.getKey()).append("=").append(entry.getValue());
      appender = "&";
    }
    StringBuilder uri =
        new StringBuilder("http://localhost:" + webServerPort + Util.RCA_QUERY_URL);
    uri.append("?").append(queryString);

    URL url = null;
    try {
      url = new URL(uri.toString());
    } catch (MalformedURLException e) {
      e.printStackTrace();
      Assert.fail();
    }

    String response = "";
    HttpURLConnection connection = null;

    try {
      connection = (HttpURLConnection) url.openConnection();
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail();
    }

    try {
      connection.setRequestMethod("GET");
    } catch (ProtocolException e) {
      e.printStackTrace();
      connection.disconnect();
      Assert.fail();
    }

    try {
      int status = connection.getResponseCode();
      if (status != 200) {
        List<String> ret =
            new BufferedReader(new InputStreamReader(connection.getErrorStream())).lines().collect(Collectors.toList());
        throw new IllegalStateException(ret.toString());
      }
    } catch (IOException e) {
      e.printStackTrace();
      connection.disconnect();
      Assert.fail();
    }

    try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
      String inputLine;
      StringBuffer content = new StringBuffer();
      while ((inputLine = in.readLine()) != null) {
        content.append(inputLine);
      }
      response = content.toString();
    } catch (IOException e) {
      e.printStackTrace();
      connection.disconnect();
      Assert.fail();
    }
    return response;
  }
}
