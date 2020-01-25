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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeRole;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.handler.MetricsServerHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetServer;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.exceptions.MalformedConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.MetricsDBProvider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Stats;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ThresholdMain;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaRuntimeMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.NodeStateManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.ReceivedFlowUnitStore;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.SubscriptionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.WireHopper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.handler.PublishRequestHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.handler.SubscribeServerHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.NetPersistor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.Persistable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.PersistenceFactory;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.RCAScheduler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.RcaSchedulerState;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor.NodeDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rest.QueryRcaRequestHandler;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This is responsible for bootstrapping the RCA. It sets the necessary state, initializes the
 * pollers to periodically check for the state of the system and react to a change if necessary. It
 * does all the preparations so that RCA runtime and graph evaluation can be started with a
 * configuration change but without needing a process restart.
 */
public class RcaController {

  private static final Logger LOG = LogManager.getLogger(RcaController.class);

  private static final String RCA_ENABLED_CONF_FILE = "rca_enabled.conf";

  public static final String CAT_MASTER_URL = "http://localhost:9200/_cat/master?h=ip";

  private final ScheduledExecutorService netOpsExecutorService;
  private final boolean useHttps;

  private boolean rcaEnabledDefaultValue = false;

  // This needs to be volatile as the RcaConfPoller writes it but the Nanny reads it.
  private volatile boolean rcaEnabled = false;

  // This needs to be volatile as the NodeRolePoller writes it but the Nanny reads it.
  private volatile NodeRole currentRole = NodeRole.UNKNOWN;

  private RCAScheduler rcaScheduler;

  private NetPersistor netPersistor;
  private NetClient rcaNetClient;
  private NetServer rcaNetServer;
  private NodeStateManager nodeStateManager;
  private HttpServer httpServer;
  private QueryRcaRequestHandler queryRcaRequestHandler;

  private SubscriptionManager subscriptionManager;

  private final String RCA_ENABLED_CONF_LOCATION;
  private final String ELECTED_MASTER_RCA_CONF_PATH;
  private final String MASTER_RCA_CONF_PATH;
  private final String RCA_CONF_PATH;

  private long rcaConfPollerPeriodicity;
  private long rcaNannyPollerPeriodicity;
  private long nodeRolePollerPeriodicty;
  private TimeUnit timeUnit;
  private List<Thread> exceptionHandlerThreads;
  private List<ScheduledFuture<?>> pollingExecutors;
  // Atomic reference to the networking threadpool as it is used by multiple threads. When we
  // replace the threadpool instance, we want the update to be visible to all others holding a
  // reference.
  private AtomicReference<ExecutorService> networkThreadPoolReference = new AtomicReference<>();
  private ReceivedFlowUnitStore receivedFlowUnitStore;

  public RcaController(
      final ScheduledExecutorService netOpsExecutorService,
      final GRPCConnectionManager grpcConnectionManager,
      final NetClient rcaNetClient,
      final NetServer rcaNetServer,
      final HttpServer httpServer,
      final String rca_enabled_conf_location,
      final String electedMasterRcaConf,
      final String masterRcaConf,
      final String rcaConf,
      long rcaNannyPollerPeriodicity,
      long rcaConfPollerPeriodicity,
      long nodeRolePollerPeriodicty,
      TimeUnit timeUnit) {
    this.netOpsExecutorService = netOpsExecutorService;
    this.rcaNetClient = rcaNetClient;
    this.rcaNetServer = rcaNetServer;
    this.httpServer = httpServer;
    RCA_ENABLED_CONF_LOCATION = rca_enabled_conf_location;
    netPersistor = new NetPersistor();
    this.useHttps = PluginSettings.instance().getHttpsEnabled();
    subscriptionManager = new SubscriptionManager(grpcConnectionManager);
    nodeStateManager = new NodeStateManager();
    queryRcaRequestHandler = new QueryRcaRequestHandler();
    this.rcaScheduler = null;
    this.ELECTED_MASTER_RCA_CONF_PATH = electedMasterRcaConf;
    this.MASTER_RCA_CONF_PATH = masterRcaConf;
    this.RCA_CONF_PATH = rcaConf;
    this.rcaNannyPollerPeriodicity = rcaNannyPollerPeriodicity;
    this.rcaConfPollerPeriodicity = rcaConfPollerPeriodicity;
    this.timeUnit = timeUnit;
    this.nodeRolePollerPeriodicty = nodeRolePollerPeriodicty;
    this.exceptionHandlerThreads = new ArrayList<>();
    this.pollingExecutors = new ArrayList<>();
  }

  /**
   * Starts the pollers. Each poller is a thread that checks for the state of the system that the
   * RCA is concerned with. - RcaConfPoller: Periodically reads a config file to determine if RCA is
   * supposed to be running or not and accordingly sets a flag. - NodeRolePoller: This checks for
   * the change of role for an elastic search Master node. - RcaNanny: RcaConfPoller sets the flag
   * but this thread works to start Rca or shut it down based on the flag.
   */
  public void startPollers() {
    pollingExecutors = new ArrayList<>();
    pollingExecutors.add(startRcaConfPoller());
    pollingExecutors.add(startRcaNanny());
    pollingExecutors.add(startNodeRolePoller());
    startExceptionHandlers(pollingExecutors);
  }

  public static String getCatMasterUrl() {
    return CAT_MASTER_URL;
  }

  public static String getRcaEnabledConfFile() {
    return RCA_ENABLED_CONF_FILE;
  }

  public boolean isRcaEnabled() {
    return rcaEnabled;
  }

  public NodeRole getCurrentRole() {
    return currentRole;
  }

  public RCAScheduler getRcaScheduler() {
    return rcaScheduler;
  }

  private void startExceptionHandlers(List<ScheduledFuture<?>> scheduledFutures) {
    scheduledFutures.forEach(
        future -> {
          Thread t =
              new Thread(
                  () -> {
                    try {
                      future.get();
                    } catch (CancellationException cex) {
                      LOG.info("Executor cancellation requested.");
                    } catch (Exception ex) {
                      LOG.error("RCA Exception cause : {}", ex.getCause());
                      ex.printStackTrace();
                    }
                  });
          exceptionHandlerThreads.add(t);
          t.start();
        });
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

  private ScheduledFuture<?> startRcaConfPoller() {
    return netOpsExecutorService.scheduleAtFixedRate(
        this::readRcaEnabledFromConf, 0, rcaConfPollerPeriodicity, timeUnit);
  }

  private ScheduledFuture<?> startNodeRolePoller() {
    return netOpsExecutorService.scheduleAtFixedRate(() -> {
      if (rcaEnabled) {
        final NodeDetails nodeDetails = ClusterDetailsEventProcessor.getCurrentNodeDetails();
        if (nodeDetails != null) {
          handleNodeRoleChange(nodeDetails);
        }
      }
    }, 2, nodeRolePollerPeriodicty, timeUnit);
  }

  /**
   * Starts or stops the RCA runtime. If the RCA runtime is up but the currently RCA is disabled,
   * then this gracefully shuts down the RCA runtime. It restarts the RCA runtime if the node role
   * has changed in the meantime (such as a new elected master). It also starts the RCA runtime if
   * it wasn't already running but the current state of the flag expects it to.
   */
  private ScheduledFuture<?> startRcaNanny() {
    return netOpsExecutorService.scheduleAtFixedRate(
        () -> {
          if (rcaScheduler != null && rcaScheduler.getState() == RcaSchedulerState.STATE_STARTED) {
            if (!rcaEnabled) {
              // Need to shutdown the rca scheduler
              stop();
              PerformanceAnalyzerApp.RCA_RUNTIME_METRICS_AGGREGATOR.updateStat(
                      RcaRuntimeMetrics.RCA_STOPPED_BY_OPERATOR, "", 1);
            } else {
              if (rcaScheduler.getRole() != currentRole) {
                restart();
                PerformanceAnalyzerApp.RCA_RUNTIME_METRICS_AGGREGATOR.updateStat(
                        RcaRuntimeMetrics.RCA_RESTARTED_BY_OPERATOR, "", 1);
              }
            }
          } else {
            // Start the scheduler if all the following conditions are met:
            // 1. rca is enabled
            // 2. we know the role of this es node
            // 3. scheduler is not stopped due to an exception.
            if (rcaEnabled && NodeRole.UNKNOWN != currentRole && (rcaScheduler == null
                || rcaScheduler.getState() != RcaSchedulerState.STATE_STOPPED_DUE_TO_EXCEPTION)) {
              start();
            }
          }
        },
        2 * rcaConfPollerPeriodicity,
        rcaNannyPollerPeriodicity,
        timeUnit);
  }

  private String getElectedMasterHostAddress() {
    try {
      LOG.info("Making _cat/master call");
      PerformanceAnalyzerApp.RCA_RUNTIME_METRICS_AGGREGATOR.updateStat(
              RcaRuntimeMetrics.ES_APIS_CALLED, "catMaster", 1);

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

  private void handleNodeRoleChange(final NodeDetails currentNode) {
    final NodeRole currentNodeRole = NodeRole.valueOf(currentNode.getRole());
    Boolean isMasterNode = currentNode.getIsMasterNode();
    if (isMasterNode != null) {
      currentRole = isMasterNode ? NodeRole.ELECTED_MASTER : currentNodeRole;
    } else {
      final String electedMasterHostAddress = getElectedMasterHostAddress();
      currentRole = currentNode.getHostAddress().equalsIgnoreCase(electedMasterHostAddress)
          ? NodeRole.ELECTED_MASTER : currentNodeRole;
    }
  }

  /**
   * Reads the enabled/disabled value for RCA from the conf file.
   */
  private void readRcaEnabledFromConf() {
    Path filePath = Paths.get(RCA_ENABLED_CONF_LOCATION, RCA_ENABLED_CONF_FILE);

    Util.invokePrivileged(
        () -> {
          try (Scanner sc = new Scanner(filePath)) {
            String nextLine = sc.nextLine();
            rcaEnabled = Boolean.parseBoolean(nextLine);
          } catch (IOException e) {
            LOG.error("Error reading file '{}': {}", filePath.toString(), e.getMessage());
            e.printStackTrace();
            rcaEnabled = rcaEnabledDefaultValue;
          }
        });
  }

  private void start() {
    final RcaConf rcaConf = pickRcaConfForRole(currentRole);
    try {
      subscriptionManager.setCurrentLocus(rcaConf.getTagMap().get("locus"));
      List<ConnectedComponent> connectedComponents = RcaUtil.getAnalysisGraphComponents(rcaConf);
      Queryable db = new MetricsDBProvider();
      ThresholdMain thresholdMain = new ThresholdMain(RcaConsts.THRESHOLDS_PATH, rcaConf);
      Persistable persistable = PersistenceFactory.create(rcaConf);
      networkThreadPoolReference.set(buildNetworkThreadPool(rcaConf.getNetworkQueueLength()));
      addRcaRequestHandler();
      queryRcaRequestHandler.setPersistable(persistable);
      receivedFlowUnitStore = new ReceivedFlowUnitStore(rcaConf.getPerVertexBufferLength());
      WireHopper net =
          new WireHopper(nodeStateManager, rcaNetClient, subscriptionManager,
              networkThreadPoolReference, receivedFlowUnitStore);
      this.rcaScheduler =
          new RCAScheduler(connectedComponents, db, rcaConf, thresholdMain, persistable, net);

      rcaNetServer.setMetricsHandler(new MetricsServerHandler());
      rcaNetServer.setSendDataHandler(new PublishRequestHandler(
          nodeStateManager, receivedFlowUnitStore, networkThreadPoolReference));
      rcaNetServer.setSubscribeHandler(
          new SubscribeServerHandler(subscriptionManager, networkThreadPoolReference));

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
    }
  }

  private ExecutorService buildNetworkThreadPool(final int queueLength) {
    final ThreadFactory rcaNetThreadFactory =
        new ThreadFactoryBuilder().setNameFormat(RcaConsts.RCA_NETWORK_THREAD_NAME_FORMAT)
                                  .setDaemon(true)
                                  .build();
    final BlockingQueue<Runnable> threadPoolQueue = new LinkedBlockingQueue<>(queueLength);
    return new ThreadPoolExecutor(RcaConsts.NETWORK_CORE_THREAD_COUNT,
        RcaConsts.NETWORK_MAX_THREAD_COUNT, 0L, TimeUnit.MILLISECONDS, threadPoolQueue,
        rcaNetThreadFactory);
  }

  private void stop() {
    rcaScheduler.shutdown();
    rcaNetClient.stop();
    rcaNetServer.stop();
    receivedFlowUnitStore.drainAll();
    networkThreadPoolReference.get().shutdown();
    try {
      networkThreadPoolReference.get().awaitTermination(1, TimeUnit.MINUTES);
    } catch (InterruptedException e) {
      LOG.warn("Awaiting termination interrupted. {}", e.getCause(), e);
      networkThreadPoolReference.get().shutdownNow();
      // Set the thread's interrupt interrupt flag so that higher level interrupt handlers can
      // take appropriate actions.
      Thread.currentThread().interrupt();
    }
    removeRcaRequestHandler();
    Stats.getInstance().reset();
  }

  private void restart() {
    stop();
    start();
    StatsCollector.instance().logMetric(RcaConsts.RCA_SCHEDULER_RESTART_METRIC);
  }

  private RcaConf pickRcaConfForRole(final NodeRole nodeRole) {
    if (NodeRole.ELECTED_MASTER == nodeRole) {
      LOG.debug("picking elected master conf");
      return new RcaConf(ELECTED_MASTER_RCA_CONF_PATH);
    }

    if (NodeRole.MASTER == nodeRole) {
      LOG.debug("picking idle master conf");
      return new RcaConf(MASTER_RCA_CONF_PATH);
    }

    if (NodeRole.DATA == nodeRole) {
      LOG.debug("picking data node conf");
      return new RcaConf(RCA_CONF_PATH);
    }

    LOG.debug("picking default conf");
    return new RcaConf(RCA_CONF_PATH);
  }
}
