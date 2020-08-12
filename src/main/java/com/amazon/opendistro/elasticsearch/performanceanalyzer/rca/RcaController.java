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

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.MUTE_ERROR_METRIC;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.ClientServers;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerThreads;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeRole;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetServer;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.exceptions.MalformedConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.AESRcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.MetricsDBProvider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Stats;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ThresholdMain;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaRuntimeMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rest.QueryRcaRequestHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.threads.ThreadProvider;
import com.google.common.annotations.VisibleForTesting;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
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

  public static final String RCA_ENABLED_CONF_FILE = "rca_enabled.conf";

  private final ScheduledExecutorService netOpsExecutorService;
  private final boolean useHttps;

  private boolean rcaEnabledDefaultValue = false;

  private final int WAIT_FOR_SCHED_START_SECS = 10;

  // This needs to be volatile as the RcaConfPoller writes it but the Nanny reads it.
  private volatile boolean rcaEnabled = false;

  // This needs to be volatile as the RcaConfPoller writes it but the Nanny reads it.
  private volatile long lastModifiedTimeInMillisInMemory = 0;

  // This needs to be volatile as the NodeRolePoller writes it but the Nanny reads it.
  protected volatile NodeRole currentRole = NodeRole.UNKNOWN;
  private volatile List<ConnectedComponent> connectedComponents;

  private final ThreadProvider threadProvider;
  private RCAScheduler rcaScheduler;

  private NetPersistor netPersistor;
  private NetClient rcaNetClient;
  private NetServer rcaNetServer;
  private NodeStateManager nodeStateManager;
  private HttpServer httpServer;
  private QueryRcaRequestHandler queryRcaRequestHandler;

  private SubscriptionManager subscriptionManager;
  private volatile RcaConf rcaConf;

  private final String RCA_ENABLED_CONF_LOCATION;
  private final long rcaStateCheckIntervalMillis;
  private final long roleCheckPeriodicity;

  private volatile boolean deliberateInterrupt;

  // Atomic reference to the networking threadpool as it is used by multiple threads. When we
  // replace the threadpool instance, we want the update to be visible to all others holding a
  // reference.
  private AtomicReference<ExecutorService> networkThreadPoolReference = new AtomicReference<>();
  private ReceivedFlowUnitStore receivedFlowUnitStore;

  private final AppContext appContext;

  protected volatile Queryable dbProvider = null;

  private volatile Persistable persistenceProvider;

  public RcaController(
      final ThreadProvider threadProvider,
      final ScheduledExecutorService netOpsExecutorService,
      final GRPCConnectionManager grpcConnectionManager,
      final ClientServers clientServers,
      final String rca_enabled_conf_location,
      final long rcaStateCheckIntervalMillis,
      final long nodeRoleCheckPeriodicityMillis,
      final AppContext appContext,
      final Queryable dbProvider) {
    this.threadProvider = threadProvider;
    this.appContext = appContext;
    this.netOpsExecutorService = netOpsExecutorService;
    this.rcaNetClient = clientServers.getNetClient();
    this.rcaNetServer = clientServers.getNetServer();
    this.httpServer = clientServers.getHttpServer();
    RCA_ENABLED_CONF_LOCATION = rca_enabled_conf_location;
    netPersistor = new NetPersistor();
    this.useHttps = PluginSettings.instance().getHttpsEnabled();
    subscriptionManager = new SubscriptionManager(grpcConnectionManager);
    nodeStateManager = new NodeStateManager(this.appContext);
    queryRcaRequestHandler = new QueryRcaRequestHandler(this.appContext);
    this.rcaScheduler = null;
    this.rcaStateCheckIntervalMillis = rcaStateCheckIntervalMillis;
    this.roleCheckPeriodicity = nodeRoleCheckPeriodicityMillis;
    this.deliberateInterrupt = false;
    this.connectedComponents = null;
    this.dbProvider = dbProvider;
    this.persistenceProvider = null;
  }

  @VisibleForTesting
  public RcaController() {
    netOpsExecutorService = null;
    useHttps = false;
    threadProvider = null;
    RCA_ENABLED_CONF_LOCATION = "";
    rcaStateCheckIntervalMillis = 0;
    roleCheckPeriodicity = 0;
    appContext = null;
    this.persistenceProvider = null;
  }

  protected List<ConnectedComponent> getRcaGraphComponents(
      RcaConf rcaConf) throws ClassNotFoundException,
      NoSuchMethodException,
      InstantiationException,
      IllegalAccessException,
      InvocationTargetException {
    return RcaUtil.getAnalysisGraphComponents(rcaConf);
  }

  private void start() {
    try {
      Objects.requireNonNull(subscriptionManager);
      Objects.requireNonNull(rcaConf);
      if (dbProvider == null) {
        return;
      }

      subscriptionManager.setCurrentLocus(rcaConf.getTagMap().get("locus"));
      this.connectedComponents = getRcaGraphComponents(rcaConf);

      // Mute the rca nodes after the graph creation and before the scheduler start
      readAndUpdateMutedComponentsDuringStart();

      ThresholdMain thresholdMain = new ThresholdMain(RcaConsts.THRESHOLDS_PATH, rcaConf);
      persistenceProvider = PersistenceFactory.create(rcaConf);
      networkThreadPoolReference
          .set(RcaControllerHelper.buildNetworkThreadPool(rcaConf.getNetworkQueueLength()));
      addRcaRequestHandler();
      queryRcaRequestHandler.setPersistable(persistenceProvider);
      receivedFlowUnitStore = new ReceivedFlowUnitStore(rcaConf.getPerVertexBufferLength());
      WireHopper net =
          new WireHopper(nodeStateManager, rcaNetClient, subscriptionManager,
              networkThreadPoolReference, receivedFlowUnitStore, appContext);

      // RcaScheduler should be started with a snapshot of the AppContext as RcaController
      // monitors it for stale state and always restarts the scheduler if it finds its state
      // stale.
      AppContext copyAppContext = new AppContext(this.appContext);

      this.rcaScheduler =
          new RCAScheduler(connectedComponents,
              dbProvider,
              rcaConf,
              thresholdMain,
              persistenceProvider,
              net,
              copyAppContext);

      rcaNetServer.setSendDataHandler(new PublishRequestHandler(
          nodeStateManager, receivedFlowUnitStore, networkThreadPoolReference));
      rcaNetServer.setSubscribeHandler(
          new SubscribeServerHandler(subscriptionManager, networkThreadPoolReference));

      Thread rcaSchedulerThread = threadProvider.createThreadForRunnable(() -> rcaScheduler.start(),
          PerformanceAnalyzerThreads.RCA_SCHEDULER,
          copyAppContext.getMyInstanceDetails().getInstanceId().toString());

      CountDownLatch schedulerStartLatch = new CountDownLatch(1);
      rcaScheduler.setSchedulerTrackingLatch(schedulerStartLatch);
      rcaSchedulerThread.start();
      if (!schedulerStartLatch.await(WAIT_FOR_SCHED_START_SECS, TimeUnit.SECONDS)) {
        LOG.error("Failed to start RcaScheduler.");
        throw new IllegalStateException(
            "Failed to start RcaScheduler within " + WAIT_FOR_SCHED_START_SECS + " seconds.");
      }

      if (rcaScheduler.getState() != RcaSchedulerState.STATE_STARTED) {
        LOG.error("RCA scheduler didn't start within {} seconds", WAIT_FOR_SCHED_START_SECS);
      }
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | InvocationTargetException
        | InstantiationException
        | IllegalAccessException
        | MalformedConfig
        | SQLException
        | IOException e) {
      LOG.error("Couldn't build connected components or persistable..", e);
    } catch (Exception ex) {
      LOG.error("Couldn't start RcaController", ex);
    }
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

  protected RcaConf getRcaConfForMyRole(NodeRole role) {
    return RcaControllerHelper.pickRcaConfForRole(role);
  }

  public void run() {
    long tick = 0;
    long nodeRoleCheckInTicks = roleCheckPeriodicity / rcaStateCheckIntervalMillis;
    while (true) {
      try {
        long startTime = System.currentTimeMillis();
        readRcaEnabledFromConf();
        if (rcaEnabled && tick % nodeRoleCheckInTicks == 0) {
          tick = 0;
          final InstanceDetails nodeDetails = appContext.getMyInstanceDetails();
          if (nodeDetails.getRole() != NodeRole.UNKNOWN) {
            currentRole = nodeDetails.getRole();
          }
        }

        // If RCA is enabled, update Analysis graph with Muted RCAs value
        if (rcaEnabled) {
          rcaConf = getRcaConfForMyRole(currentRole);
          LOG.debug("Updating Analysis Graph with Muted RCAs");
          readAndUpdateMutedComponents();
        }
        updateRcaState();

        long duration = System.currentTimeMillis() - startTime;
        if (duration < rcaStateCheckIntervalMillis) {
          Thread.sleep(rcaStateCheckIntervalMillis - duration);
        }
      } catch (InterruptedException ie) {
        if (deliberateInterrupt) {
          // This should only happen in case of tests. So, its okay for this log level to be info.
          LOG.info("RcaController thread interrupted..");
        } else {
          LOG.error("RCA controller thread was interrupted.", ie);
        }
        break;
      }
      tick++;
    }
    LOG.error("RcaController exits..");
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
            boolean oldVal = rcaEnabled;
            boolean newVal = Boolean.parseBoolean(nextLine);
            if (oldVal != newVal) {
              rcaEnabled = newVal;
              LOG.info("RCA enabled changed from {} to {}", oldVal, newVal);
            }
          } catch (IOException e) {
            LOG.error("Error reading file '{}': {}", filePath.toString(), e);
            e.printStackTrace();
            rcaEnabled = rcaEnabledDefaultValue;
          }
        });
  }

  private void readAndUpdateMutedComponentsDuringStart() {
    /* We have an edge case where both `readAndUpdateMutedComponents()` and `readAndUpdateMutedComponentsDuringStart()`
     * can try to update the muted Rca list back to back, reading rca.conf twice. This will happen when rca
     * was turned off and then on.
     *
     * <p> `readAndUpdateMutedComponentsDuringStart()` should only be read at the start of the process, when
     * RCA graph is not constructed and we cannot validate the new new muted RCAs. For any other update
     * to the muted list, the periodic rca.conf update checker will take care of it.
     *
     */
    if (lastModifiedTimeInMillisInMemory == 0) {
      updateMutedComponents();
    }
  }

  private boolean updateMutedComponents() {
    try {
      Set<String> allNodes = ConnectedComponent.getNodesForAllComponents(this.connectedComponents);
      if (allNodes.isEmpty()) {
        LOG.info("Analysis graph not initialized/has been reset; returning.");
        return false;
      }

      Set<String> actionsForMute = new HashSet<>(rcaConf.getMutedActionList());

      Set<String> graphNodesForMute = new HashSet<>();
      graphNodesForMute.addAll(rcaConf.getMutedRcaList());
      graphNodesForMute.addAll(rcaConf.getMutedDeciderList());
      LOG.info("Graph nodes provided for muting : {}", graphNodesForMute);
      LOG.info("Actions provided for muting: {}", actionsForMute);

      // Update rcasForMute to retain only valid RCAs
      graphNodesForMute.retainAll(allNodes);

      // If rcasForMute post validation is empty but neither rcaConf.getMutedRcaList() nor
      // rcaConf.getMutedDeciderList() are empty all the input RCAs/deciders are incorrect.
      if (graphNodesForMute.isEmpty()
          && (!rcaConf.getMutedRcaList().isEmpty() || !rcaConf.getMutedDeciderList().isEmpty())) {
        if (lastModifiedTimeInMillisInMemory == 0) {
          LOG.error(
              "Removing Incorrect RCA(s): {} provided before RCA Scheduler start. Valid RCAs: {}.",
              rcaConf.getMutedRcaList(), allNodes);

        } else {
          LOG.error("Incorrect RCA(s): {}, cannot be muted. Valid RCAs: {}, Muted RCAs: {}",
              rcaConf.getMutedRcaList(), allNodes,
              Stats.getInstance().getMutedGraphNodes());
          return false;
        }
      }

      LOG.info("Updating the muted graph nodes to : {}", graphNodesForMute);
      Stats.getInstance().updateMutedGraphNodes(graphNodesForMute);
      appContext.updateMutedActions(actionsForMute);
    } catch (Exception e) {
      LOG.error("Couldn't read/update the muted RCAs", e);
      StatsCollector.instance().logMetric(MUTE_ERROR_METRIC);
      return false;
    }

    return true;
  }

  /**
   * Reads the mutedRCAList value from the rca.conf file, performs validation on the param value
   * provided and on successful validation, updates the AnalysisGraph with muted RCA value.
   *
   * <p>In case all the RCAs in param value are incorrect, return without any update.
   */
  private void readAndUpdateMutedComponents() {
    // If the rca config file has been updated since the lastModifiedTimeInMillisInMemory in memory,
    // refresh the `muted-rcas` value from rca config file.
    long lastModifiedTimeInMillisOnDisk = rcaConf.getLastModifiedTime();
    if (lastModifiedTimeInMillisOnDisk > lastModifiedTimeInMillisInMemory) {
      if (updateMutedComponents()) {
        lastModifiedTimeInMillisInMemory = lastModifiedTimeInMillisOnDisk;
      }
    }

  }

  /**
   * Starts or stops the RCA runtime. If the RCA runtime is up but the currently RCA is disabled,
   * then this gracefully shuts down the RCA runtime. It restarts the RCA runtime if the node role
   * has changed in the meantime (such as a new elected master). It also starts the RCA runtime if
   * it wasn't already running but the current state of the flag expects it to.
   */
  private void updateRcaState() {
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
  }

  private void removeRcaRequestHandler() {
    try {
      httpServer.removeContext(Util.RCA_QUERY_URL);
    } catch (IllegalArgumentException e) {
      LOG.debug("Http(s) context for path: {} was not found to remove.", Util.RCA_QUERY_URL);
    }
  }

  public static String getCatMasterUrl() {
    return RcaControllerHelper.CAT_MASTER_URL;
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

  @VisibleForTesting
  public AppContext getAppContext() {
    return this.appContext;
  }

  public RCAScheduler getRcaScheduler() {
    return rcaScheduler;
  }

  private void addRcaRequestHandler() {
    httpServer.createContext(Util.RCA_QUERY_URL, queryRcaRequestHandler);
  }

  public void setDeliberateInterrupt() {
    deliberateInterrupt = true;
  }

  public RcaConf getRcaConf() {
    return rcaConf;
  }

  public void setDbProvider(Queryable dbProvider) {
    this.dbProvider = dbProvider;
  }

  @VisibleForTesting
  public List<ConnectedComponent> getConnectedComponents() {
    return connectedComponents;
  }

  @VisibleForTesting
  public Persistable getPersistenceProvider() {
    return persistenceProvider;
  }
}
