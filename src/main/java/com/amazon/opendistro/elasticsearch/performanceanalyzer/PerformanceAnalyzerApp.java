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

package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.ScheduledMetricCollectorsExecutor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.TroubleshootingConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsRestUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.handler.MetricsServerHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetServer;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.ExceptionsAndErrors;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.JvmMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaGraphMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaRuntimeMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.sys.AllJvmSamplers;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.listener.MisbehavingGraphOperateMethodListener;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.RcaStatsReporter;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.collectors.SampleAggregator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.emitters.PeriodicSamplers;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.stats.listeners.IListener;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ReaderMetricsProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rest.QueryMetricsRequestHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks.ControllableTask;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks.GrpcServerRunnerTask;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks.PerformanceAnalyzerReaderTask;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks.RcaControllerTask;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks.WebServerRunnerTask;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sun.net.httpserver.HttpServer;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PerformanceAnalyzerApp {
  public static final String QUERY_URL = "/_opendistro/_performanceanalyzer/metrics";
  private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerApp.class);
  private static final ScheduledMetricCollectorsExecutor METRIC_COLLECTOR_EXECUTOR =
      new ScheduledMetricCollectorsExecutor(1, false);
  private static final ScheduledExecutorService netOperationsExecutor =
      Executors.newScheduledThreadPool(
          2, new ThreadFactoryBuilder().setNameFormat("network-thread-%d").build());

  private static RcaController rcaController = null;
  private static Thread rcaNetServerThread = null;
  private static BlockingQueue<PerformanceAnalyzerThreadException> concurrentQueue =
      new LinkedBlockingQueue<>(10);

  public static final SampleAggregator RCA_GRAPH_METRICS_AGGREGATOR =
          new SampleAggregator(RcaGraphMetrics.values());
  public  static final SampleAggregator RCA_RUNTIME_METRICS_AGGREGATOR =
          new SampleAggregator(RcaRuntimeMetrics.values());

  private static final IListener MISBEHAVING_NODES_LISTENER =
          new MisbehavingGraphOperateMethodListener();
  public static final SampleAggregator ERRORS_AND_EXCEPTIONS_AGGREGATOR =
          new SampleAggregator(MISBEHAVING_NODES_LISTENER.getMeasurementsListenedTo(),
                  MISBEHAVING_NODES_LISTENER,
                  ExceptionsAndErrors.values());

  public static final SampleAggregator JVM_METRICS_AGGREGATOR =
          new SampleAggregator(JvmMetrics.values());

  public static final RcaStatsReporter RCA_STATS_REPORTER =
          new RcaStatsReporter(Arrays.asList(RCA_GRAPH_METRICS_AGGREGATOR,
                  RCA_RUNTIME_METRICS_AGGREGATOR, ERRORS_AND_EXCEPTIONS_AGGREGATOR,
                  JVM_METRICS_AGGREGATOR));
  public static final PeriodicSamplers PERIODIC_SAMPLERS =
          new PeriodicSamplers(JVM_METRICS_AGGREGATOR, AllJvmSamplers.getJvmSamplers(),
                  (MetricsConfiguration.CONFIG_MAP.get(StatsCollector.class).samplingInterval) / 2,
                  TimeUnit.MILLISECONDS);

  public static void main(String[] args) throws Exception {
    // Initialize settings before creating threads.
    PluginSettings settings = PluginSettings.instance();

    StatsCollector.STATS_TYPE = "agent-stats-metadata";
    METRIC_COLLECTOR_EXECUTOR.addScheduledMetricCollector(StatsCollector.instance());
    StatsCollector.instance().addDefaultExceptionCode(StatExceptionCode.READER_RESTART_PROCESSING);
    METRIC_COLLECTOR_EXECUTOR.setEnabled(true);
    // Sensitive thread: Need to refactor with caution as it has implications on the writer.
    METRIC_COLLECTOR_EXECUTOR.start();
    ClientServers clientServers = getClientServers();

    // the top level thread manager.
    TaskManager taskManager = new TaskManager(concurrentQueue);

    ControllableTask readerTask = new PerformanceAnalyzerReaderTask();
    taskManager.submit(readerTask);

    ControllableTask grpcServerRunnerTask = new GrpcServerRunnerTask(clientServers.getNetServer());
    taskManager.submit(grpcServerRunnerTask);

    ControllableTask webServerTask = new WebServerRunnerTask(clientServers.getHttpServer());
    taskManager.submit(webServerTask);

    ControllableTask rcaControllerTask = new RcaControllerTask();
    taskManager.submit(rcaControllerTask);

    taskManager.waitForExceptionsOrTermination();
  }

  /**
   * Start all the servers and clients for request processing. We start two servers: - httpServer:
   * To handle the curl requests sent to the endpoint. This is human readable and also used by the
   * perftop. - gRPC server: This is how metrics, RCAs etc are transported between nodes. and a gRPC
   * client.
   *
   * @return gRPC client and the gRPC server and the httpServer wrapped in a class.
   */
  public static ClientServers getClientServers() {
    boolean useHttps = PluginSettings.instance().getHttpsEnabled();

    GRPCConnectionManager connectionManager = new GRPCConnectionManager(useHttps);
    NetServer netServer = new NetServer(Util.RPC_PORT, 1, useHttps);
    NetClient netClient = new NetClient(connectionManager);
    MetricsRestUtil metricsRestUtil = new MetricsRestUtil();

    netServer.setMetricsHandler(new MetricsServerHandler());
    PerformanceAnalyzerWebServer webServer = new PerformanceAnalyzerWebServer();
    HttpServer httpServer = webServer.createInternalServer();
    httpServer.createContext(QUERY_URL, new QueryMetricsRequestHandler(netClient, metricsRestUtil));

    return new ClientServers(httpServer, netServer, netClient, connectionManager);
  }

  /**
   * This starts the necessary threads to facilitate the running of the RCA framework. This may or
   * may not cause the RCA to start. RCA is started only if enableRCA flag is set through POST
   * request, otherwise, this method just spins up the necessary threads to start RCA on demand
   * without requiring a process restart.
   *
   * @param clientServers The httpServer, the gRPC server and client wrapper.
   */
  private static void startRcaController(ClientServers clientServers) {
    rcaController =
        new RcaController(
            netOperationsExecutor,
            clientServers.getConnectionManager(),
            clientServers.getNetClient(),
            clientServers.getNetServer(),
            clientServers.getHttpServer(),
            Util.DATA_DIR,
            RcaConsts.RCA_CONF_MASTER_PATH,
            RcaConsts.RCA_CONF_IDLE_MASTER_PATH,
            RcaConsts.RCA_CONF_PATH,
            RcaConsts.rcaNannyPollerPeriodicity,
            RcaConsts.rcaConfPollerPeriodicity,
            RcaConsts.nodeRolePollerPeriodicity,
            RcaConsts.rcaPollerPeriodicityTimeUnit);

    rcaController.startPollers();
  }
}
