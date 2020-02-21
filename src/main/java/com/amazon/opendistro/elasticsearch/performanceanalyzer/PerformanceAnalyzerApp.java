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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks.ThreadProvider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks.exceptions.PAThreadException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sun.net.httpserver.HttpServer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PerformanceAnalyzerApp {

  private static final int WEBSERVICE_DEFAULT_PORT = 9600;
  private static final String WEBSERVICE_PORT_CONF_NAME = "webservice-listener-port";
  private static final String WEBSERVICE_BIND_HOST_NAME = "webservice-bind-host";
  // Use system default for max backlog.
  private static final int INCOMING_QUEUE_LENGTH = 1;
  // current number of threads spawned through the thread provider.
  private static final int EXCEPTION_QUEUE_LENGTH = 5;
  public static final String QUERY_URL = "/_opendistro/_performanceanalyzer/metrics";
  private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerApp.class);
  private static final ScheduledMetricCollectorsExecutor METRIC_COLLECTOR_EXECUTOR =
      new ScheduledMetricCollectorsExecutor(1, false);
  private static final ScheduledExecutorService netOperationsExecutor =
      Executors.newScheduledThreadPool(
          2, new ThreadFactoryBuilder().setNameFormat("network-thread-%d").build());

  private static RcaController rcaController = null;
  private static Thread rcaNetServerThread = null;

  public static final SampleAggregator RCA_GRAPH_METRICS_AGGREGATOR =
      new SampleAggregator(RcaGraphMetrics.values());
  public static final SampleAggregator RCA_RUNTIME_METRICS_AGGREGATOR =
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
  public static final BlockingQueue<PAThreadException> exceptionQueue =
      new LinkedBlockingQueue<>(EXCEPTION_QUEUE_LENGTH);
  private static final int ERROR_HANDLING_POLLING_INTERVAL_IN_MS = 5000;

  public static void main(String[] args) throws Exception {
    PluginSettings settings = PluginSettings.instance();
    final GRPCConnectionManager connectionManager = new GRPCConnectionManager(
        settings.getHttpsEnabled());
    final ClientServers clientServers = startServers(connectionManager);
    startErrorHandlingThread();
    startReaderThread();
    startGrpcServerThread(clientServers.getNetServer());
    startWebServerThread(clientServers.getHttpServer());
    startRcaTopLevelThread(clientServers, connectionManager);
  }

  private static void startRcaTopLevelThread(final ClientServers clientServers,
      final GRPCConnectionManager connectionManager) {
    rcaController =
        new RcaController(
            netOperationsExecutor,
            connectionManager,
            clientServers,
            Util.DATA_DIR,
            RcaConsts.RCA_STATE_CHECK_INTERVAL_IN_MS,
            RcaConsts.nodeRolePollerPeriodicityInSeconds * 1000
        );

    Thread rcaControllerThread = ThreadProvider.instance()
                                               .createThreadForRunnable(() -> rcaController.run(),
                                                   "rca-controller");
    rcaControllerThread.start();
  }

  private static void startErrorHandlingThread() {
    final Thread errorHandlingThread = ThreadProvider.instance().createThreadForRunnable(() -> {
      while (true) {
        try {
          long startTime = System.currentTimeMillis();
          List<PAThreadException> exceptions = new ArrayList<>();
          exceptionQueue.drainTo(exceptions);
          for (PAThreadException e : exceptions) {
            handle(e);
          }
          long duration = System.currentTimeMillis() - startTime;
          if (duration < ERROR_HANDLING_POLLING_INTERVAL_IN_MS) {
            Thread.sleep(ERROR_HANDLING_POLLING_INTERVAL_IN_MS - duration);
          }
        } catch (InterruptedException ie) {
          LOG.error("Exception handling thread was interrupted. Cause: {}", ie.getCause(), ie);
        }
      }
    }, "pa-err-handler");

    errorHandlingThread.start();
  }

  private static void handle(PAThreadException exception) {
    LOG.error("Thread: {} ran into an uncaught exception: {}", exception.getThreadName(),
        exception.getInnerThrowable());
  }

  private static void startWebServerThread(final HttpServer server) {
    final Thread webServerThread = ThreadProvider.instance().createThreadForRunnable(server::start,
        "pa-web-server");
    webServerThread.start();
  }

  private static void startGrpcServerThread(final NetServer server) {
    final Thread grpcServerThread = ThreadProvider.instance()
                                                  .createThreadForRunnable(server, "grpc-server");
    grpcServerThread.start();
  }

  private static void startReaderThread() {
    PluginSettings settings = PluginSettings.instance();
    final Thread readerThread = ThreadProvider.instance().createThreadForRunnable(() -> {
      while (true) {
        try {
          ReaderMetricsProcessor mp =
              new ReaderMetricsProcessor(settings.getMetricsLocation(), true);
          ReaderMetricsProcessor.setCurrentInstance(mp);
          mp.run();
        } catch (Throwable e) {
          if (TroubleshootingConfig.getEnableDevAssert()) {
            break;
          }
          LOG.error(
              "Error in ReaderMetricsProcessor...restarting, ExceptionCode: {}",
              StatExceptionCode.READER_RESTART_PROCESSING.toString());
          StatsCollector.instance()
                        .logException(StatExceptionCode.READER_RESTART_PROCESSING);
        }
      }
    }, "pa-reader");

    readerThread.start();
  }

  /**
   * Start all the servers and clients for request processing. We start two servers: - httpServer:
   * To handle the curl requests sent to the endpoint. This is human readable and also used by the
   * perftop. - gRPC server: This is how metrics, RCAs etc are transported between nodes. and a gRPC
   * client.
   *
   * @return gRPC client and the gRPC server and the httpServer wrapped in a class.
   */
  public static ClientServers startServers(final GRPCConnectionManager connectionManager) {
    boolean useHttps = PluginSettings.instance().getHttpsEnabled();

    NetServer netServer = new NetServer(Util.RPC_PORT, 1, useHttps);
    NetClient netClient = new NetClient(connectionManager);
    MetricsRestUtil metricsRestUtil = new MetricsRestUtil();

    netServer.setMetricsHandler(new MetricsServerHandler());
    HttpServer httpServer =
        PerformanceAnalyzerWebServer.createInternalServer(PluginSettings.instance());
    httpServer.createContext(QUERY_URL, new QueryMetricsRequestHandler(netClient, metricsRestUtil));

    return new ClientServers(httpServer, netServer, netClient);
  }
}
