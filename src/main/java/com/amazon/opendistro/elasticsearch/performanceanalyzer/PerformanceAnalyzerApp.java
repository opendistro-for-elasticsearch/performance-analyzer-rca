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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

public class PerformanceAnalyzerApp {
  private static final int WEBSERVICE_DEFAULT_PORT = 9600;
  private static final String WEBSERVICE_PORT_CONF_NAME = "webservice-listener-port";
  private static final String WEBSERVICE_BIND_HOST_NAME = "webservice-bind-host";
  // Use system default for max backlog.
  private static final int INCOMING_QUEUE_LENGTH = 1;
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
    METRIC_COLLECTOR_EXECUTOR.start();

    Thread readerThread =
        new Thread(
            () -> {
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
            });
    readerThread.start();

    ClientServers clientServers = startServers();
    startRcaController(clientServers);
  }

  /**
   * Start all the servers and clients for request processing. We start two servers: - httpServer:
   * To handle the curl requests sent to the endpoint. This is human readable and also used by the
   * perftop. - gRPC server: This is how metrics, RCAs etc are transported between nodes. and a gRPC
   * client.
   *
   * @return gRPC client and the gRPC server and the httpServer wrapped in a class.
   */
  public static ClientServers startServers() {
    boolean useHttps = PluginSettings.instance().getHttpsEnabled();

    GRPCConnectionManager connectionManager = new GRPCConnectionManager(useHttps);
    NetServer netServer = new NetServer(Util.RPC_PORT, 1, useHttps);
    NetClient netClient = new NetClient(connectionManager);
    MetricsRestUtil metricsRestUtil = new MetricsRestUtil();

    netServer.setMetricsHandler(new MetricsServerHandler());
    startRpcServerThread(netServer);
    HttpServer httpServer = createInternalServer(PluginSettings.instance(), getPortNumber());
    httpServer.createContext(QUERY_URL, new QueryMetricsRequestHandler(netClient, metricsRestUtil));

    return new ClientServers(httpServer, netServer, netClient);
  }

  /** This starts the GRPC server in a thread of its own. */
  private static void startRpcServerThread(NetServer netServer) {
    rcaNetServerThread = new Thread(netServer);
    rcaNetServerThread.start();
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
    boolean useHttps = PluginSettings.instance().getHttpsEnabled();

    GRPCConnectionManager connectionManager = new GRPCConnectionManager(useHttps);
    rcaController =
        new RcaController(
            netOperationsExecutor,
            connectionManager,
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

  public static HttpServer createInternalServer(PluginSettings settings, int internalPort) {
    try {
      Security.addProvider(new BouncyCastleProvider());
      HttpServer server;
      if (settings.getHttpsEnabled()) {
        server = createHttpsServer(internalPort);
      } else {
        server = createHttpServer(internalPort);
      }
      server.setExecutor(Executors.newCachedThreadPool());
      server.start();
      return server;
    } catch (java.net.BindException ex) {
      Runtime.getRuntime().halt(1);
    } catch (Exception ex) {
      ex.printStackTrace();
      Runtime.getRuntime().halt(1);
    }

    return null;
  }

  private static HttpServer createHttpsServer(int readerPort) throws Exception {
    HttpsServer server = null;
    String bindHost = getBindHost();
    if (bindHost != null && !bindHost.trim().isEmpty()) {
      LOG.info("Binding to Interface: {}", bindHost);
      server =
          HttpsServer.create(
              new InetSocketAddress(InetAddress.getByName(bindHost.trim()), readerPort),
              INCOMING_QUEUE_LENGTH);
    } else {
      LOG.info(
          "Value Not Configured for: {} Using default value: binding to all interfaces",
          WEBSERVICE_BIND_HOST_NAME);
      server = HttpsServer.create(new InetSocketAddress(readerPort), INCOMING_QUEUE_LENGTH);
    }

    TrustManager[] trustAllCerts =
        new TrustManager[] {
          new X509TrustManager() {

            public X509Certificate[] getAcceptedIssuers() {
              return null;
            }

            public void checkClientTrusted(X509Certificate[] certs, String authType) {}

            public void checkServerTrusted(X509Certificate[] certs, String authType) {}
          }
        };

    HostnameVerifier allHostsValid =
        new HostnameVerifier() {
          public boolean verify(String hostname, SSLSession session) {
            return true;
          }
        };

    // Install the all-trusting trust manager
    SSLContext sslContext = SSLContext.getInstance("TLSv1.2");

    KeyStore ks = CertificateUtils.createKeyStore();
    KeyManagerFactory kmf = KeyManagerFactory.getInstance("NewSunX509");
    kmf.init(ks, CertificateUtils.IN_MEMORY_PWD.toCharArray());
    sslContext.init(kmf.getKeyManagers(), trustAllCerts, null);

    HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
    HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
    server.setHttpsConfigurator(new HttpsConfigurator(sslContext));
    return server;
  }

  private static HttpServer createHttpServer(int readerPort) throws Exception {
    HttpServer server = null;
    String bindHost = getBindHost();
    if (bindHost != null && !bindHost.trim().isEmpty()) {
      LOG.info("Binding to Interface: {}", bindHost);
      server =
          HttpServer.create(
              new InetSocketAddress(InetAddress.getByName(bindHost.trim()), readerPort),
              INCOMING_QUEUE_LENGTH);
    } else {
      LOG.info(
          "Value Not Configured for: {} Using default value: binding to all interfaces",
          WEBSERVICE_BIND_HOST_NAME);
      server = HttpServer.create(new InetSocketAddress(readerPort), INCOMING_QUEUE_LENGTH);
    }

    return server;
  }

  private static int getPortNumber() {
    String readerPortValue;
    try {
      readerPortValue = PluginSettings.instance().getSettingValue(WEBSERVICE_PORT_CONF_NAME);

      if (readerPortValue == null) {
        LOG.info(
            "{} not configured; using default value: {}",
            WEBSERVICE_PORT_CONF_NAME,
            WEBSERVICE_DEFAULT_PORT);
        return WEBSERVICE_DEFAULT_PORT;
      }

      return Integer.parseInt(readerPortValue);
    } catch (Exception ex) {
      LOG.error(
          "Invalid Configuration: {} Using default value: {} AND Error: {}",
          WEBSERVICE_PORT_CONF_NAME,
          WEBSERVICE_DEFAULT_PORT,
          ex.toString());
      return WEBSERVICE_DEFAULT_PORT;
    }
  }

  private static String getBindHost() {
    return PluginSettings.instance().getSettingValue(WEBSERVICE_BIND_HOST_NAME);
  }
}
