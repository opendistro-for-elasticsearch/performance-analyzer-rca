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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsRestUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.handler.MetricsServerHandler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetServer;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaController;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
            new Runnable() {
              public void run() {
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
              }
            });
    readerThread.start();

    boolean useHttps = PluginSettings.instance().getHttpsEnabled();
    GRPCConnectionManager connectionManager = new GRPCConnectionManager(useHttps);
    NetServer netServer = new NetServer(Util.RPC_PORT, 1, useHttps);
    NetClient netClient = new NetClient(connectionManager);
    MetricsRestUtil metricsRestUtil = new MetricsRestUtil();
    HttpServer httpServer = createInternalServer(settings, getPortNumber(), netClient, netServer);
    httpServer.createContext(QUERY_URL, new QueryMetricsRequestHandler(netClient, metricsRestUtil));
    RcaController rcaController =
        new RcaController(
            netOperationsExecutor, connectionManager, netClient, netServer, httpServer);
    rcaController.startPollers();
  }

  public static HttpServer createInternalServer(
      PluginSettings settings, int internalPort, NetClient netClient, NetServer netServer) {
    try {
      Security.addProvider(new BouncyCastleProvider());
      HttpServer server;
      if (settings.getHttpsEnabled()) {
        server = createHttpsServer(internalPort);
      } else {
        server = createHttpServer(internalPort);
      }
      netServer.setMetricsHandler(new MetricsServerHandler());
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
