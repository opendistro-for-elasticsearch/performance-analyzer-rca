package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsRestUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetServer;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rest.QueryMetricsRequestHandler;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class RcaControllerTest {

  @Test
  public void startPollers() throws IOException, InterruptedException {
    GRPCConnectionManager connectionManager = new GRPCConnectionManager(true);
    NetServer netServer = new NetServer(Util.RPC_PORT, 1, true);
    NetClient netClient = new NetClient(connectionManager);
    MetricsRestUtil metricsRestUtil = new MetricsRestUtil();
    HttpServer httpServer =
        HttpServer.create(new InetSocketAddress(InetAddress.getByName("localhost"), 9200), 0);
    httpServer.createContext("/", new QueryMetricsRequestHandler(netClient, metricsRestUtil));
    final ScheduledExecutorService netOperationsExecutor =
        Executors.newScheduledThreadPool(
            2, new ThreadFactoryBuilder().setNameFormat("network-thread-%d").build());
    RcaController rcaController =
        new RcaController(
            netOperationsExecutor, connectionManager, netClient, netServer, httpServer);
    rcaController.startPollers();
    Thread.sleep(7000);
  }
}
