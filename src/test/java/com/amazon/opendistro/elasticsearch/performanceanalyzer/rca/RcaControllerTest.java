package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ClientServers;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.MetricsRestUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetServer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(GradleTaskForRca.class)
public class RcaControllerTest {

  @Test
  public void startPollers() throws IOException, InterruptedException {
    // ClientServers clientServers = PerformanceAnalyzerApp.startServers();
    // boolean useHttps = PluginSettings.instance().getHttpsEnabled();

    // GRPCConnectionManager connectionManager = new GRPCConnectionManager(useHttps);
    // ScheduledExecutorService netOperationsExecutor =
    //         Executors.newScheduledThreadPool(
    //                 2, new ThreadFactoryBuilder().setNameFormat("network-thread-%d").build());
    // RcaController rcaController =
    //         new RcaController(
    //                 netOperationsExecutor,
    //                 connectionManager,
    //                 clientServers.getNetClient(),
    //                 clientServers.getNetServer(),
    //                 clientServers.getHttpServer(),
    //                 Util.DATA_DIR);

    // String cwd = System.getProperty("user.dir");
    // System.out.println(cwd);
    // rcaController.startPollers();
    NetServer netServer = new NetServer(Util.RPC_PORT, 1, false);
    netServer.run();
    //new Thread(netServer).start();
    //Thread.sleep(10000);
  }
}
