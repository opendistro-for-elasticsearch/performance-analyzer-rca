package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ClientServers;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.jooq.tools.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class RcaControllerTest {
  private ScheduledExecutorService netOperationsExecutor;
  private ClientServers clientServers;
  private GRPCConnectionManager connectionManager;
  private Path rcaEnabledFileLoc;
  private Path rcaEnabledFile;
  private HttpServer dummyEsServer;
  private RcaController rcaController;
  private String masterIP;

  @Before
  public void setUp() throws IOException {
    String cwd = System.getProperty("user.dir");
    rcaEnabledFileLoc = Paths.get(cwd, "src", "test", "resources", "rca");
    rcaEnabledFile = Paths.get(rcaEnabledFileLoc.toString(), RcaController.getRcaEnabledConfFile());
    netOperationsExecutor =
        Executors.newScheduledThreadPool(
            1, new ThreadFactoryBuilder().setNameFormat("test-network-thread-%d").build());
    clientServers = PerformanceAnalyzerApp.startServers();

    URI uri = URI.create(RcaController.getCatMasterUrl());
    masterIP = "";

    dummyEsServer =
        HttpServer.create(
            new InetSocketAddress(InetAddress.getByName(uri.getHost()), uri.getPort()), 1);
    dummyEsServer.createContext(
        "/",
        exchange -> {
          String response = "Only supported endpoint is " + uri.getPath();
          exchange.sendResponseHeaders(200, response.getBytes().length);
          OutputStream os = exchange.getResponseBody();
          os.write(response.getBytes());
          os.close();
        });
    dummyEsServer.createContext(
        uri.getPath(),
        exchange -> {
          String response = masterIP;
          exchange.sendResponseHeaders(200, response.getBytes().length);
          OutputStream os = exchange.getResponseBody();
          os.write(response.getBytes());
          os.close();
        });
    dummyEsServer.start();
    System.out.println("Started dummy endpoint..");

    boolean useHttps = PluginSettings.instance().getHttpsEnabled();
    connectionManager = new GRPCConnectionManager(useHttps);
    rcaController =
        new RcaController(
            netOperationsExecutor,
            connectionManager,
            clientServers.getNetClient(),
            clientServers.getNetServer(),
            clientServers.getHttpServer(),
            rcaEnabledFileLoc.toString(),
            Paths.get(rcaEnabledFileLoc.toString(), "rca_elected_master.conf").toString(),
            Paths.get(rcaEnabledFileLoc.toString(), "rca_master.conf").toString(),
            Paths.get(rcaEnabledFileLoc.toString(), "rca.conf").toString());

    rcaController.startPollers();
  }

  @After
  public void tearDown() throws InterruptedException {
    netOperationsExecutor.shutdown();
    netOperationsExecutor.awaitTermination(1, TimeUnit.MINUTES);
    clientServers.getHttpServer().stop(0);
    clientServers.getNetClient().shutdown();
    clientServers.getNetServer().shutdown();
    //connectionManager.shutdown();
    dummyEsServer.stop(0);
  }

  @Test
  public void readRcaEnabledFromConf() throws IOException, InterruptedException {
    changeRcaRunState(RcaState.STOP);
    for (int i = 0; i < 10; i++) {
      if (!rcaController.isRcaEnabled()) {
        System.out.println("Config value read as false. Exiting early..");
        break;
      }
      Thread.sleep(1000);
    }
    Assert.assertFalse(rcaController.isRcaEnabled());

    changeRcaRunState(RcaState.RUN);
    for (int i = 0; i < 10; i++) {
      if (rcaController.isRcaEnabled()) {
        System.out.println("Config value read as true. Exiting early..");
        break;
      }
      Thread.sleep(1000);
    }
    Assert.assertTrue(rcaController.isRcaEnabled());
  }

  @Test
  public void nodeRoleChange() throws InterruptedException, IOException {
    changeRcaRunState(RcaState.STOP);
    masterIP = "10.10.192.168";
    setMyIp(masterIP, AllMetrics.NodeRole.ELECTED_MASTER);

    for (int i = 0; i < 10; i++) {
      if (rcaController.getCurrentRole() == AllMetrics.NodeRole.ELECTED_MASTER) {
        break;
      }
      Thread.sleep(1000);
    }
    Assert.assertEquals(rcaController.getCurrentRole(), AllMetrics.NodeRole.ELECTED_MASTER);

    AllMetrics.NodeRole nodeRole = AllMetrics.NodeRole.MASTER;
    setMyIp("10.10.192.200", nodeRole);
    for (int i = 0; i < 10; i++) {
      if (rcaController.getCurrentRole() == nodeRole) {
        break;
      }
      Thread.sleep(1000);
    }
    Assert.assertEquals(rcaController.getCurrentRole(), nodeRole);
  }

  private void setMyIp(String ip, AllMetrics.NodeRole nodeRole) {
    JSONObject jtime = new JSONObject();
    jtime.put("current_time", 1566414001749L);

    JSONObject jNode = new JSONObject();
    jNode.put(AllMetrics.NodeDetailColumns.ID.toString(), "4sqG_APMQuaQwEW17_6zwg");
    jNode.put(AllMetrics.NodeDetailColumns.HOST_ADDRESS.toString(), ip);
    jNode.put(AllMetrics.NodeDetailColumns.ROLE.toString(), nodeRole);

    ClusterDetailsEventProcessor eventProcessor = new ClusterDetailsEventProcessor();
    eventProcessor.processEvent(
        new Event("", jtime.toString() + System.lineSeparator() + jNode.toString(), 0));
  }

  enum RcaState {
    RUN,
    STOP;
  }

  private void changeRcaRunState(RcaState state) throws IOException {
    String value = "unknown";
    switch (state) {
      case RUN:
        value = "true";
        break;
      case STOP:
        value = "false";
        break;
    }
    Files.write(Paths.get(rcaEnabledFile.toString()), value.getBytes());
  }

  /**
   * Nanny starts and stops the RCA scheduler. condition for start: - rcaEnabled and NodeRole is not
   * UNKNOWN. condition for restart: - scheduler is running and node role has changed condition for
   * stop: - scheduler is running and rcaEnabled is false.
   */
  @Test
  public void testRcaNanny() throws IOException, InterruptedException {
    changeRcaRunState(RcaState.RUN);
    setMyIp("192.168.0.1", AllMetrics.NodeRole.MASTER);

    for (int i = 0; i < 10; i++) {
      if (rcaController.getRcaScheduler() != null && rcaController.getRcaScheduler().isRunning()) {
        break;
      }
      Thread.sleep(1000);
    }
    Assert.assertTrue(rcaController.getRcaScheduler().isRunning());

    setMyIp("192.168.0.1", AllMetrics.NodeRole.ELECTED_MASTER);
    for (int i = 0; i < 10; i++) {
      if (rcaController.getRcaScheduler() != null
          && rcaController.getRcaScheduler().getRole() == AllMetrics.NodeRole.ELECTED_MASTER) {
        break;
      }
      Thread.sleep(1000);
    }
    Assert.assertEquals(
        rcaController.getRcaScheduler().getRole(), AllMetrics.NodeRole.ELECTED_MASTER);

    changeRcaRunState(RcaState.STOP);
    for (int i = 0; i < 10; i++) {
      if (!rcaController.getRcaScheduler().isRunning()) {
        break;
      }
      Thread.sleep(1000);
    }
    Assert.assertFalse(rcaController.getRcaScheduler().isRunning());
  }
}
