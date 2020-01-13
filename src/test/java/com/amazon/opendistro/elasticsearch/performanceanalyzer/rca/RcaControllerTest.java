package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ClientServers;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.RCAScheduler;
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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class RcaControllerTest {

  private static final int IEVAL_ITERATIONS = 20;
  // NodeRolePoller has an periodicity of 60 seconds, hence we need iterations number greater than that
  private static final int NODE_ROLE_EVAL_ITERATIONS = 70;
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
            3, new ThreadFactoryBuilder().setNameFormat("test-network-thread-%d").build());
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
            Paths.get(rcaEnabledFileLoc.toString(), "rca.conf").toString(),
            10,
            TimeUnit.MILLISECONDS);

    setMyIp(masterIP, AllMetrics.NodeRole.UNKNOWN);
    rcaController.startPollers();

    // We just want to wait enough so that we all the pollers start up.
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @After
  public void tearDown() throws InterruptedException {
    RCAScheduler rcaScheduler = rcaController.getRcaScheduler();
    if (rcaScheduler != null && rcaScheduler.isRunning()) {
      rcaScheduler.shutdown();
    }
    netOperationsExecutor.shutdown();
    netOperationsExecutor.awaitTermination(1, TimeUnit.MINUTES);
    clientServers.getHttpServer().stop(0);
    clientServers.getNetClient().shutdown();
    clientServers.getNetServer().shutdown();
    // connectionManager.shutdown();
    dummyEsServer.stop(0);
  }

  @Test
  public void readRcaEnabledFromConf() throws IOException {
    changeRcaRunState(RcaState.STOP);
    Assert.assertTrue(check(new RcaEnabledEval(rcaController), false, IEVAL_ITERATIONS));
    Assert.assertFalse(rcaController.isRcaEnabled());

    changeRcaRunState(RcaState.RUN);
    Assert.assertTrue(check(new RcaEnabledEval(rcaController), true, IEVAL_ITERATIONS));
    Assert.assertTrue(rcaController.isRcaEnabled());
  }

  @Test
  public void nodeRoleChange() throws IOException {
    changeRcaRunState(RcaState.RUN);
    masterIP = "10.10.192.168";
    setMyIp(masterIP, AllMetrics.NodeRole.ELECTED_MASTER);
    Assert.assertTrue(check(new NodeRoleEval(rcaController), AllMetrics.NodeRole.ELECTED_MASTER, NODE_ROLE_EVAL_ITERATIONS));
    Assert.assertEquals(rcaController.getCurrentRole(), AllMetrics.NodeRole.ELECTED_MASTER);

    AllMetrics.NodeRole nodeRole = AllMetrics.NodeRole.MASTER;
    setMyIp("10.10.192.200", nodeRole);
    Assert.assertTrue(check(new NodeRoleEval(rcaController), nodeRole, NODE_ROLE_EVAL_ITERATIONS));
    Assert.assertEquals(rcaController.getCurrentRole(), nodeRole);
  }

  /**
   * Nanny starts and stops the RCA scheduler. condition for start: - rcaEnabled and NodeRole is not
   * UNKNOWN. condition for restart: - scheduler is running and node role has changed condition for
   * stop: - scheduler is running and rcaEnabled is false.
   */
  @Test
  public void testRcaNanny() throws IOException {
    changeRcaRunState(RcaState.RUN);
    AllMetrics.NodeRole nodeRole = AllMetrics.NodeRole.MASTER;
    setMyIp("192.168.0.1", nodeRole);
    Assert.assertTrue(check(new RcaSchedulerRunningEval(rcaController), true, IEVAL_ITERATIONS));
    Assert.assertTrue(check(new RcaSchedulerRoleEval(rcaController), nodeRole, NODE_ROLE_EVAL_ITERATIONS));
    Assert.assertTrue(rcaController.getRcaScheduler().isRunning());

    nodeRole = AllMetrics.NodeRole.ELECTED_MASTER;
    setMyIp("192.168.0.1", nodeRole);
    Assert.assertTrue(check(new RcaSchedulerRunningEval(rcaController), true, IEVAL_ITERATIONS));
    Assert.assertTrue(check(new RcaSchedulerRoleEval(rcaController), nodeRole, NODE_ROLE_EVAL_ITERATIONS));
    Assert.assertEquals(rcaController.getRcaScheduler().getRole(), nodeRole);

    nodeRole = AllMetrics.NodeRole.DATA;
    setMyIp("192.168.0.1", nodeRole);
    Assert.assertTrue(check(new RcaSchedulerRunningEval(rcaController), true, IEVAL_ITERATIONS));
    Assert.assertTrue(check(new RcaSchedulerRoleEval(rcaController), nodeRole, NODE_ROLE_EVAL_ITERATIONS));
    Assert.assertEquals(rcaController.getRcaScheduler().getRole(), nodeRole);

    changeRcaRunState(RcaState.STOP);
    Assert.assertTrue(check(new RcaSchedulerRunningEval(rcaController), false, IEVAL_ITERATIONS));
    Assert.assertFalse(rcaController.getRcaScheduler().isRunning());
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

  private <T> boolean check(IEval eval, T expected, int iterations) {
    final long SLEEP_TIME_MILLIS = 1000;

    for (int i = 0; i < iterations; i++) {
      if (eval.evaluateAndCheck(expected)) {
        return true;
      }
      try {
        Thread.sleep(SLEEP_TIME_MILLIS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return false;
  }

  interface IEval<T> {
    boolean evaluateAndCheck(T t);
  }

  class RcaEnabledEval implements IEval<Boolean> {
    private final RcaController rcaController;

    RcaEnabledEval(RcaController rcaController) {
      this.rcaController = rcaController;
    }

    @Override
    public boolean evaluateAndCheck(Boolean t) {
      return rcaController.isRcaEnabled() == t;
    }
  }

  class NodeRoleEval implements IEval<AllMetrics.NodeRole> {
    private final RcaController rcaController;

    NodeRoleEval(RcaController rcaController) {
      this.rcaController = rcaController;
    }

    @Override
    public boolean evaluateAndCheck(AllMetrics.NodeRole role) {
      return rcaController.getCurrentRole() == role;
    }
  }

  class RcaSchedulerRoleEval implements IEval<AllMetrics.NodeRole> {
    private final RcaController rcaController;

    RcaSchedulerRoleEval(RcaController rcaController) {
      this.rcaController = rcaController;
    }

    @Override
    public boolean evaluateAndCheck(AllMetrics.NodeRole role) {
      RCAScheduler rcaScheduler = rcaController.getRcaScheduler();
      return rcaScheduler != null && rcaScheduler.getRole() == role;
    }
  }

  class RcaSchedulerRunningEval implements IEval<Boolean> {
    private final RcaController rcaController;

    RcaSchedulerRunningEval(RcaController rcaController) {
      this.rcaController = rcaController;
    }

    @Override
    public boolean evaluateAndCheck(Boolean expected) {
      RCAScheduler rcaScheduler = rcaController.getRcaScheduler();
      return rcaScheduler != null && rcaScheduler.isRunning() == expected;
    }
  }
}
