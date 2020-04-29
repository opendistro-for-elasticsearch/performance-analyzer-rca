package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaTestHelper.updateConfFileForMutedRcas;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ClientServers;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerThreads;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Stats;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.RCAScheduler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.RcaSchedulerState;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.threads.ThreadProvider;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
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
  private Thread controllerThread;
  private ThreadProvider threadProvider;

  @Before
  public void setUp() throws Exception {
    threadProvider = new ThreadProvider();
    String cwd = System.getProperty("user.dir");
    rcaEnabledFileLoc = Paths.get(cwd, "src", "test", "resources", "rca");
    rcaEnabledFile = Paths.get(rcaEnabledFileLoc.toString(), RcaController.getRcaEnabledConfFile());
    netOperationsExecutor =
        Executors.newScheduledThreadPool(
            3, new ThreadFactoryBuilder().setNameFormat("test-network-thread-%d").build());
    boolean useHttps = PluginSettings.instance().getHttpsEnabled();
    connectionManager = new GRPCConnectionManager(useHttps);
    clientServers = PerformanceAnalyzerApp.createClientServers(connectionManager);
    clientServers.getHttpServer().start();

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

    RcaControllerHelper.set(Paths.get(rcaEnabledFileLoc.toString(), "rca.conf").toString(),
        Paths.get(rcaEnabledFileLoc.toString(), "rca_master.conf").toString(),
        Paths.get(rcaEnabledFileLoc.toString(), "rca_elected_master.conf").toString());
    rcaController =
        new RcaController(
            threadProvider,
            netOperationsExecutor,
            connectionManager,
            clientServers,
            rcaEnabledFileLoc.toString(),
            100,
            200
        );

    setMyIp(masterIP, AllMetrics.NodeRole.UNKNOWN);

    // since we are using 2 rca.conf files here for testing, 'rca_muted.conf' for testing Muted RCAs
    // and 'rca.conf' for remainging tests, use reflection to access the private rcaConf class variable.
    String rcaConfPath = Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString();
    Field field = rcaController.getClass().getDeclaredField("rcaConf");
    field.setAccessible(true);
    field.set(rcaController, new RcaConf(rcaConfPath));

    controllerThread =
        threadProvider.createThreadForRunnable(() -> rcaController.run(),
            PerformanceAnalyzerThreads.RCA_CONTROLLER);
    controllerThread.start();
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
    if (rcaScheduler != null && rcaScheduler.getState() == RcaSchedulerState.STATE_STARTED) {
      rcaScheduler.shutdown();
    }
    netOperationsExecutor.shutdown();
    netOperationsExecutor.awaitTermination(1, TimeUnit.MINUTES);
    clientServers.getHttpServer().stop(0);
    clientServers.getNetClient().stop();
    clientServers.getNetServer().stop();

    // connectionManager.stop();
    dummyEsServer.stop(0);
    controllerThread.interrupt();

    try {
      Thread.sleep(1000);
    } catch (InterruptedException ie) {
      ie.printStackTrace();
    }
  }

  @Test
  public void readRcaEnabledFromConf() throws IOException {
    changeRcaRunState(RcaState.STOP);
    Assert.assertTrue(check(new RcaEnabledEval(rcaController), false));
    Assert.assertFalse(RcaController.isRcaEnabled());

    changeRcaRunState(RcaState.RUN);
    Assert.assertTrue(check(new RcaEnabledEval(rcaController), true));
    Assert.assertTrue(RcaController.isRcaEnabled());
  }

  @Test
  public void readAndUpdateMutesRcas() throws Exception {
    String rcaConfPath = Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca_muted.conf").toString();
    Method readAndUpdateMutesRcas = rcaController.getClass()
            .getDeclaredMethod("readAndUpdateMutesRcas", null);
    readAndUpdateMutesRcas.setAccessible(true);

    Field field = rcaController.getClass().getDeclaredField("rcaConf");
    field.setAccessible(true);

    // 0. rcaConf is null
    updateConfFileForMutedRcas(rcaConfPath, "CPU_Utilization, Heap_AllocRate");
    field.set(rcaController, null);
    readAndUpdateMutesRcas.invoke(rcaController);
    Assert.assertTrue(Stats.getInstance().getMutedGraphNodes().isEmpty());

    // 1. Muted Graph : "CPU_Utilization, Heap_AllocRate", updating RCA Config with "CPU_Utilization, Heap_AllocRate"
    // Muted Graph should have "CPU_Utilization, Heap_AllocRate"
    updateConfFileForMutedRcas(rcaConfPath, "CPU_Utilization, Heap_AllocRate");
    field.set(rcaController, new RcaConf(rcaConfPath));
    readAndUpdateMutesRcas.invoke(rcaController);
    Assert.assertEquals(2,Stats.getInstance().getMutedGraphNodes().size());
    Assert.assertTrue(Stats.getInstance().isNodeMuted("CPU_Utilization"));
    Assert.assertTrue(Stats.getInstance().isNodeMuted("Heap_AllocRate"));

    // 2. Muted Graph : "CPU_Utilization, Heap_AllocRate", updating RCA Config with ""
    // Muted Graph should have no nodes
    updateConfFileForMutedRcas(rcaConfPath, "");
    field.set(rcaController, new RcaConf(rcaConfPath));
    readAndUpdateMutesRcas.invoke(rcaController);
    Assert.assertTrue(Stats.getInstance().getMutedGraphNodes().isEmpty());

    // 3. Muted Graph : "", updating RCA Config with ""
    // Muted Graph should have no nodes
    updateConfFileForMutedRcas(rcaConfPath, "");
    field.set(rcaController, new RcaConf(rcaConfPath));
    readAndUpdateMutesRcas.invoke(rcaController);
    Assert.assertTrue(Stats.getInstance().getMutedGraphNodes().isEmpty());

    // 4. On RCA Config, "muted-rcas" : "CPU_Utilization, Heap_AllocRate", Updating RCA Config with "Paging_MajfltRate"
    // Muted Graph should retain only "Paging_MajfltRate"
    updateConfFileForMutedRcas(rcaConfPath, "Paging_MajfltRate");
    field.set(rcaController, new RcaConf(rcaConfPath));
    readAndUpdateMutesRcas.invoke(rcaController);
    Assert.assertEquals(1, Stats.getInstance().getMutedGraphNodes().size());
    Assert.assertTrue(Stats.getInstance().isNodeMuted("Paging_MajfltRate"));

    // 5. On RCA Config, "muted-rcas" : "Paging_MajfltRate", Updating RCA Config with "Paging_MajfltRate_Check"
    // Muted Graph should still have "Paging_MajfltRate"
    updateConfFileForMutedRcas(rcaConfPath, "Paging_MajfltRate_Check");
    field.set(rcaController, new RcaConf(rcaConfPath));
    readAndUpdateMutesRcas.invoke(rcaController);
    Assert.assertEquals(1, Stats.getInstance().getMutedGraphNodes().size());
    Assert.assertTrue(Stats.getInstance().isNodeMuted("Paging_MajfltRate"));

    updateConfFileForMutedRcas(rcaConfPath, "CPU_Utilization, Heap_AllocRate");
    // 6. On RCA Config, "muted-rcas" : "CPU_Utilization, Heap_AllocRate"
    // Updating RCA Config with "Paging_MajfltRate_Check, Paging_MajfltRate"
    // Muted Graph should have "Paging_MajfltRate"
    updateConfFileForMutedRcas(rcaConfPath, "Paging_MajfltRate_Check, Paging_MajfltRate");
    field.set(rcaController, new RcaConf(rcaConfPath));
    readAndUpdateMutesRcas.invoke(rcaController);
    Assert.assertEquals(1, Stats.getInstance().getMutedGraphNodes().size());
    Assert.assertTrue(Stats.getInstance().isNodeMuted("Paging_MajfltRate"));

    // Re-set the 'rcaConf' variable to track 'rca.conf' for remaining tests
    field.set(rcaController, new RcaConf(Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString()));
  }

  @Test
  public void nodeRoleChange() throws IOException {
    changeRcaRunState(RcaState.RUN);
    masterIP = "10.10.192.168";
    setMyIp(masterIP, AllMetrics.NodeRole.ELECTED_MASTER);
    Assert.assertTrue(check(new NodeRoleEval(rcaController), AllMetrics.NodeRole.ELECTED_MASTER));
    Assert.assertEquals(rcaController.getCurrentRole(), AllMetrics.NodeRole.ELECTED_MASTER);

    AllMetrics.NodeRole nodeRole = AllMetrics.NodeRole.MASTER;
    setMyIp("10.10.192.200", nodeRole);
    Assert.assertTrue(check(new NodeRoleEval(rcaController), nodeRole));
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
    Assert.assertTrue(
        check(new RcaSchedulerRunningEval(rcaController), RcaSchedulerState.STATE_STARTED));
    Assert.assertTrue(check(new RcaSchedulerRoleEval(rcaController), nodeRole));
    Assert
        .assertEquals(RcaSchedulerState.STATE_STARTED, rcaController.getRcaScheduler().getState());

    nodeRole = AllMetrics.NodeRole.ELECTED_MASTER;
    setMyIp("192.168.0.1", nodeRole);
    Assert.assertTrue(check(new RcaSchedulerRunningEval(rcaController),
        RcaSchedulerState.STATE_STARTED));
    Assert.assertTrue(check(new RcaSchedulerRoleEval(rcaController), nodeRole));
    Assert.assertEquals(rcaController.getRcaScheduler().getRole(), nodeRole);

    nodeRole = AllMetrics.NodeRole.DATA;
    setMyIp("192.168.0.1", nodeRole);
    Assert.assertTrue(
        check(new RcaSchedulerRunningEval(rcaController), RcaSchedulerState.STATE_STARTED));
    Assert.assertTrue(check(new RcaSchedulerRoleEval(rcaController), nodeRole));
    Assert.assertEquals(rcaController.getRcaScheduler().getRole(), nodeRole);

    changeRcaRunState(RcaState.STOP);
    Assert.assertTrue(
        check(new RcaSchedulerRunningEval(rcaController), RcaSchedulerState.STATE_STOPPED));
    Assert
        .assertEquals(RcaSchedulerState.STATE_STOPPED, rcaController.getRcaScheduler().getState());
  }

  @Test
  public void testHandlers() throws IOException {
    // Only the metrics rpc handler should be set.
    Assert.assertNotNull(clientServers.getNetServer().getMetricsServerHandler());
    Assert.assertNull(clientServers.getNetServer().getSubscribeHandler());
    Assert.assertNull(clientServers.getNetServer().getSendDataHandler());

    changeRcaRunState(RcaState.RUN);
    AllMetrics.NodeRole nodeRole = AllMetrics.NodeRole.MASTER;
    setMyIp("192.168.0.1", nodeRole);
    Assert.assertTrue(
        check(new RcaSchedulerRunningEval(rcaController), RcaSchedulerState.STATE_STARTED));
    Assert.assertTrue(check(new RcaSchedulerRoleEval(rcaController), nodeRole));

    // Both RCA and metrics handlers should be set.
    Assert.assertNotNull(clientServers.getNetServer().getMetricsServerHandler());
    Assert.assertNotNull(clientServers.getNetServer().getSubscribeHandler());
    Assert.assertNotNull(clientServers.getNetServer().getSendDataHandler());

    // Metrics handler should still be set.
    changeRcaRunState(RcaState.STOP);
    Assert.assertTrue(
        check(new RcaSchedulerRunningEval(rcaController), RcaSchedulerState.STATE_STOPPED));

    Assert.assertNotNull(clientServers.getNetServer().getMetricsServerHandler());
    Assert.assertNull(clientServers.getNetServer().getSubscribeHandler());
    Assert.assertNull(clientServers.getNetServer().getSendDataHandler());
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

  private <T> boolean check(IEval eval, T expected) {
    final long SLEEP_TIME_MILLIS = 1000;

    for (int i = 0; i < 2; i++) {
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
      return RcaController.isRcaEnabled() == t;
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

  class RcaSchedulerRunningEval implements IEval<RcaSchedulerState> {

    private final RcaController rcaController;

    RcaSchedulerRunningEval(RcaController rcaController) {
      this.rcaController = rcaController;
    }

    @Override
    public boolean evaluateAndCheck(RcaSchedulerState expected) {
      RCAScheduler rcaScheduler = rcaController.getRcaScheduler();
      return rcaScheduler != null && rcaScheduler.getState() == expected;
    }
  }
}
