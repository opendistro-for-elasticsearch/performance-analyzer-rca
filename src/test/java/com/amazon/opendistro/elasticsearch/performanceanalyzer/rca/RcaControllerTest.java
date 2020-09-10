package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaTestHelper.updateConfFileForMutedRcas;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.ClientServers;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerThreads;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Stats;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.RCAScheduler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.RcaSchedulerState;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.spec.MetricsDBProviderTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.threads.ThreadProvider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.WaitFor;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.swing.JSeparator;
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


    ClusterDetailsEventProcessor clusterDetailsEventProcessor = new ClusterDetailsEventProcessor();
    clusterDetailsEventProcessor.setNodesDetails(
            Collections.singletonList(new ClusterDetailsEventProcessor.NodeDetails(
                    AllMetrics.NodeRole.UNKNOWN,
                    "node1",
                    "127.0.0.1",
                    false))
    );
    AppContext appContext = new AppContext();
    appContext.setClusterDetailsEventProcessor(clusterDetailsEventProcessor);

    clientServers = PerformanceAnalyzerApp.createClientServers(connectionManager, appContext);
    clientServers.getHttpServer().start();

    URI uri = URI.create(RcaController.getCatMasterUrl());
    masterIP = "127.0.0.4";

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
            200,
            appContext,
                new MetricsDBProviderTestHelper()
        );
    rcaController.setDbProvider(new MetricsDBProviderTestHelper());

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
    Assert.assertFalse(rcaController.isRcaEnabled());

    changeRcaRunState(RcaState.RUN);
    Assert.assertTrue(check(new RcaEnabledEval(rcaController), true));
    Assert.assertTrue(rcaController.isRcaEnabled());
  }

  @Test
  public void readAndUpdateMutedRcasBeforeGraphCreation() throws Exception {
    Method readAndUpdateMutesRcas = rcaController.getClass()
            .getDeclaredMethod("readAndUpdateMutedComponents");
    readAndUpdateMutesRcas.setAccessible(true);

    String rcaConfPath = Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca_muted.conf").toString();
    Field rcaConfField = rcaController.getClass().getDeclaredField("rcaConf");
    rcaConfField.setAccessible(true);
    rcaConfField.set(rcaController, new RcaConf(rcaConfPath));
    updateConfFileForMutedRcas(rcaConfPath, Arrays.asList("CPU_Utilization", "Heap_AllocRate"));

    Field mutedGraphNodesField = Stats.class.getDeclaredField("mutedGraphNodes");
    mutedGraphNodesField.setAccessible(true);
    mutedGraphNodesField.set(Stats.getInstance(), null);
    Set<String> initialComponentSet = ConnectedComponent.getNodesForAllComponents(rcaController.getConnectedComponents());
    // Whitebox.setInternalState(ConnectedComponent.class, "nodeNames", new HashSet<>());

    readAndUpdateMutesRcas.invoke(rcaController);
    Assert.assertNull(Stats.getInstance().getMutedGraphNodes());

    // Re-set back to initialComponentSet
    // Whitebox.setInternalState(ConnectedComponent.class, "nodeNames", initialComponentSet);
  }

  @Test
  public void readAndUpdateMutedRcasWithRCAEnableAndDisabled() throws Exception {
    String mutedRcaConfPath = Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca_muted.conf").toString();
    List<String> mutedRcas1 = Arrays.asList("CPU_Utilization", "Heap_AllocRate");
    List<String> mutedRcas2 = Arrays.asList("Paging_MajfltRate");

    // RCA enabled, mutedRcas1 is muted nodes
    changeRcaRunState(RcaState.RUN);
    setMyIp(masterIP, AllMetrics.NodeRole.MASTER);
    RcaControllerHelper.set(Paths.get(rcaEnabledFileLoc.toString(), "rca.conf").toString(),
            mutedRcaConfPath,
            Paths.get(rcaEnabledFileLoc.toString(), "rca_elected_master.conf").toString());
    WaitFor.waitFor(() -> rcaController.getCurrentRole() == AllMetrics.NodeRole.MASTER, 10, TimeUnit.SECONDS);
    WaitFor.waitFor(() -> RcaControllerHelper.pickRcaConfForRole(AllMetrics.NodeRole.MASTER).getConfigFileLoc() == mutedRcaConfPath,
            10, TimeUnit.SECONDS);
    updateConfFileForMutedRcas(mutedRcaConfPath, mutedRcas1);
    Assert.assertTrue(check(new MutedRCAEval(rcaController), mutedRcas1));

    // Disable RCA
    changeRcaRunState(RcaState.STOP);
    Assert.assertTrue(check(new MutedRCAEval(rcaController), mutedRcas1));

    // Update rca.conf
    updateConfFileForMutedRcas(mutedRcaConfPath, mutedRcas2);

    // Enable RCA, assert mutedRcas2 is muted nodes
    changeRcaRunState(RcaState.RUN);
    Assert.assertTrue(check(new MutedRCAEval(rcaController), mutedRcas2));
  }

  @Test
  public void readAndUpdateMutedRcas() throws Exception {
    String mutedRcaConfPath = Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca_muted.conf").toString();
    List<String> mutedRcas1 = Arrays.asList("CPU_Utilization", "Heap_AllocRate");
    List<String> mutedRcas2 = Arrays.asList("Paging_MajfltRate");
    List<String> mutedRcas3 = Arrays.asList("Paging_MajfltRate_Check");
    List<String> mutedRcas4 = Arrays.asList("Paging_MajfltRate", "Paging_MajfltRate_Check");
    changeRcaRunState(RcaState.RUN);
    setMyIp(masterIP, AllMetrics.NodeRole.MASTER);
    RcaControllerHelper.set(Paths.get(rcaEnabledFileLoc.toString(), "rca.conf").toString(),
            mutedRcaConfPath,
            Paths.get(rcaEnabledFileLoc.toString(), "rca_elected_master.conf").toString());

    WaitFor.waitFor(() -> rcaController.getCurrentRole() == AllMetrics.NodeRole.MASTER, 10, TimeUnit.SECONDS);
    WaitFor.waitFor(() -> RcaControllerHelper.pickRcaConfForRole(AllMetrics.NodeRole.MASTER).getConfigFileLoc() == mutedRcaConfPath,
            10, TimeUnit.SECONDS);

    // 1. Muted Graph : "CPU_Utilization, Heap_AllocRate", updating RCA Config with "CPU_Utilization, Heap_AllocRate"
    // Muted Graph should have "CPU_Utilization, Heap_AllocRate"
    updateConfFileForMutedRcas(mutedRcaConfPath, mutedRcas1);
    Assert.assertTrue(check(new MutedRCAEval(rcaController), mutedRcas1));

    // 2. Muted Graph : "CPU_Utilization, Heap_AllocRate", updating RCA Config with ""
    // Muted Graph should have no nodes
    updateConfFileForMutedRcas(mutedRcaConfPath, Collections.emptyList());
    Assert.assertTrue(check(new MutedRCAEval(rcaController), Collections.emptyList()));


    // 3. Muted Graph : "", updating RCA Config with ""
    // Muted Graph should have no nodes
    updateConfFileForMutedRcas(mutedRcaConfPath, Collections.emptyList());
    Assert.assertTrue(check(new MutedRCAEval(rcaController), Collections.emptyList()));

    // 4. On RCA Config, "muted-rcas" : "CPU_Utilization, Heap_AllocRate", Updating RCA Config with "Paging_MajfltRate"
    // Muted Graph should retain only "Paging_MajfltRate"
    updateConfFileForMutedRcas(mutedRcaConfPath, mutedRcas2);
    Assert.assertTrue(check(new MutedRCAEval(rcaController), mutedRcas2));

    // 5. On RCA Config, "muted-rcas" : "Paging_MajfltRate", Updating RCA Config with "Paging_MajfltRate_Check"
    // Muted Graph should still have "Paging_MajfltRate"
    updateConfFileForMutedRcas(mutedRcaConfPath, mutedRcas3);
    Assert.assertTrue(check(new MutedRCAEval(rcaController), mutedRcas2));

    // 6. On RCA Config, "muted-rcas" : "CPU_Utilization, Heap_AllocRate"
    // Updating RCA Config with "Paging_MajfltRate_Check, Paging_MajfltRate"
    // Muted Graph should have "Paging_MajfltRate"
    updateConfFileForMutedRcas(mutedRcaConfPath, mutedRcas1);
    updateConfFileForMutedRcas(mutedRcaConfPath, mutedRcas4);
    Assert.assertTrue(check(new MutedRCAEval(rcaController), mutedRcas2));
  }

  @Test
  public void nodeRoleChange() throws IOException {
    changeRcaRunState(RcaState.RUN);
    masterIP = "10.10.192.168";
    setMyIp(masterIP, AllMetrics.NodeRole.ELECTED_MASTER);
    Assert.assertTrue(check(new NodeRoleEval(rcaController), AllMetrics.NodeRole.ELECTED_MASTER));
    Assert.assertEquals(rcaController.getCurrentRole(), AllMetrics.NodeRole.ELECTED_MASTER);
    Assert.assertEquals(rcaController.getCurrentRole(), rcaController.getRcaScheduler().getRole());

    AllMetrics.NodeRole nodeRole = AllMetrics.NodeRole.MASTER;
    setMyIp("10.10.192.200", nodeRole);
    Assert.assertTrue(check(new NodeRoleEval(rcaController), nodeRole));
    Assert.assertEquals(rcaController.getCurrentRole(), nodeRole);
    Assert.assertEquals(rcaController.getCurrentRole(), rcaController.getRcaScheduler().getRole());
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
    final String separator = System.lineSeparator();
    JSONObject jtime = new JSONObject();
    jtime.put("current_time", 1566414001749L);

    JSONObject jOverrides = new JSONObject();
    long overridesTimestamp = System.currentTimeMillis();

    JSONObject jNode = new JSONObject();
    jNode.put(AllMetrics.NodeDetailColumns.ID.toString(), "4sqG_APMQuaQwEW17_6zwg");
    jNode.put(AllMetrics.NodeDetailColumns.HOST_ADDRESS.toString(), ip);
    jNode.put(AllMetrics.NodeDetailColumns.ROLE.toString(), nodeRole);
    jNode.put(AllMetrics.NodeDetailColumns.IS_MASTER_NODE,
        nodeRole == AllMetrics.NodeRole.ELECTED_MASTER);

    ClusterDetailsEventProcessor eventProcessor = new ClusterDetailsEventProcessor();
    StringBuilder nodeDetails = new StringBuilder();
    nodeDetails.append(jtime);
    nodeDetails.append(separator);
    nodeDetails.append(jOverrides);
    nodeDetails.append(separator);
    nodeDetails.append(overridesTimestamp);
    nodeDetails.append(separator);
    nodeDetails.append(jNode.toString());
    eventProcessor.processEvent(
        new Event("", nodeDetails.toString(), 0));
    rcaController.getAppContext().setClusterDetailsEventProcessor(eventProcessor);
  }

  enum RcaState {
    RUN,
    STOP
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

    for (int i = 0; i < 10; i++) {
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

  class MutedRCAEval implements IEval<List<String>> {

    private final RcaController rcaController;

    MutedRCAEval(RcaController rcaController) {
      this.rcaController = rcaController;
    }

    @Override
    public boolean evaluateAndCheck(List<String> mutedRcas) {
      Set<String> actualMutedRcas = Stats.getInstance().getMutedGraphNodes();
      if (actualMutedRcas == null) {
        return false;
      }
      if (mutedRcas.isEmpty()) {
        return actualMutedRcas.isEmpty();
      } else {
        return actualMutedRcas.size() == mutedRcas.size() && actualMutedRcas.containsAll(mutedRcas);

      }
    }

  }
}
