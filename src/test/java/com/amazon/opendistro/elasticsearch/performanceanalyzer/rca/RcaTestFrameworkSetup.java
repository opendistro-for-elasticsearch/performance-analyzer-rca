package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.ClientServers;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.core.Util;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetServer;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Stats;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.spec.MetricsDBProviderTestHelper;
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
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.jooq.tools.json.JSONObject;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

/** This is the base class to set up Rca framework. */
public class RcaTestFrameworkSetup {
  private ScheduledExecutorService netOperationsExecutor;
  private ClientServers clientServers;
  private GRPCConnectionManager connectionManager;
  private Path rcaEnabledFileLoc;
  private Path testResourcesPath;
  private Path rcaEnabledFile;
  private HttpServer dummyEsServer;
  private RcaControllerOverride rcaController;
  private String masterIP;
  private Path testStatsLogs;
  private Path testPerformanceAnalyzerLogs;
  private int rpcServerPort;
  private int webServerPort;

  public Path getTestStatsLogs() {
    return testStatsLogs;
  }

  public Path getTestPerformanceAnalyzerLogs() {
    return testPerformanceAnalyzerLogs;
  }

  public StatsCollector getStatsCollector() {
    return statsCollector;
  }

  private StatsCollector statsCollector;

  private class RcaControllerOverride extends RcaController {
    private AnalysisGraph analysisGraph;

    public RcaControllerOverride(
        ScheduledExecutorService netOpsExecutorService,
        GRPCConnectionManager grpcConnectionManager,
        NetClient rcaNetClient,
        NetServer rcaNetServer,
        HttpServer httpServer,
        String rca_enabled_conf_location,
        String electedMasterRcaConf,
        String masterRcaConf,
        String rcaConf,
        long rcaNannyPollerPeriodicity,
        long rcaConfPollerPeriodicity,
        long nodeRolePollerPeriodicty,
        TimeUnit timeUnit,
        Queryable metricsDB) {
      super(
          netOpsExecutorService,
          grpcConnectionManager,
          rcaNetClient,
          rcaNetServer,
          httpServer,
          rca_enabled_conf_location,
          electedMasterRcaConf,
          masterRcaConf,
          rcaConf,
          rcaNannyPollerPeriodicity,
          rcaConfPollerPeriodicity,
          nodeRolePollerPeriodicty,
          timeUnit,
          metricsDB);
    }

    // TODO: implement this.
    @Override
    protected List<ConnectedComponent> getRcaGraphConnectedComponents(RcaConf rcaConf) {
      return Stats.getInstance().getConnectedComponents();
    }
  }

  RcaTestFrameworkSetup(String analysisGraph) throws Exception {
    this(3, Util.RPC_PORT, PerformanceAnalyzerApp.getPortNumber(), AllMetrics.NodeRole.DATA);
  }

  RcaTestFrameworkSetup(AnalysisGraph analysisGraph) throws Exception {
    this(3, Util.RPC_PORT, PerformanceAnalyzerApp.getPortNumber(), AllMetrics.NodeRole.DATA);
  }

  RcaTestFrameworkSetup(
      int netServerThreadCount, int rpcServerPort, int webServerPort, AllMetrics.NodeRole nodeRole)
      throws Exception {
    this.rpcServerPort = rpcServerPort;
    this.webServerPort = webServerPort;
    this.statsCollector = new StatsCollector("test-stats", 1000, new HashMap<>());

    initFilePaths();
    netOperationsExecutor =
        Executors.newScheduledThreadPool(
            netServerThreadCount,
            new ThreadFactoryBuilder().setNameFormat("test-network-thread-%d").build());
    clientServers = PerformanceAnalyzerApp.startServers(this.rpcServerPort, this.webServerPort);

    masterIP = "10.10.192.168";
    setDummyEsServer();
    boolean useHttps = PluginSettings.instance().getHttpsEnabled();
    connectionManager = new GRPCConnectionManager(useHttps);
    rcaController =
        new RcaControllerOverride(
            netOperationsExecutor,
            connectionManager,
            clientServers.getNetClient(),
            clientServers.getNetServer(),
            clientServers.getHttpServer(),
            rcaEnabledFileLoc.toString(),
            Paths.get(rcaEnabledFileLoc.toString(), "rca_elected_master.conf").toString(),
            Paths.get(rcaEnabledFileLoc.toString(), "rca_master.conf").toString(),
            Paths.get(rcaEnabledFileLoc.toString(), "rca.conf").toString(),
            1,
            1,
            1,
            TimeUnit.MILLISECONDS,
            new MetricsDBProviderTestHelper());

    setMyIp(masterIP, nodeRole);
    rcaController.startPollers();

    // We just want to wait enough so that we all the pollers start up.
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void initFilePaths() {
    String cwd = System.getProperty("user.dir");
    testResourcesPath = Paths.get(cwd, "src", "test", "resources");
    rcaEnabledFileLoc = Paths.get(testResourcesPath.toString(), "rca");
    rcaEnabledFile = Paths.get(rcaEnabledFileLoc.toString(), RcaController.getRcaEnabledConfFile());
    try {
      testPerformanceAnalyzerLogs = Paths.get(getLogFilePath("PerformanceAnalyzerLog"));
      testStatsLogs = Paths.get(getLogFilePath("StatsLog"));
    } catch (ParserConfigurationException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (SAXException e) {
      e.printStackTrace();
    } catch (XPathExpressionException e) {
      e.printStackTrace();
    }
  }

  private void setMyIp(String ip, AllMetrics.NodeRole nodeRole) {
    JSONObject jtime = new JSONObject();
    jtime.put("current_time", 1566414001749L);

    JSONObject jNode = new JSONObject();
    jNode.put(AllMetrics.NodeDetailColumns.ID.toString(), "4sqG_APMQuaQwEW17_6zwg");
    jNode.put(AllMetrics.NodeDetailColumns.HOST_ADDRESS.toString(), ip);
    jNode.put(AllMetrics.NodeDetailColumns.ROLE.toString(), nodeRole);
    if (nodeRole != AllMetrics.NodeRole.UNKNOWN) {
      jNode.put(
          AllMetrics.NodeDetailColumns.IS_MASTER_NODE.toString(),
          nodeRole == AllMetrics.NodeRole.MASTER);
    }

    ClusterDetailsEventProcessor eventProcessor = new ClusterDetailsEventProcessor();
    eventProcessor.processEvent(
        new Event("", jtime.toString() + System.lineSeparator() + jNode.toString(), 0));
  }

  private void setDummyEsServer() throws IOException {
    URI uri = URI.create(RcaController.getCatMasterUrl());

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
  }

  private String getLogFilePath(String filename)
      throws ParserConfigurationException, IOException, SAXException, XPathExpressionException {
    String testResourcesPath =
        Paths.get(this.testResourcesPath.toString(), "log4j2.xml").toString();

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();
    Document doc = builder.parse(testResourcesPath);
    XPathFactory xPathfactory = XPathFactory.newInstance();
    XPath xpath = xPathfactory.newXPath();
    return xpath.evaluate(
        String.format("Configuration/Appenders/File[@name='%s']/@fileName", filename), doc);
  }

  public void cleanUpLogs() {
    try {
      System.out.println("Deleting file: " + testPerformanceAnalyzerLogs);
      Files.deleteIfExists(testPerformanceAnalyzerLogs);
      System.out.println("Deleting file: " + testStatsLogs);
      Files.deleteIfExists(testStatsLogs);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
